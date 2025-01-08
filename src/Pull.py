import config_static
from src.Config import *
import os
from time import sleep
import pandas as pd
import json

class Pull:
    def __init__(self):
        self.raw = config_static.raw_data
        self.input_dir = config_static.input_dir
        self.input_structure_output = config_static.input_structure_output
        self.input_table_pairs_output = config_static.input_table_pairs_output
        self.output_table_pairs_output = config_static.output_table_pairs_output
        self.processed_data = config_static.processed_data
        self.output_dir = config_static.output_dir
        self.output_structure_output = config_static.output_structure_output
        self.example_output_dir = config_static.example_output_dir



    def unzip_file (self, file_path):
        if os.path.exists(file_path): 
            print(f'Unzipping file {file_path}')
            os.system(f'gzip -d {file_path}')



    def pull_raw_mimic_iv_data (self):
        config = Config()
        gz_files = list()

        if config.get_config('SYSTEM','mimic_iv_raw_data_pull_complete') == "True":
            print('[WARNING] Pull of MIMIC-IV data initiated but config_dynamic.ini says its complete... bypassing')
            return

        else:
            print('[COMPLETE] Pull of MIMIC-IV data not detected\nInitiating pull of data in 10 seconds...')
            sleep(10)
            os.system(f'wget -r -N -c -np --user {self.physionet_user} --ask-password https://physionet.org/files/mimiciv/3.1/ -P {self.raw}')
            for root, dirs, files in os.walk(self.input_dir):
                for file in files:
                    if file.endswith(".gz"):
                        gz_files.append(os.path.join(root, file))

            for gz_file in gz_files: self.unzip_file(gz_file)
            config.set_config('SYSTEM','mimic_iv_raw_data_pull_complete',True)

        print(f'[COMPLETE] Pulling of MIMIC-IV data complete and output to {self.raw}')



    def pull_example_omop_data (self):
        config = Config()
        gz_files = list()

        if config.get_config('SYSTEM','example_omop_data_pull_complete') == "True":
            print('[WARNING] Pull of example OMOP data initiated but config_dynamic.ini says its complete... bypassing')
            return

        else:
            print('[COMPLETE] Pull of example OMOP data not detected\nInitiating pull of data in 10 seconds...')
            sleep(10)
            os.system(f'wget -r -N -c -np https://physionet.org/files/mimic-iv-demo-omop/0.9/ -P {self.processed_data}')
            for root, dirs, files in os.walk(self.input_dir):
                for file in files:
                    if file.endswith(".gz"):
                        gz_files.append(os.path.join(root, file))

            for gz_file in gz_files: self.unzip_file(gz_file)
            config.set_config('SYSTEM','example_omop_data_pull_complete',True)

        print(f'[COMPLETE] Pulling of example OMOP data complete and output to {self.processed_data}')
        print(f'[VERIFY] Please ensure that the \"example_output_dir\" path variable in the \"config_static.py\" file correctly points to the correct path before continuing')



    def pull_raw_mimic_iv_data_model (self):
        def read_input_json (csv_info):
            csv_structure = dict()
            for k,v in csv_info.items():
                csv_df = pd.read_csv(v, nrows=0, index_col=False)
                csv_structure[k] = csv_df.columns.to_list()
            return csv_structure

        def write_input_json (self, csv_structure):
            with open(self.input_structure_output, "w") as json_file:
                json.dump(csv_structure, json_file, indent=4)
                
        csv_files = dict()
        for root, dirs, files in os.walk(self.input_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files[file.split('.')[0]] = f'{root}/{file}'

        csv_structure = read_input_json(csv_files)
        write_input_json(self, csv_structure)

        print(f'[COMPLETE] Input data structure parsed and output to {self.input_structure_output}')



    def pair_input_files (self):
        def write_input_table_pairs (self, table_pairs):
            with open(self.input_table_pairs_output, "w") as json_file:
                json.dump(table_pairs, json_file, indent=4)

        table_pairs = dict()
        with open(self.input_structure_output, "r") as file:
            data = json.load(file)

            for k,v in data.items():
                paired_tables = list()
                for i,j in data.items():
                    for value in v:
                        if value in j:
                            paired_tables.append(i)
                table_pairs[k] = list(set(paired_tables))

            write_input_table_pairs(self, table_pairs)
            print(f'[COMPLETE] Paired tables parsed and output to {self.input_table_pairs_output}')



    def pair_output_files (self):
        def write_output_table_pairs (self, table_pairs):
            with open(self.output_table_pairs_output, "w") as json_file:
                json.dump(table_pairs, json_file, indent=4)

        table_pairs = dict()
        with open(self.output_structure_output, "r") as file:
            data = json.load(file)

            for k,v in data.items():
                paired_tables = list()
                for i,j in data.items():
                    for value in v:
                        if value in j:
                            paired_tables.append(i)
                table_pairs[k] = list(set(paired_tables))

            write_output_table_pairs(self, table_pairs)
            print(f'[COMPLETE] Paired tables parsed and output to {self.output_table_pairs_output}')



    def auto_detect_output_data_model (self):
        def read_input_json (csv_info):
            csv_structure = dict()
            for k,v in csv_info.items():
                csv_df = pd.read_csv(v, nrows=0, index_col=False)
                csv_structure[k] = csv_df.columns.to_list()
            return csv_structure

        def write_input_json (self, csv_structure):
            with open(self.output_structure_output, "w") as json_file:
                json.dump(csv_structure, json_file, indent=4)
                
        csv_files = dict()
        for root, dirs, files in os.walk(self.example_output_dir):
            for file in files:
                if file.endswith(".csv"):
                    csv_files[file.split('.')[0]] = f'{root}/{file}'

        csv_structure = read_input_json(csv_files)
        write_input_json(self, csv_structure)

        print(f'[COMPLETE] Output data structure parsed and output to {self.output_structure_output}')



    def manually_define_output_data_model(self):
        # For if you are converting to a different model that is not OMOP
        print("[ERROR] NOT BUILT YET - manually_define_output_data_model()")
        exit()



    def input_setup (self):
        print('[MESSAGE] STARTING INPUT SETUP. SYSTEM CURRENTLY ONLY SUPPORTS A MIMIC-IV INPUT')
        config = Config()
        config.set_config('SYSTEM','input_setup_complete',False)

        self.pull_raw_mimic_iv_data()

        self.pull_raw_mimic_iv_data_model()

        self.pair_input_files()

        config.set_config('SYSTEM','input_setup_complete',True)
        print("[COMPLETE] INPUT SETUP COMPLETE")



    def output_setup (self):
        print('[MESSAGE] STARTING OUTPUT SETUP')
        config = Config()
        config.set_config('SYSTEM','output_setup_complete',False)
        
        print("""
              [RESPONSE REQUIRED] WOULD YOU LIKE TO MANUALLY DETECT YOUR OUTPUTS DATA MODEL VIA EXISTING DATA OR DEFINE IT YOURSELF?
              1) GENERATE USING EXISTING DATA
              2) MANUALLY DEFINE DATA MODEL
              """)
        
        response = input()

        if response == "1":
            print("[RESPONSE REQUIRED] IF YOU ARE CONVERTING MIMIC-IV TO OMOP, WOULD YOU LIKE TO PULL THE EXAMPLE DATA (y/n)?")
            response = input().lower()

            if response == "y":
                self.pull_example_omop_data()
                self.auto_detect_output_data_model()
                self.pair_output_files()
            elif response == "n":
                print(f'[VERIFY] Please ensure that the \"example_output_dir\" path variable in the \"config_static.py\" file correctly points to the correct path before continuing')
                print(f'[RESPONSE REQUIRED] Would you like to continue with detecting the data structure located at {self.example_output_dir} (y/n)?')
                response = input().lower()

                if response == "y":
                    self.auto_detect_output_data_model()
                    self.pair_output_files()
                elif response == "n":
                    print("[WARNING] RETURNING BACK TO MENU")
                    return

        elif response == "2":
            self.manually_define_output_data_model()
        else:
            print("[ERROR] INVALID RESPONSE, TRY AGAIN")
            self.output_setup()

        config.set_config('SYSTEM','output_setup_complete',True)
        print("[COMPLETE] OUTPUT SETUP COMPLETE")
