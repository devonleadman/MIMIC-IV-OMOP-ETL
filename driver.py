from test import *
from src.Pull import *
from src import pipeline

pull_module = Pull()
while True:
    print("""
    1) START PIPELINE
    2) INPUT SETUP **MUST BE RAN BEFORE STARTING PIPELINE**
    3) OUTPUT SETUP **MUST BE RAN BEFORE STARTING PIPELINE**
    """)

    response = input()

    if response == "1":
        print("""
        1) RUN FULL PIPELINE
        2) RUN SINGLE STEP OF PIPELINE
        """)

        response = input()

        if response == "1":
            pipeline.start()

        if response == "2":
            print("""
            1) pull mimic-iv data
            2) pull input data model [MIMIC]
            3) pull input table pairs [MIMIC]
            4) pull example OMOP data [OMOP]
            5) auto detect output data structure [OMOP]
            6) pull output table pairs [OMOP]
            """)    

            response = input()

            if response == "1":
                pull_module.pull_raw_mimic_iv_data()

            elif response == "2":
                pull_module.pull_raw_mimic_iv_data_model()

            elif response == "3":
                pull_module.pair_input_files()

            elif response == "4":
                pull_module.pull_example_omop_data()

            elif response == "5":
                pull_module.auto_detect_output_data_model()

            elif response == "6":
                pull_module.pair_output_files()

    elif response == "2":
        print("INITIATING INPUT SETUP")
        pull_module.input_setup()  

    elif response == "3":
        print("INITIATING OUTPUT SETUP")
        pull_module.output_setup()   
        
    else:
        print('Invalid Response')