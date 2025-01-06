import json
import config_static
import pandas as pd

def start():
    print('Do you want to search the input or output data? (in/out)')
    response = input().lower()

    if response == "in":
        search_structure = config_static.input_structure_output
        search_base_path = config_static.input_dir + "hosp/"
    elif response == "out":
        search_structure = config_static.output_structure_output
        search_base_path = config_static.example_output_dir + "1_omop_data_csv/"
    else:
        start()


    print('Enter a phrase or field name to search the data for')
    phrase = input().lower()

    with open(search_structure, "r") as file:
        data = json.load(file)

    for k,v in data.items():
        for field in v:
            if phrase in field.lower():
                print(f'MATCH FOUND ({response})')
                print(f'Table -> {k}')
                print(f'Field -> {field}')

                csv_name = k + ".csv"
                full_csv_path = search_base_path + csv_name
                try:
                    print(pd.read_csv(full_csv_path, nrows=100, index_col=False).head(5))
                except Exception:
                    csv_path_test = search_base_path.split('/hosp')[0] + "/icu/" + csv_name
                    print(pd.read_csv(csv_path_test, nrows=100, index_col=False).head(5))
    
    start()

start()