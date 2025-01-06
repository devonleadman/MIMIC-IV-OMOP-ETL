import config_static
import pandas as pd
import os

class Table:
    def __init__(self):
        pass

    def omop_table_generate_empty (self, table):
        columns = table.columns
        return pd.DataFrame(columns=columns)
    
    def omop_table_person (self):
        # Read the first row of the CSV to get the ending CSV column names
        output_df = pd.read_csv(f'{config_static.example_output_dir}1_omop_data_csv/person.csv', nrows=1, index_col=False)
        # Generate a new table with the new column names
        final_df = self.omop_table_generate_empty(output_df)
        # Read the raw MIMIC data for the table
        admissions = pd.read_csv(f'{config_static.input_dir}hosp/admissions.csv', index_col=False)
        # Use conversion_structure.json to get details on where to extract values

        # Then if the field is an ID field, use Athena to convert it to the OMOP concept ID

table_module = Table()