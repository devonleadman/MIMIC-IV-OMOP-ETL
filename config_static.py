"""
Contains variables used in the system that will not change during runtime
"""

import os

# User Information
local_user = os.getenv('USER')

# PostgreSQL Information
postgre_db_config = {
        'dbname': 'mimic_omop_etl',
        'user': local_user,
        'password': local_user,
        'host': 'localhost',
        'port': 5432
    }
postgre_port_protocol = 'tcp'

# Pathing Information
root = f'{os.getcwd()}/'
src_dir = f'{root}src/'
raw_data = f'{root}data/raw/'
processed_data = f'{root}data/processed/'
input_dir = f'{raw_data}physionet.org/files/mimiciv/3.1/'
example_output_dir = f'{processed_data}physionet.org/files/mimic-iv-demo-omop/0.9/'
output_dir = f'{processed_data}conversion/'
input_structure_output = f'{processed_data}input_structure.json'
output_structure_output = f'{processed_data}output_structure.json'
input_table_pairs_output = f'{processed_data}input_table_pairs.json'
output_table_pairs_output = f'{processed_data}output_table_pairs.json'
config_dynamic = f'{root}config_dynamic.ini'
vocab_dir = f'{root}data/vocab/'
sql_scripts = f'{root}sql/'
sql_etl_scripts = f'{sql_scripts}etl/'
etl_conf_path = f'{root}conf/'
full_etl_conf = f'{etl_conf_path}full.etlconf'
sql_db_path = f'{sql_scripts}mimic_omop_etl.db'
mimic_datatypes_dir = f'{raw_data}mimic_datatypes'

# ETL Information
workflows = ['ddl','staging','etl','ut','metrics','unload']