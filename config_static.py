"""
Contains variables used in the system that will not change during runtime
"""

import os

# User Information
local_user = os.getenv('USER')
physionet_user = 'dleadman4716'

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