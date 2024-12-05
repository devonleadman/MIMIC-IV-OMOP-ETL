from src.Pull import *
from src.Config import *

pull_module = Pull()
config = Config()

def start ():
    if not config.get_config('SYSTEM', 'input_setup_complete') == 'True':
        print('[WARNING] Input setup has not been completed... starting setup now')
        pull_module.input_setup()
        start()
    elif not config.get_config('SYSTEM', 'output_setup_complete') == 'True':
        print('[WARNING] Output setup has not been completed... starting setup now')
        pull_module.output_setup()
        start()
    
    else:
        # Data Processing Code Here
        # CODE FOR PAIRING TOGETHER INPUT AND OUTPUT TABLES
        # ^ make dictionary str:str format. key is a input table name and value is a output table name
        print('[FINISHED] FULL CONVERSION PIPELINE COMPLETE')