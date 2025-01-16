from src import pipeline
import argparse
from src.Config import *

def main():
    config = Config()
    parser = argparse.ArgumentParser(description="Driver for initiating the ETL pipeline")
    parser.add_argument("--restart", action="store_true", help="Reset all ETL Progress")
    args = parser.parse_args()
    
    if args.restart:
        print('Resetting all ETL Setup')
        config.set_config('SYSTEM', 'mimic_iv_raw_data_pull_complete', "False")
        config.set_config('SYSTEM', 'input_setup_complete', "False")
        config.set_config('SYSTEM', 'postgre_install_setup', "False")
        config.set_config('SYSTEM', 'postgre_database_setup', "False")

    pipeline.start()

if __name__ == "__main__":
    main()