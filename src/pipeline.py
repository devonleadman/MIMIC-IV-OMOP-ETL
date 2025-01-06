from src.Pull import *
from src.Config import *
from src.Table import *
from src.DB import *
from src.SQLiteWrapper import *
from src.vocabulary_refresh import start_vocabulary_refresh
import subprocess
import sys

pull_module = Pull()
config = Config()
table_gen = Table()

def setup_postgre_all():
    def run_command(command, check=True):
        """Run a shell command and print its output."""
        try:
            result = subprocess.run(command, shell=True, check=check, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error running command: {command}\n{e.stderr}")
            raise

    def install_postgresql():
        config_dynamic = Config()
        if config_dynamic.get_config('SYSTEM', 'postgre_install_setup') == 'True':
            print('Postgre Install Already Complete... Returning')
            return
        
        """Install PostgreSQL on the system."""
        print("Updating package list...")
        run_command("sudo apt update")
        
        print("Installing PostgreSQL...")
        run_command("sudo apt install -y postgresql postgresql-contrib")

    def setup_postgresql():
        config_dynamic = Config()
        if config_dynamic.get_config('SYSTEM', 'postgre_database_setup') == 'True':
            print('Postgre Database Already Setup... Returning')
            return

        """Set up PostgreSQL database server."""
        print("Starting PostgreSQL service...")
        run_command("sudo service postgresql start")
        
        username = config_static.postgre_db_config['user']
        password = config_static.postgre_db_config['password']
        dbname = config_static.postgre_db_config['dbname']

        run_command(f"sudo -i -u postgres psql -c \"CREATE USER {username} WITH PASSWORD '{password}';\"")

        # Create the database
        run_command(f"sudo -i -u postgres psql -c \"CREATE DATABASE {dbname} OWNER {username};\"")

        # Grant privileges
        run_command(f"sudo -i -u postgres psql -c \"GRANT ALL PRIVILEGES ON DATABASE {dbname} TO {username};\"")

        print(f"User '{username}' and database '{dbname}' created successfully.")

    install_postgresql()
    setup_postgresql()

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
        setup_postgre_all()
        start_vocabulary_refresh()
        
        

        print('[FINISHED] FULL CONVERSION PIPELINE COMPLETE')