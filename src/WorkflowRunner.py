import os
import json
import config_static
from src.PostgreWrapper import *

class WorkflowRunner:
    def __init__(self):
        self.config_file = None
        self.config = None        
        self.workflow = None
        self.workflow_config = None
        self.workflow_type = None
        self.workflow_config_filename = None
        

    def read_workflow_config(self):
        """
        Read and merge configuration from the default, global, and local sources.
        """
        print("Reading config...")

        workflow_config_path = f'{config_static.etl_conf_path}{self.workflow_config_filename}'
        if self.config_file and os.path.isfile(workflow_config_path):
            with open(workflow_config_path) as f:
                self.workflow_config = json.load(f)
                self.workflow_type = self.workflow_config['type']
                self.workflow_scripts = self.workflow_config['scripts']

    def read_config(self):
        """
        Read and merge configuration from the default, global, and local sources.
        """
        print("Reading config...")

        if self.config_file and os.path.isfile(config_static.full_etl_conf):
            with open(self.config_file) as f:
                self.config = json.load(f)[self.workflow]
                self.workflow_config_filename = self.config['conf']

    def run_workflow(self, config_file=config_static.full_etl_conf, workflow=None):
        """
        Execute the workflow based on the configuration.
        """
        print("Running workflow...")

        self.config_file = config_file
        self.config = None

        if workflow:
            self.workflow = workflow
            self.workflow_config = None
            self.workflow_type = None
            self.workflow_config_filename = None
        else:
            raise Exception('No worflow supplied for WorkflowRunner object')

        self.read_config()
        self.read_workflow_config()

        if self.workflow_type == 'sql':
            db = PostgreWrapper()
            
            for script in self.workflow_scripts:
                script_filename = script['script']
                db.execute_script(f'{config_static.sql_etl_scripts}{self.workflow}/{script_filename}')

        elif self.workflow_type == 'py':
            pass
        else:
            raise Exception('No \'type\' field in config file')

        #run_command_bq_script = "python scripts/bq_run_script.py {e} {etlconf_file} {c} {config_file} {script_file}"

        # Determine scripts to run
        #to_run = self.config['scripts']

        '''
        run_command = run_command_bq_script.format(
            script_file=' '.join([script['script'] for script in to_run]),
            e='-e' if self.etlconf_file else '',
            etlconf_file=self.etlconf_file or '',
            c='-c' if self.config_file else '',
            config_file=self.config_file or ''
        )

        print("run_workflow calls:")
        print(run_command)

        return_code = os.system(run_command)
        print("Workflow execution completed with return code:", return_code)
        return return_code
        '''