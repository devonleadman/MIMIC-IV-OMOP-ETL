# MIMIC-IV-OMOP-ETL
## BEFORE YOU BEGIN
This adaptation of the MIMIC repository utilizes a local PostgreSQL database which requires you to host a server
1. Clone the GitHub repo into a Linux environment (I used WSL:Ubuntu during development. Ensure you have Git downloaded, you can download Ubuntu from the Microsoft Store)
2. Run `pip install -r requirements.txt` in the root of the project (MIMIC-IV-OMOP-ETL/) - This would eventually be improved by automatically downloading requirements within a Docker container
3. Verify the default port on your Linux system (default port 5432 for PostgreSQL)
4. Set all of the variables stored in `config_dynamic.ini` to `False`

"
SYSTEM
mimic_iv_raw_data_pull_complete = True -> False
input_setup_complete = True -> False
postgre_install_setup = True -> False
postgre_database_setup = True -> False
"

5. Download the all of the Athena vocabulary from their website (There is no way to automate this process) and place the files under `{project_root}/data/vocab`, should contain about 10 different CSV files
6. I believe thats all that is required, my adaptation will automatically setup the PostgreSQL database for you during runtime

## START THE PIPELINE
1. In the root of the project, run `python3 driver.py` and type "1" and "Enter" to start the full pipeline - Ive simplified running the pipeline to a single access point
2. That's all!

## DEVELOPMENT NOTES
- As of right now in development, the full pipeline is not converted to use PostgreSQL. The pipeline consists of 6 main steps (ddl,staging,etl,ut,metrics,unload)
- the ddl and staging has been converted and im working through converting the etl step, which is the longest and most complex step. Ive converted and verified that the etl step works up to the population of cdm_caresite.sql

## IMPROVEMENT NOTES
- To ensure reproducibility, containing this code within a Docker container would be a good first improvement. This would also allow for the automation of setting up steps 1-3 in the BEFORE YOU BEGIN section of the README.md
- Allowing for arguements to be added, for example `python3 driver.py --restart` to reset the variables in config_dynamic.ini. This would setup the code to be able to easily automate step 4 in the BEFORE YOU BEGIN section of the README.md