from src.PostgreWrapper import *
import config_static
import os
import csv
import pandas as pd

def generate_create_table_query(csv_file_path, table_name):
    """
    Generates a SQL query to create a table based on the structure of a CSV file.

    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the SQL table to create.
    :return: SQL query as a string.
    """
    if not os.path.isfile(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")

    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        headers = next(reader, None)  # Read the first row for column names

        if not headers:
            raise ValueError("CSV file is empty or has no headers.")

        # Generate SQL column definitions
        column_definitions = []
        for header in headers:
            sanitized_header = header.replace(' ', '_').lower()  # Sanitize column names
            column_definitions.append(f"{sanitized_header} TEXT")

        # Build the CREATE TABLE query
        column_definitions_str = ',\n    '.join(column_definitions)
        create_table_query = f"""
        CREATE TABLE {table_name} (
            {column_definitions_str}
        );
        """
        return create_table_query.strip()

def get_csv_files_recursive(directory):
    """
    Recursively get all CSV files in a directory.

    :param directory: Path to the directory.
    :return: List of paths to CSV files.
    """
    csv_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    return csv_files

def populate_table_from_csv(connection, csv_file_path, table_name):
    """
    Populate a SQLite table with data from a CSV file.

    :param connection: SQLite connection object.
    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the SQLite table to populate.
    """
    with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t')
        headers = next(reader, None)  # Read the headers

        if not headers:
            raise ValueError(f"CSV file {csv_file_path} is empty or has no data.")

        # Insert rows into the table
        row_count = 0
        for row in reader:
            if row_count >= 1000:
                break

            row = tuple(row)
            placeholders = ', '.join(['%s' for i in range(len(row))])
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            connection.execute_query(insert_query, params=row)
            row_count += 1

        print(f"Populated table '{table_name}' with data from {csv_file_path}.")

def start_vocabulary_refresh():
    db = PostgreWrapper()
    
    csv_files = get_csv_files_recursive(config_static.vocab_dir)
    print(f"Found {len(csv_files)} CSV file(s):")

    '''
    for csv_file in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_file))[0].lower()  # Use the file name (without extension) as the table name
        query = generate_create_table_query(csv_file, table_name)
        db.execute_query(f'DROP TABLE IF EXISTS {table_name};')
        db.execute_query(query)
        populate_table_from_csv(db, csv_file, table_name)
    '''
        
    #db.execute_script(f'{config_static.sql_scripts}vocabulary_check_bq.sql')
    db.execute_script(f'{config_static.sql_scripts}vocabulary_cleanup_bq_m.sql')
    


    

