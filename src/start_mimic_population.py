from src.PostgreWrapper import *
import config_static
import os
import csv
import pandas as pd
import re
import json

def infer_data_type_pandas(dtype):
    """
    Maps pandas data types to SQL data types.
    :param dtype: Pandas data type.
    :return: SQL data type as a string.
    """
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "TEXT"

def save_data_types_to_json(csv_file_path, table_name, output_dir="mimic_types"):
    """
    Infers data types for a CSV file and saves them as a JSON file in the specified folder.

    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the table to use as the JSON file name.
    :param output_dir: Directory to save the JSON files.
    """
    os.makedirs(output_dir, exist_ok=True)  # Create the folder if it doesn't exist
    output_file = os.path.join(output_dir, f"{table_name}.json")

    # If the JSON file already exists, load and return the types
    if os.path.isfile(output_file):
        print(f"Data types for table '{table_name}' already exist. Using existing file.")
        with open(output_file, "r") as f:
            return json.load(f)

    # Read a sample of the CSV file to infer data types
    df = pd.read_csv(csv_file_path, nrows=5)
    data_types = {}

    for column in df.columns:
        sanitized_column = column.replace(' ', '_').lower()  # Sanitize column names
        data_types[sanitized_column] = infer_data_type_pandas(df[column].dtype)

    # Save inferred types to JSON
    with open(output_file, "w") as f:
        json.dump(data_types, f, indent=4)

    print(f"Inferred data types for table '{table_name}' saved to {output_file}.")
    return data_types

def generate_create_table_query(csv_file_path, table_name, output_dir=config_static.mimic_datatypes_dir):
    """
    Generates a SQL query to create a table based on the structure of a CSV file using inferred data types.

    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the SQL table to create.
    :param output_dir: Directory where JSON files with data types are stored.
    :return: SQL query as a string.
    """
    data_types = save_data_types_to_json(csv_file_path, table_name, output_dir)

    # Generate SQL column definitions from data types
    column_definitions = [f"{column} {dtype}" for column, dtype in data_types.items()]
    column_definitions_str = ',\n    '.join(column_definitions)

    # Build the CREATE TABLE query
    create_table_query = f"""
    CREATE TABLE {table_name} (
        {column_definitions_str}
    );
    """
    return create_table_query.strip()

def populate_table_from_csv(connection, csv_file_path, table_name, chunk_size=10000, output_dir="mimic_types"):
    """
    Populate a PostgreSQL table with data from a large CSV file in chunks.

    :param connection: PostgreSQL connection object.
    :param csv_file_path: Path to the CSV file.
    :param table_name: Name of the PostgreSQL table to populate.
    :param chunk_size: Number of rows to process at a time.
    :param output_dir: Directory where JSON files with data types are stored.
    """

    # Process the CSV file in chunks
    chunk_iter = pd.read_csv(csv_file_path, chunksize=chunk_size)
    for chunk_num,chunk in enumerate(chunk_iter):
        for _, row in chunk.iterrows():
            row_tuple = tuple(None if pd.isna(value) else value for value in row)
            placeholders = ', '.join(['%s'] * len(row_tuple))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
            connection.execute_query(insert_query, row_tuple)
            break

        print(f'Processed {chunk_num} chunks of data from {table_name}')
        print(f"Processed {len(chunk)} rows from {csv_file_path}")
        break

    print(f"Populated table '{table_name}' with data from {csv_file_path}.")

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
        
def start_mimic_population():
    db = PostgreWrapper()
    
    csv_files = get_csv_files_recursive(config_static.input_dir)
    print(f"Found {len(csv_files)} CSV file(s):")

    for csv_file in csv_files:
        print(f'MIMIC File {csv_file} to SQL Starting...')
        table_name = os.path.splitext(os.path.basename(csv_file))[0].lower()  # Use the file name (without extension) as the table name
        db.execute_query(f'DROP TABLE IF EXISTS {table_name};')
        
        query = generate_create_table_query(csv_file, table_name)
        db.execute_query(query)
        populate_table_from_csv(db, csv_file, table_name)