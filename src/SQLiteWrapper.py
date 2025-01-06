import sqlite3
import config_static
import re

class SQLiteWrapper:
    def __init__(self, db_path=config_static.sql_db_path):
        """
        Initialize the SQLiteWrapper with a database file path.
        If the file doesn't exist, it will create a new database.

        :param db_path: Path to the SQLite database file
        """
        self.db_path = db_path
        self.connection = None
        self.connect()

    def connect(self):
        """Establish a connection to the SQLite database."""
        if not self.connection:
            self.connection = sqlite3.connect(self.db_path)

    def close(self):
        """Close the connection to the SQLite database."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute_query(self, query, params=None):
        """
        Execute a single query.

        :param query: SQL query to execute
        :param params: Optional tuple of parameters for the query
        :return: Query results if it is a SELECT statement
        """
        self.connect()
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            return None
        finally:
            cursor.close()

    def split_statements(self, script_path):
        with open(script_path, 'r') as file:
            sql_script = file.read()
            # Remove single-line comments (--) and the text inside them
            sql_script = re.sub(r'--.*?$', '', sql_script, flags=re.M)  # Remove single-line comments

            # Remove multi-line comments (/* */) and the text inside them
            sql_script = re.sub(r'/\*.*?\*/', '', sql_script, flags=re.S)  # Remove multi-line comments

            # Remove all newlines
            sql_script = re.sub(r'\n', ' ', sql_script)  # Replace newlines with spaces

            # Replace multiple spaces with a single space
            sql_script = re.sub(r'\s{2,}', ' ', sql_script)  # Normalize extra spaces

            # Split by semicolon to extract individual statements
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
        return statements
    
    def execute_script(self, script_path, batch_size = 100, offset=0):
        """
        Execute a SQL script from a file.

        :param script_path: Path to the .sql file
        """
        self.connect()
        try:
            statements = self.split_statements(script_path)
            num_statements = len(statements)
            
            for idx,statement in enumerate(statements):
                self.connection.execute(statement)
                self.connection.commit()
                print(f"SQL script executed successfully from {script_path} [{str(idx)}/{num_statements+1}].")
        except FileNotFoundError:
            print(f"File not found: {script_path}")
        except sqlite3.Error as e:
            print(f"An error occurred while executing the script: {e}")

    def fetch_all(self, table_name):
        """
        Fetch all records from a specified table.

        :param table_name: Name of the table to fetch records from
        :return: List of all records in the table
        """
        query = f"SELECT * FROM table"
        return self.execute_query(query)

    def insert(self, table_name, columns, values):
        """
        Insert a record into a specified table.

        :param table_name: Name of the table to insert into
        :param columns: List of column names
        :param values: Tuple of values corresponding to the columns
        """
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['?'] * len(values))
        query = f"INSERT INTO table ({columns_str}) VALUES ({placeholders})"
        self.execute_query(query, values)

    def delete(self, table_name, condition, params):
        """
        Delete records from a specified table based on a condition.

        :param table_name: Name of the table to delete records from
        :param condition: SQL condition for deletion (e.g., "id = ?")
        :param params: Tuple of parameters for the condition
        """
        query = f"DELETE FROM table WHERE {condition}"
        self.execute_query(query, params)

    def update(self, table_name, updates, condition, params):
        """
        Update records in a specified table based on a condition.

        :param table_name: Name of the table to update
        :param updates: Dictionary of column-value pairs to update
        :param condition: SQL condition for the update (e.g., "id = ?")
        :param params: Tuple of parameters for the condition
        """
        update_str = ', '.join([f"{col} = ?" for col in updates.keys()])
        query = f"UPDATE table SET {update_str} WHERE {condition}"
        self.execute_query(query, tuple(updates.values()) + params)

# Example usage:
# wrapper = SQLiteWrapper('example.db')
# wrapper.execute_script('schema.sql')
# wrapper.insert('users', ['name', 'age'], ('John', 30))
# print(wrapper.fetch_all('users'))
# wrapper.close()