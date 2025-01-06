import psycopg2
from psycopg2.extras import RealDictCursor
import config_static

class PostgreWrapper:
    def __init__(self, db_config = config_static.postgre_db_config):
        """
        Initialize the PostgreSQLWrapper with database connection parameters.
        :param db_config: Dictionary containing dbname, user, password, host, port.
        """
        self.db_config = db_config
        self.connection = self.connect()
        self.cursor = self.connection.cursor()

    def connect(self):
        """Establish a connection to the PostgreSQL database."""
        try:
            return psycopg2.connect(**self.db_config)
        except Exception as e:
            print(f"Error connecting to the database: {e}")
            raise

    def close(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")

    def execute_query(self, query, params=None, fetch_results=False, fetch_one=False):
        """
        Execute a SQL query.
        :param query: The SQL query to execute.
        :param params: Optional tuple of parameters for the query.
        :param fetch_results: Whether to fetch all results of the query.
        :param fetch_one: Whether to fetch only one result.
        :return: Query results if fetch_results or fetch_one is True; otherwise, None.
        """
        try:
            self.cursor.execute(query, params)
            if fetch_results:
                return self.cursor.fetchall()
            if fetch_one:
                return self.cursor.fetchone()
            self.connection.commit()
            print("Query executed successfully.")
        except Exception as e:
            self.connection.rollback()
            print(f"Error executing query: {e}")
            raise

    def execute_script(self, script_path):
        """
        Execute a SQL script from a file.
        :param script_path: Path to the SQL script file.
        """
        try:
            with open(script_path, 'r') as file:
                sql_script = file.read()
                self.cursor.execute(sql_script)
                self.connection.commit()
                print(f"SQL script executed successfully from {script_path}.")
        except Exception as e:
            self.connection.rollback()
            print(f"Error executing script: {e}")
            raise