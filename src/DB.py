from neo4j import GraphDatabase, Auth
import os
import csv

class DB:
    def __init__(self):
        # Connects to neo4j databasej
        user = os.environ['NEO4J_USERNAME']
        password = os.environ['NEO4J_PASSWORD']
        server_uri = os.environ['NEO4J_URI']

        #user = 'neo4j'
        #password = '#8114716Ss'
        #server_uri = 'bolt://192.168.1.87:7687'

        #watch("neo4j")

        neo4j_auth = Auth(scheme='basic',principal=user,credentials=password)
        connection = GraphDatabase.driver(uri=server_uri, auth=neo4j_auth) 
        self.session = connection.session(database='neo4j')

    def run(self, query, args=None):
        """
        Executes a Cypher query on the Neo4j database.

        Parameters:
        - query (str): The Cypher query to execute.
        - args (dict, optional): Additional parameters for the query.

        Returns:
        - Response from the Neo4j database.
        """

        if args == None:
            response = self.session.run(query)
        else:
            response = self.session.run(query, **args)
        return response
    
    def close (self):
        self.session.close()
    
    def generate_csv_files_and_labels(self, directory):
        csv_files = []
        for file in os.listdir(directory):
            # Only include files with .csv extension
            if file.lower().endswith(".csv"):
                label = os.path.splitext(file)[0]  # Use the file name (without extension) as the label
                csv_files.append({"file": file, "label": label})
        return csv_files

    # Extract column names from a CSV file
    def get_csv_headers(self, file_path):
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            headers = next(reader)  # First row contains headers
        return headers

    # Load CSV data into Neo4j
    def load_csv_to_neo4j(self, db, file_path, label, headers):
        # Construct dynamic Cypher query
        properties = ", ".join([f"{header}: row.{header}" for header in headers])
        query = f"""
        LOAD CSV WITH HEADERS FROM 'file:///{file_path}' AS row
        WITH row
        LIMIT 1000
        CREATE (n:{label} {{ {properties} }});
        """
        db.run(query)
        print(f"Loaded {file_path} into Neo4j as {label} nodes.")