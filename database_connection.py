import azure.functions as func
import logging
import pyodbc
import json
import os
from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

# Conexión a la base de datos
class DatabaseConnection:
    def __init__(self):
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("SQL_DATABASE")
        self.username = os.getenv("SQL_USERNAME")
        self.password = os.getenv("SQL_PASSWORD")
        self.driver = '{ODBC Driver 17 for SQL Server}'
        self.connection = None

    def connect(self):
        try:
            self.connection = pyodbc.connect(
                f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};Connection Timeout=30"
            )
            return self.connection
        except Exception as e:
            logging.error(f"Error connecting to database: {str(e)}")
            raise