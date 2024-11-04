import azure.functions as func
import logging
import pyodbc
import json
import os
from azure.identity import DefaultAzureCredential
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

class DatabaseConnection:
    def __init__(self):
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("SQL_DATABASE")
        self.username = os.getenv("SQL_USERNAME")

        if os.getenv("ENVIRONMENT") == "LOCAL": #LOCAL for local test or AZURE to test in cloud
            
            credential = DefaultAzureCredential()
            key_vault_url = os.getenv("KEY_VAULT_URL")
            secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
            self.password = secret_client.get_secret("dbpassword").value
        else:
            # Usar contraseña local para pruebas
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
        
