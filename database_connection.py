import logging
import pyodbc
import os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

import os
import json

# Carga las variables de local.settings.json manualmente
with open("local.settings.json") as f:
    settings = json.load(f)
    for key, value in settings["Values"].items():
        os.environ[key] = value

class DatabaseConnection:
    def __init__(self):
        # Obtén las variables de entorno desde local.settings.json
        self.server = os.getenv("SQL_SERVER")
        self.database = os.getenv("SQL_DATABASE")
        self.username = os.getenv("SQL_USERNAME")
        self.driver = '{ODBC Driver 17 for SQL Server}'
        
        # Verifica si estás en un entorno local o en la nube
        if os.getenv("ENVIRONMENT") == "LOCAL":
            # Para entorno local, usa la contraseña definida en local.settings.json
            self.password = os.getenv("SQL_PASSWORD")
        else:
            # En un entorno de Azure, usa Azure Key Vault para obtener la contraseña
            credential = DefaultAzureCredential()
            key_vault_url = os.getenv("KEY_VAULT_URL")
            print(f"Key Vault URL: {key_vault_url}")
            secret_client = SecretClient(vault_url=key_vault_url, credential=credential)
            self.password = secret_client.get_secret("dbpassword").value

        self.connection = None

    def connect(self):
        try:
            # Configura la cadena de conexión con las variables obtenidas
            self.connection = pyodbc.connect(
                f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password};Connection Timeout=30"
            )
            return self.connection
        except Exception as e:
            logging.error(f"Error connecting to database: {str(e)}")
            raise
