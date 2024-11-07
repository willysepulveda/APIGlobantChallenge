import logging
import json
import os
from io import BytesIO
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from fastavro import writer, parse_schema, reader
from database_connection import DatabaseConnection

# Configurar el esquema de AVRO para el respaldo
def get_avro_schema(table_name):
    schemas = {
        "HiredEmployees": {
            "name": "HiredEmployee",
            "type": "record",
            "fields": [
                {"name": "FirstName", "type": "string"},
                {"name": "LastName", "type": "string"},
                {"name": "HireDate", "type": "string"},
                {"name": "JobID", "type": "int"},
                {"name": "DepartmentID", "type": "int"}
            ]
        },
        "Departments": {
            "name": "Department",
            "type": "record",
            "fields": [
                {"name": "DepartmentID", "type": "int"},
                {"name": "DepartmentName", "type": "string"}
            ]
        },
        "Jobs": {
            "name": "Job",
            "type": "record",
            "fields": [
                {"name": "JobID", "type": "int"},
                {"name": "JobTitle", "type": "string"}
            ]
        }
    }
    return parse_schema(schemas[table_name])

# Clase para manejar el respaldo de datos
class DataBackup:
    def __init__(self):
        self.db_connection = DatabaseConnection()
        self.container_name = os.getenv("BLOB_CONTAINER_NAME")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("BLOB_STORAGE_CONNECTION_STRING")
        )
        self.tables = ["HiredEmployees", "Departments", "Jobs"]

    def backup_table(self, table_name):
        connection = self.db_connection.connect()
        cursor = connection.cursor()

        try:
            cursor.execute(f"SELECT * FROM GlobantPoc.{table_name}")
            
            rows = []
            for row in cursor.fetchall():
                row_dict = dict(zip([column[0] for column in cursor.description], row))
                
                if "HireDate" in row_dict and row_dict["HireDate"] is not None:
                    row_dict["HireDate"] = str(row_dict["HireDate"])
                
                rows.append(row_dict)

            # Convertir datos a formato AVRO
            avro_schema = get_avro_schema(table_name)
            avro_buffer = BytesIO()
            writer(avro_buffer, avro_schema, rows)
            avro_buffer.seek(0)

            # Subir a Blob Storage
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{table_name}_backup.avro")
            blob_client.upload_blob(avro_buffer, overwrite=True)

            logging.info(f"Backup for table {table_name} completed and saved to blob storage.")
            return {"status": "success", "message": f"Backup for table {table_name} completed."}
        
        except Exception as e:
            logging.error(f"Error in backing up table {table_name}: {str(e)}")
            return {"status": "error", "message": str(e)}
        
        finally:
            cursor.close()
            connection.close()

    def backup_all_tables(self):
        results = []
        for table in self.tables:
            result = self.backup_table(table)
            results.append(result)
        return results

class DataRestore:
    def __init__(self):
        self.db_connection = DatabaseConnection()
        self.container_name = os.getenv("BLOB_CONTAINER_NAME")
        self.blob_service_client = BlobServiceClient.from_connection_string(
            os.getenv("BLOB_STORAGE_CONNECTION_STRING")
        )
        self.tables = ["HiredEmployees", "Departments", "Jobs"]

    def restore_table(self, table_name):
        connection = self.db_connection.connect()
        cursor = connection.cursor()

        try:
            # Descargar el archivo AVRO desde Blob Storage
            blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=f"{table_name}_backup.avro")
            avro_data = blob_client.download_blob().readall()

            # Leer los datos desde el archivo AVRO
            avro_buffer = BytesIO(avro_data)
            avro_reader = reader(avro_buffer)

            # Activar IDENTITY_INSERT si es necesario
            if table_name in ["Departments", "Jobs"]:
                cursor.execute(f"SET IDENTITY_INSERT GlobantPoc.{table_name} ON")

            # Insertar datos en la tabla correspondiente
            for record in avro_reader:
                if table_name == "HiredEmployees":
                    cursor.execute(
                        "INSERT INTO GlobantPoc.HiredEmployees (FirstName, LastName, HireDate, JobID, DepartmentID) VALUES (?, ?, ?, ?, ?)",
                        record["FirstName"], record["LastName"], record["HireDate"], record["JobID"], record["DepartmentID"]
                    )
                elif table_name == "Departments":
                    cursor.execute(
                        "INSERT INTO GlobantPoc.Departments (DepartmentID, DepartmentName) VALUES (?, ?)",
                        record["DepartmentID"], record["DepartmentName"]
                    )
                elif table_name == "Jobs":
                    cursor.execute(
                        "INSERT INTO GlobantPoc.Jobs (JobID, JobTitle) VALUES (?, ?)",
                        record["JobID"], record["JobTitle"]
                    )

            # Desactivar IDENTITY_INSERT
            if table_name in ["Departments", "Jobs"]:
                cursor.execute(f"SET IDENTITY_INSERT GlobantPoc.{table_name} OFF")

            connection.commit()
            logging.info(f"Restore for table {table_name} completed.")
            return {"status": "success", "message": f"Restore for table {table_name} completed."}
        
        except Exception as e:
            logging.error(f"Error in restoring table {table_name}: {str(e)}")
            return {"status": "error", "message": str(e)}
        
        finally:
            cursor.close()
            connection.close()

    def restore_all_tables(self):
        results = []
        for table in self.tables:
            result = self.restore_table(table)
            results.append(result)
        return results