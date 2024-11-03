import logging
import json
import pyodbc
import os
from azure.functions import HttpRequest, HttpResponse

# Conexión a la base de datos
class DatabaseConnection:
    def __init__(self):
        self.server = os.getenv('SQL_SERVER')
        self.database = os.getenv('SQL_DATABASE')
        self.username = os.getenv('SQL_USERNAME')
        self.password = os.getenv('SQL_PASSWORD')
        self.driver = '{ODBC Driver 17 for SQL Server}'
        self.connection = None

    def connect(self):
        try:
            self.connection = pyodbc.connect(
                f'DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
            )
            return self.connection
        except Exception as e:
            logging.error(f"Database connection error: {str(e)}")
            raise

# Validar transacciones
class DataValidator:
    @staticmethod
    def validate(transaction):
        required_fields = ["id", "name", "datetime", "department_id", "job_id"]
        for field in required_fields:
            if field not in transaction or transaction[field] is None:
                return False, f"Field {field} is missing or null."
        return True, None

# Insertar datos
class DataInserter:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def insert_transaction(self, transaction):
        try:
            self.cursor.execute(
                "INSERT INTO GlobantPoc.HiredEmployees (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)",
                transaction["id"],
                transaction["name"],
                transaction["datetime"],
                transaction["department_id"],
                transaction["job_id"]
            )
            return True
        except Exception as e:
            logging.error(f"Error inserting transaction: {str(e)}")
            return False, str(e)

    def commit(self):
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

# Registro de errores
class ErrorLogger:
    @staticmethod
    def log_error(transaction, error_message):
        logging.error(f"Transaction failed: {transaction}, Error: {error_message}")
        return {"transaction": transaction, "error": error_message}

# Main API
class API_Transactional_GC:
    def __init__(self):
        self.db_connection = DatabaseConnection()

    def process_request(self, req: HttpRequest) -> HttpResponse:
        try:
            req_body = req.get_json()
            transactions = req_body.get("transactions")

            if not transactions:
                return HttpResponse("No data provided", status_code=400)
            
            connection = self.db_connection.connect()
            data_inserter = DataInserter(connection)

            successful_inserts = 0
            failed_inserts = []
            
            for transaction in transactions:
                is_valid, error_message = DataValidator.validate(transaction)
                if not is_valid:
                    failed_inserts.append(ErrorLogger.log_error(transaction, error_message))
                    continue

                success, insert_error = data_inserter.insert_transaction(transaction)
                if success:
                    successful_inserts += 1
                else:
                    failed_inserts.append(ErrorLogger.log_error(transaction, insert_error))

            data_inserter.commit()
            data_inserter.close()

            response = {
                "successCount": successful_inserts,
                "failureCount": len(failed_inserts),
                "errors": failed_inserts
            }
            return HttpResponse(json.dumps(response), status_code=200)

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            return HttpResponse("An error occurred", status_code=500)
