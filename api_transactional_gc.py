import logging
#import pyodbc
import json
from fastapi import HTTPException
from database_connection import DatabaseConnection

# Validar transacciones
class DataValidator:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def validate_hired_employee(self, transaction):
        required_fields = ["FirstName", "LastName", "HireDate", "JobID", "DepartmentID"]
        for field in required_fields:
            if field not in transaction or transaction[field] is None:
                return False, f"{field} is missing or null."

        # Validar que el department_id exista en Departments
        self.cursor.execute("SELECT COUNT(*) FROM GlobantPoc.Departments WHERE DepartmentID = ?", transaction["DepartmentID"])
        if self.cursor.fetchone()[0] == 0:
            return False, "DepartmentID does not exist in Departments."

        # Validar que el job_id exista en Jobs
        self.cursor.execute("SELECT COUNT(*) FROM GlobantPoc.Jobs WHERE JobID = ?", transaction["JobID"])
        if self.cursor.fetchone()[0] == 0:
            return False, "JobID does not exist in Jobs."

        return True, None   

    @staticmethod
    def validate_department(data):
        if "DepartmentName" not in data or data["DepartmentName"] is None:
            return False, "DepartmentName is missing or null."
        return True, None

    @staticmethod
    def validate_job(data):
        if "JobTitle" not in data or data["JobTitle"] is None:
            return False, "JobTitle is missing or null."
        return True, None

# Insertar en BD
class DataInserter:
    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def insert_hired_employee(self, employee):
        try:
            self.cursor.execute(
                "INSERT INTO GlobantPoc.HiredEmployees (FirstName, LastName, HireDate, JobID, DepartmentID) VALUES (?, ?, ?, ?, ?)",
                employee["FirstName"],
                employee["LastName"],
                employee["HireDate"],
                employee["JobID"],
                employee["DepartmentID"]
            )
            self.connection.commit() 
            return True, None 
        except Exception as e:
            logging.error(f"Error inserting employee: {str(e)}")
            return False, str(e)

    def insert_department(self, department):
        try:
            self.cursor.execute(
                "INSERT INTO GlobantPoc.Departments (DepartmentName) VALUES (?)",
                department["DepartmentName"]
            )
            self.connection.commit() 
            return True, None
        except Exception as e:
            logging.error(f"Error inserting department: {str(e)}")
            return False, str(e)

    def insert_job(self, job):
        try:
            self.cursor.execute(
                "INSERT INTO GlobantPoc.Jobs (JobTitle) VALUES (?)",
                job["JobTitle"]
            )
            self.connection.commit() 
            return True, None
        except Exception as e:
            logging.error(f"Error inserting job: {str(e)}")
            return False, str(e)

    def log_transaction_error(self, transaction_type, transaction_data, error_message):
        try:
            self.cursor.execute(
                "INSERT INTO GlobantPoc.TransactionLogs (TransactionType, TransactionData, ErrorMessage) VALUES (?, ?, ?)",
                transaction_type,
                json.dumps(transaction_data),
                error_message
            )
            self.connection.commit()
        except Exception as e:
            logging.error(f"Error logging transaction: {str(e)}")

    def commit(self):
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()

# Log errores de transacciones
class ErrorLogger:
    def __init__(self):
        self.failed_transactions = []

    def log_error(self, transaction, error_message):
        logging.error(f"Transaction failed: {transaction}, Error: {error_message}")
        self.failed_transactions.append({"transaction": transaction, "error": error_message})

    def get_errors(self):
        return self.failed_transactions

# Main API
class API_Transactional_GC:
    def __init__(self):
        self.db_connection = DatabaseConnection()

    def process_batch(self, transactions, transaction_type):
        connection = self.db_connection.connect()
        data_inserter = DataInserter(connection)
        error_logger = ErrorLogger()

        successful_inserts = 0

        for transaction in transactions:
            if transaction_type == "HiredEmployees":
                is_valid, error_message = DataValidator(connection).validate_hired_employee(transaction)
                if not is_valid:
                    error_logger.log_error(transaction, error_message)
                    continue
                success, insert_error = data_inserter.insert_hired_employee(transaction)
            elif transaction_type == "Departments":
                is_valid, error_message = DataValidator.validate_department(transaction)
                if not is_valid:
                    error_logger.log_error(transaction, error_message)
                    continue
                success, insert_error = data_inserter.insert_department(transaction)
            elif transaction_type == "Jobs":
                is_valid, error_message = DataValidator.validate_job(transaction)
                if not is_valid:
                    error_logger.log_error(transaction, error_message)
                    continue
                success, insert_error = data_inserter.insert_job(transaction)
            else:
                error_logger.log_error(transaction, "Invalid transaction type.")
                continue

            if success:
                successful_inserts += 1
            else:
                error_logger.log_error(transaction, insert_error)

        data_inserter.commit()
        data_inserter.close()

        response = {
            "successCount": successful_inserts,
            "failureCount": len(error_logger.get_errors()),
            "errors": error_logger.get_errors()
        }
        return response