from fastapi import FastAPI, HTTPException, Request
from api_transactional_gc import API_Transactional_GC, DatabaseConnection, DataInserter, DataValidator
import logging
from api_datamanagement_gc import DataBackup, DataRestore
from api_reporting_gc import APIReportingGC 

reporting_api = APIReportingGC()

app = FastAPI()

@app.post("/InsertData")
async def insert_data(request: Request):
    logging.info('Processing InsertData request...')

    try:
        req_body = await request.json()
        transaction_type = req_body.get("transactionType")
        transactions = req_body.get("transactions")

        if not transaction_type or not transactions or not isinstance(transactions, list):
            raise HTTPException(status_code=400, detail="Transaction type and a list of transactions are required")

        connection = DatabaseConnection().connect()
        data_inserter = DataInserter(connection)
        data_validator = DataValidator(connection)

        success_count = 0
        failure_count = 0
        errors = []

        # Validar y procesar las transacciones
        for transaction in transactions:
            is_valid = False
            error_message = None
            success = False

            if transaction_type == "HiredEmployees":
                is_valid, error_message = data_validator.validate_hired_employee(transaction)
                if is_valid:
                    success, error_message = data_inserter.insert_hired_employee(transaction)
            elif transaction_type == "Departments":
                is_valid, error_message = data_validator.validate_department(transaction)
                if is_valid:
                    success, error_message = data_inserter.insert_department(transaction)
            elif transaction_type == "Jobs":
                is_valid, error_message = data_validator.validate_job(transaction)
                if is_valid:
                    success, error_message = data_inserter.insert_job(transaction)
            else:
                error_message = "Invalid transaction type."

            if is_valid and success:
                success_count += 1
            else:
                failure_count += 1
                errors.append({"transaction": transaction, "error": error_message})
                data_inserter.log_transaction_error(transaction_type, transaction, error_message)

        connection.close()

        return {
            "successCount": success_count,
            "failureCount": failure_count,
            "errors": errors
        }
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/BackupData")
async def backup_data(request: Request):
    try:
        req_body = await request.json()
        table_name = req_body.get("tableName", "all") 
        data_backup = DataBackup()
        
        if table_name == "all":
            result = data_backup.backup_all_tables()
        else:
            result = data_backup.backup_table(table_name)
        
        return result
    except Exception as e:
        logging.error(f"Error during backup: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/RestoreData")
async def restore_data(request: Request):
    try:
        req_body = await request.json()
        table_name = req_body.get("tableName", "all")
        data_restore = DataRestore()
        
        if table_name == "all":
            result = data_restore.restore_all_tables()
        else:
            result = data_restore.restore_table(table_name)
        
        return result
    except Exception as e:
        logging.error(f"Error during restore: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/EmployeeHiresByQuarter")
async def employee_hires_by_quarter():
    try:
        result = reporting_api.get_employee_hires_by_quarter()
        return result
    except HTTPException as e:
        logging.error(f"Error in EmployeeHiresByQuarter endpoint: {e.detail}")
        raise e

@app.get("/DepartmentsAboveAverage")
async def departments_above_average():
    try:
        result = reporting_api.get_departments_above_average_hires()
        return result
    except HTTPException as e:
        logging.error(f"Error in DepartmentsAboveAverage endpoint: {e.detail}")
        raise e