from fastapi import FastAPI, HTTPException, Request
from api_transactional_gc import API_Transactional_GC, DatabaseConnection, DataInserter, DataValidator
import logging

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
