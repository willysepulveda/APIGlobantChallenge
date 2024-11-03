import azure.functions as func
from api_transactional_gc import API_Transactional_GC, DatabaseConnection,DataInserter
import json
import logging

# Instancia Azure Function
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="InsertData")
def insert_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing InsertData request...')

    try:
        req_body = req.get_json()
        transaction_type = req_body.get("transactionType")
        transactions = req_body.get("transactions")

        if not transaction_type or not transactions or not isinstance(transactions, list):
            return func.HttpResponse(
                json.dumps({"error": "Transaction type and a list of transactions are required"}),
                status_code=400,
                mimetype="application/json"
            )

        # Crear una instancia de DataInserter
        connection = DatabaseConnection().connect()
        data_inserter = DataInserter(connection)

        success_count = 0
        failure_count = 0
        errors = []

        for transaction in transactions:
            if transaction_type == "HiredEmployees":
                success, error_message = data_inserter.insert_hired_employee(transaction)
            elif transaction_type == "Departments":
                success, error_message = data_inserter.insert_department(transaction)
            elif transaction_type == "Jobs":
                success, error_message = data_inserter.insert_job(transaction)
            else:
                success = False
                error_message = "Invalid transaction type."

            if success:
                success_count += 1
            else:
                failure_count += 1
                errors.append({"transaction": transaction, "error": error_message})
                
                # Registrar el error en TransactionLogs
                data_inserter.log_transaction_error(transaction_type, transaction, error_message)

        # Cerrar la conexión a la base de datos
        connection.close()

        # Devolver la respuesta
        return func.HttpResponse(
            json.dumps({
                "successCount": success_count,
                "failureCount": failure_count,
                "errors": errors
            }),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

