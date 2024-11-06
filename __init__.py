import azure.functions as func
from .api_transactional_gc import API_Transactional_GC
from .api_datamanagement_gc import DataBackup  # Importa la clase para el respaldo

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# Endpoint para InsertData
@app.route(route="InsertData", methods=["POST"])
def insert_data(req: func.HttpRequest) -> func.HttpResponse:
    api = API_Transactional_GC()
    return api.process_request(req)

# Nuevo endpoint para BackupData
@app.route(route="BackupData", methods=["POST"])
def backup_data(req: func.HttpRequest) -> func.HttpResponse:
    data_backup = DataBackup()
    table_name = req.get_json().get("tableName", "all")  # Respaldar todas las tablas por defecto
    
    if table_name == "all":
        result = data_backup.backup_all_tables()
    else:
        result = data_backup.backup_table(table_name)
    
    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json"
    )


@app.route(route="RestoreData", methods=["POST"])
def restore_data(req: func.HttpRequest) -> func.HttpResponse:
    data_restore = DataRestore()
    table_name = req.get_json().get("tableName", "all")
    
    if table_name == "all":
        result = data_restore.restore_all_tables()
    else:
        result = data_restore.restore_table(table_name)
    
    return func.HttpResponse(
        json.dumps(result),
        status_code=200,
        mimetype="application/json"
    )