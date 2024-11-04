import azure.functions as func
from .api_transactional_gc import API_Transactional_GC

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

@app.route(route="InsertData", methods=["POST"])
def main(req: func.HttpRequest) -> func.HttpResponse:
    api = API_Transactional_GC()
    return api.process_request(req)

