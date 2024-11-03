from .api_transactional_gc import API_Transactional_GC

def main(req: HttpRequest) -> HttpResponse:
    api = API_Transactional_GC()
    return api.process_request(req)
