def check_http_status_code(response):
    if response.status_code >= 300:
        txt = response.json() if response.status_code < 500 else " "
        raise Exception(f"Some thing wrong {response.status_code}, {txt}")