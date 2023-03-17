import requests
from strategy.var_aux.var_aux import URL_HTTP
from strategy.process.convert_data import convert_to_json

def auth_broker(identifier, password):
    try:
        data = {
            "identifier": identifier,
            "password": password,
        }
        return convert_to_json(requests.post(url=URL_HTTP, data=data).content)
    except Exception as e:
        # print(f"Erro durante a autenticação de usuário: {e}")
        print(f" ### Erro durante a autenticação de usuário. ### ")
        return False