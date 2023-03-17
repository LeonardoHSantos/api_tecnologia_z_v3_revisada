import pandas as pd
from strategy.var_aux.actives import PARIDADES

def process_open_actives(dados):
        df_actives_open = None
        try:
            print(" ---------------- processando dados ativos abertos ---------------- ")
            lista_ativos = [
                [], # 0 - id
                [], # 1 - name
                [], # 2 - ticker
                [], # 3 - is_suspended
                [], # 4 - enabled
                [], # 5 - mercado
            ]
            for i in dados["binary"]["actives"]:
                try:
                    id   = dados["binary"]["actives"][i]["id"]
                    name = dados["binary"]["actives"][i]["name"]
                    ticker = dados["binary"]["actives"][i]["ticker"]
                    is_suspended = dados["binary"]["actives"][i]["is_suspended"]
                    enabled = dados["binary"]["actives"][i]["enabled"]
                    
                    if enabled == True and ticker in PARIDADES.keys(): #and is_suspended == False:
                    # if ticker in PARIDADES.keys(): #and is_suspended == False:
                        
                        lista_ativos[0].append(id)
                        lista_ativos[1].append(name)
                        lista_ativos[2].append(ticker)
                        lista_ativos[3].append(is_suspended)
                        lista_ativos[4].append(enabled)

                        if "OTC" in ticker:
                            lista_ativos[5].append("otc")
                        else:
                            lista_ativos[5].append("aberto")
                    
                except Exception as e:
                    print(e)
            
            if len(lista_ativos[0]) >= 1:
                df_actives_open = pd.DataFrame(list(zip(
                        lista_ativos[0],
                        lista_ativos[1],
                        lista_ativos[2],
                        lista_ativos[3],
                        lista_ativos[4],
                        lista_ativos[5],
                    )),
                    columns=[
                        "id", "name", "ticker", "is_suspended", "enabled",
                        "mercado",
                    ])
                return df_actives_open
            else:
                return None
            
        except Exception as e:
            msg_error = f"ERROR process_open_actives | Error: {e}"
            print(msg_error)
            return None