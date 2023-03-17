import pandas as pd

import config_auth
from strategy.process.datetime_exp import datetime_now
from strategy.db.conn_db import conn_db



def lists_actives_analysis(dataframe):
    list_names_active = list(dataframe.drop_duplicates(subset="active_name", keep="first")["active_name"].values)
    print(list_names_active)
    return list_names_active

def update_result(active, padrao, expiration_alert, status_close):
    try:
        conn = conn_db()
        cursor = None
        if conn["status_conn_db"] == True:
            cursor = conn["conn"].cursor()

            comando_query = f'''
            SELECT direction, active, resultado, padrao, expiration_alert  FROM {config_auth.TABLE_NAME_M5}
            WHERE
            active = "{active}" and padrao = "{padrao}" and expiration_alert = "{expiration_alert}"
            '''
            cursor.execute(comando_query)
            result_query = cursor.fetchall()

            print(f"\n\n\n############ QUERY UPDATE: {result_query}")
            tt_query = len(result_query)

            if tt_query >= 1:
                try:

                    direcao = result_query[0][0]
                    resultado = None
                    
                    print(f"ACTIVE: {active} | DIRECTION: {direcao} | EXP: {expiration_alert} | PADRÃO: {padrao} | STATUS CLOSE: {status_close}")
                    alert_time_update = datetime_now(tzone="America/Sao Paulo").strftime("%Y-%m-%d %H:%M:%S")

                    if direcao == "call" and status_close == "alta":
                        resultado = "win"
                    elif direcao == "call" and status_close == "baixa":
                        resultado = "loss"
                    # ------

                    elif direcao == "put" and status_close == "baixa":
                        resultado = "win"
                    elif direcao == "put" and status_close == "alta":
                        resultado = "loss"
                    
                    # ------
                    elif direcao == "put" or direcao == "call" and status_close == "sem mov.":
                        resultado = "empate"
                        

                    if resultado != None: 
                        
                        comando_update = f'''
                        UPDATE {config_auth.TABLE_NAME_M5}
                        SET resultado = "{resultado}", alert_time_update = "{alert_time_update}"
                        WHERE active = "{active}" and padrao = "{padrao}" and expiration_alert = "{expiration_alert}" and id >= 0
                        '''
                        cursor.execute(comando_update)
                        conn["conn"].commit()
                        print(comando_update)
                        print(" ****** Registro atualizado com sucesso. Database desconectado. ****** ")
                
                except Exception as e:
                    print(f"#1 ERRO UPDATE DATABASE: {e}")
                    try:
                        cursor.close()
                        conn["conn"].close()
                        print(" DB - DESCONECTADO ")
                    except Exception as e:
                        print(f"#2 - ERRO DATABASE: {e}")
    except Exception as e:
        print(f"#2 ERRO UPDATE DATABASE: {e}")
        try:
            cursor.close()
            conn["conn"].close()
            print(" DB - DESCONECTADO ")
        except Exception as e:
            print(f"#3 - ERRO DATABASE: {e}")



def update_database_results(dataframe, list_strategies_process):

    try:
        dataframe["from"] = dataframe["from"].dt.strftime("%Y-%m-%d %H:%M:%S")
        
        list_names_active = lists_actives_analysis(dataframe=dataframe)
        for active in list_names_active:
            for padrao in list_strategies_process:
                base_temp_update = dataframe[ (dataframe["active_name"]==active) & (dataframe["tm_frame"]=="5M")]
                base_temp_update = pd.DataFrame(base_temp_update)
                base_temp_update.index = list(range(0, len(base_temp_update["active_name"])))
                print(base_temp_update)

                expiration_alert = base_temp_update["from"][1]
                status_close = base_temp_update["status_close"][0]

                print(f"---> ATUALIZANDO ATIVO: {active} | PADRÃO: {padrao} | EXP: {expiration_alert} | STATUS CLOSE: {status_close}")
                update_result(
                    active=active,
                    padrao=padrao,
                    expiration_alert=expiration_alert,
                    status_close=status_close)
    except Exception as e:
        print(f"ERRO PROCESSAMENTO: update_database_results | ERRO: {e}")

        