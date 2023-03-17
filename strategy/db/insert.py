import config_auth

from strategy.db.conn_db import conn_db


def insert_to_database(obj_to_database):
    try:
        conn = conn_db()
        cursor = None
        if conn["status_conn_db"] == True:
            cursor = conn["conn"].cursor()
            
            open_time                   = obj_to_database["open_time"]
            active                      = obj_to_database["active"]
            direction                   = obj_to_database["direction"]
            resultado                   = obj_to_database["resultado"]
            padrao                      = obj_to_database["padrao"]
            alert_datetime              = obj_to_database["alert_datetime"]
            expiration_alert            = obj_to_database["expiration_alert"]
            expiration_alert_timestamp  = obj_to_database["expiration_alert_timestamp"]
            status_alert                = obj_to_database["status_alert"]
            name_strategy               = obj_to_database["name_strategy"]
            mercado                     = obj_to_database["mercado"]
            alert_time_update           = obj_to_database["alert_time_update"]

            # ############################################# temporário
            # direction = "call"
            
            if status_alert == "alert-open-operation":
                resultado = "open"

            comando_query = f'''
            SELECT status_alert FROM {config_auth.TABLE_NAME_M5}
            WHERE
            active = "{active}" and padrao = "{padrao}" and expiration_alert = "{expiration_alert}" and name_strategy = "{name_strategy}"
            '''
            cursor.execute(comando_query)
            result_query = cursor.fetchall()

            tt_query = len(result_query)
            print(f"\n\n --->> Total registros DB: {tt_query}")


            if status_alert == "alert-open-operation" and direction != "-":
                open_time = open_time
                resultado = "open"
            else:
                open_time = ""

            
            if tt_query == 0:
                if status_alert != "alert-open-operation":
                    try:
                        comando_insert = f'''
                        INSERT INTO {config_auth.TABLE_NAME_M5}
                        (open_time, active, direction, resultado, padrao, alert_datetime, expiration_alert, expiration_alert_timestamp, status_alert, name_strategy, mercado, alert_time_update)
                        VALUES
                        ("{open_time}", "{active}", "{direction}", "{resultado}", "{padrao}", "{alert_datetime}", "{expiration_alert}", "{expiration_alert_timestamp}", "{status_alert}", "{name_strategy}", "{mercado}", "{alert_time_update}")
                        '''
                        print(comando_insert)
                        cursor.execute(comando_insert)
                        conn["conn"].commit()
                        print(" --->> Registro inserido com sucesso!")
                    except Exception as e:
                        print(f"#1 ERRO INSERT DATABASE: {e}")
            else:
                try:
                    comando_update = f'''
                    UPDATE {config_auth.TABLE_NAME_M5}
                    SET open_time = "{open_time}", resultado = "{resultado}", direction = "{direction}", status_alert = "{status_alert}", alert_time_update = "{alert_time_update}"
                    WHERE name_strategy = "{name_strategy}" and expiration_alert = "{expiration_alert}" and id >= 0
                    '''
                    cursor.execute(comando_update)
                    conn["conn"].commit()
                    print(comando_update)
                    print(" ****** Registro atualizado com sucesso. Database desconectado. ****** ")
                except Exception as e:
                    print(f"#2 ERRO UPDATE DATABASE: {e}")
                    try:
                        cursor.close()
                        conn["conn"].close()
                        print(" DB - DESCONECTADO ")
                    except Exception as e:
                        print(f"#3 - ERRO DATABASE: {e}")
            try:
                cursor.close()
                conn["conn"].close()
                print(" DB - DESCONECTADO ")
            except Exception as e:
                print(f"#3 - ERRO DATABASE: {e}")

    except Exception as e:
        print(f"#4 ERRO DATABASE: {e}")
        try:
            cursor.close()
            conn["conn"].close()
            print(" DB - DESCONECTADO ")
        except Exception as e_db:
            print(f"#5 ERRO DATABASE: {e_db}")
