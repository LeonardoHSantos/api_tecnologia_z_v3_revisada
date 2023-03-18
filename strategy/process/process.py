import pandas as pd

from strategy.db.insert import insert_to_database
from strategy.process.datetime_exp import expiration_operation_M5



def process_pre_analysis(dataframe, list_strategies_process, status_alert):
    lists_obj = lists_analysis(dataframe=dataframe)
    list_name_active = lists_obj["list_name_active"]
    list_name_timeframes = lists_obj["list_name_timeframes"]

    for active in list_name_active:
        print(active)

        print(f"---> Analisando ativo: {active}")
        base_temp_m5 = dataframe[ (dataframe["active_name"]==active) & (dataframe["tm_frame"]=="5M")]
        base_temp_m5 = pd.DataFrame(base_temp_m5)
        base_temp_m5.index = list(range(0, len(base_temp_m5["active_name"])))
        print(base_temp_m5)
        
        list_sup_res_final = []
        lista_res_final = []
        lista_sup_final = []
        print("\n\n\n#############################################################\n\n\n")
        for idx in base_temp_m5.index:
            from_m5                 = base_temp_m5["from"][idx]
            active_name_m5          = base_temp_m5["active_name"][idx]
            tm_frame_m5             = base_temp_m5["tm_frame"][idx]
            status_close            = base_temp_m5["status_close"][idx]
            # -------
            max_m5                  = base_temp_m5["max"][idx]
            open_m5                 = base_temp_m5["open"][idx]
            close_m5                = base_temp_m5["close"][idx]
            min_m5                  = base_temp_m5["min"][idx]

            lista_temp_sup_res = []
            lista_res  = []
            lista_sup  = []

            for tm_frame in list_name_timeframes:
                try:
                    
                    base_temp_sup_res = dataframe[ (dataframe["active_name"] == active) & (dataframe["tm_frame"] == tm_frame) & (dataframe["from"] < from_m5)]
                    #### determina a fatia de index para análises de suporte e resistência 
                    # base_temp_sup_res.index = list(range(0, len(base_temp_sup_res["id"])))
                    # max_idx = max(list(base_temp_sup_res.index.values))-30
                    # base_temp_sup_res = base_temp_sup_res[base_temp_sup_res.index >= max_idx]
                    
                    for idx_df_temp in base_temp_sup_res.index:
                        from_sup_res          = base_temp_sup_res["from"][idx_df_temp]
                        active_name_sup_res   = base_temp_sup_res["active_name"][idx_df_temp]
                        tm_frame_sup_res      = base_temp_sup_res["tm_frame"][idx_df_temp]

                        max_sup_res           = base_temp_sup_res["max"][idx_df_temp]
                        open_sup_res          = base_temp_sup_res["open"][idx_df_temp]
                        close_sup_res         = base_temp_sup_res["close"][idx_df_temp]
                        min_sup_res           = base_temp_sup_res["min"][idx_df_temp]
                        status_close_sup_res  = base_temp_sup_res["status_close"][idx_df_temp]
                        
                        
                        if tm_frame == "15M" and from_m5.hour == from_sup_res.hour and from_m5.minute + 5 <= from_sup_res.minute:
                            print(f" ---- não analisar - {from_m5} | {from_sup_res}")

                        else:
                            if status_close == "alta":
                                if max_m5 >= max_sup_res and close_m5 < max_sup_res:
                                    analisy = f"A1 - {from_m5} travou em resistência de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                                elif max_m5 > max_sup_res and close_m5 <= max_sup_res:
                                    analisy = f"A2 - {from_m5} travou em resistência de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)
                                    
                                if max_m5 >= min_sup_res and close_m5 < min_sup_res:
                                    analisy = f"A3 - {from_m5} travou em suporte de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                                elif max_m5 > min_sup_res and close_m5 <= min_sup_res:
                                    analisy = f"A4 - {from_m5} travou em suporte de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                            if status_close == "baixa":
                                if min_m5 <= max_sup_res and close_m5 > max_sup_res:
                                    analisy = f"B1 - {from_m5} travou em resistência de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                                elif min_m5 < max_sup_res and close_m5 >= max_sup_res:
                                    analisy = f"B2 - {from_m5} travou em resistência de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                                if min_m5 <= min_sup_res and close_m5 > min_sup_res:
                                    analisy = f"B3 - {from_m5} travou em suporte de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)

                                elif min_m5 < min_sup_res and close_m5 >= min_sup_res:
                                    analisy = f"B4 - {from_m5} travou em suporte de : {from_sup_res} | {tm_frame_sup_res} | max: {max_sup_res} | min: {min_sup_res}"
                                    include_analisy = False
                                    for i in lista_temp_sup_res:
                                        if i == analisy:
                                            include_analisy = True
                                    if include_analisy == False:
                                        lista_temp_sup_res.append(analisy)
                                        lista_res.append(tm_frame_sup_res)
                
                except Exception as e:
                    print(f"Erro: {e}")

            if len(lista_temp_sup_res) == 0:
                list_sup_res_final.append("-")
            else:
                list_sup_res_final.append(lista_temp_sup_res)
                print(f" ### Total lista: {len(list_sup_res_final)}")
            
            tt_sup = len(lista_sup)
            tt_res = len(lista_res)
            lista_res_final.append(tt_res)
            lista_sup_final.append(tt_sup)

        
        base_temp_m5["sup_res"] = list_sup_res_final
        base_temp_m5["tt_res"] = lista_res_final
        base_temp_m5["tt_sup"] = lista_sup_final

        # base_temp_m5["from"] = base_temp_m5["from"].dt.strftime("%Y-%m-%d %H:%M:%S")
        

        # print(" ----------------------------- datetime convertido ----------------------------- ")
        base_temp_m5["from"] = base_temp_m5["from"].dt.strftime("%Y-%m-%d %H:%M:%S")
        # print(base_temp_m5)

        ### chamada das funções de estratégias e insert/update de alertas no banco de dados
        for strategy in list_strategies_process:
            if strategy == "PADRAO-M5-V1":
                print("PROCESSANDO DADOS DA ESTRATÉGIA VERSÃO 1")
                process_strategy_v1(df=base_temp_m5, active_name=active, padrao="PADRAO-M5-V1", version="M5-V1", status_alert=status_alert)
            elif strategy == "PADRAO-M5-V2":
                print("PROCESSANDO DADOS DA ESTRATÉGIA VERSÃO 2")
                process_strategy_v2(df=base_temp_m5, active_name=active, padrao="PADRAO-M5-V2", version="M5-V2", status_alert=status_alert)
            elif strategy == "PADRAO-M5-V3":
                print("PROCESSANDO DADOS DA ESTRATÉGIA VERSÃO 3")
                process_strategy_v3(df=base_temp_m5, active_name=active, padrao="PADRAO-M5-V3", version="M5-V3", status_alert=status_alert)
            elif strategy == "PADRAO-M5-V4":
                print("PROCESSANDO DADOS DA ESTRATÉGIA VERSÃO 4")
                process_strategy_v4(df=base_temp_m5, active_name=active, padrao="PADRAO-M5-V4", version="M5-V4", status_alert=status_alert)


def lists_analysis(dataframe):
    list_name_active = list(dataframe.drop_duplicates(subset="active_name", keep="first")["active_name"].values)
    list_name_timeframes = list(dataframe.drop_duplicates(subset="tm_frame", keep="first")["tm_frame"].values)
    for i in range(len(list_name_timeframes)):
        if list_name_timeframes[i] == "5M":
            list_name_timeframes.pop(i)
    print(list_name_active)
    print(list_name_timeframes)
    return {"list_name_active": list_name_active, "list_name_timeframes": list_name_timeframes}


def process_strategy_v1(df, active_name, padrao, version, status_alert):

    try:
        list_signs = []
        direction = "-"
        for current_id in df.index:
            try:
                
                id_7 = current_id -6
                id_6 = current_id -5
                id_5 = current_id -4
                id_4 = current_id -3
                id_3 = current_id -2
                id_2 = current_id -1
                id_1 = current_id
             
                if df["status_close"][id_7] == "baixa" and df["status_close"][id_6] == "alta" and df["status_close"][id_5] == "alta" and df["status_close"][id_4] == "baixa" and df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "alta":
                    if df["sup_res"][id_1] != "-":
                        direction = "call"
                        list_signs.append(direction)
                    else:
                        list_signs.append("call - sem confluencia")

                elif df["status_close"][id_7] == "alta" and df["status_close"][id_6] == "baixa" and df["status_close"][id_5] == "baixa" and df["status_close"][id_4] == "alta" and df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "baixa":
                    if df["sup_res"][id_1] != "-":
                        direction = "put"
                        list_signs.append(direction)
                    else:
                        list_signs.append("put - sem confluencia")
                else:
                    list_signs.append("-")
            except Exception as e:
                print(f"Erro 2: {e}")
                list_signs.append("----")
        
        df["signs"] = list_signs
        print(f"DataFrame Finalizado ->> V1")
        print(df[ ["from", "active_name", "status_close", "tm_frame", "signs", "tt_res", "tt_sup"] ])
        prepare_msg_to_database(df, active_name, padrao, version, direction, status_alert)

        return
    except Exception as e:
        print(f"Erro durante processamento de: process_strategy_v1 | ERRO: {e}")
        return False

# list_minutes_v2 = [5, 20, 35, 50]
def process_strategy_v2(df, active_name, padrao, version, status_alert):
    try:
        list_signs = []
        direction = "-"
        for current_id in df.index:
            try:
                if current_id == 11:
                
                    id_6 = current_id -5
                    id_5 = current_id -4
                    id_4 = current_id -3
                    id_3 = current_id -2
                    id_2 = current_id -1
                    id_1 = current_id -0

                    if df["status_close"][id_6] == "baixa" and df["status_close"][id_5] == "alta" and df["status_close"][id_4] == "baixa" and df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "baixa" and df["status_close"][id_1] == "baixa":
                        if df["sup_res"][id_1] != "-":
                            direction = "call"
                            list_signs.append(direction)
                        else:
                            list_signs.append("call - sem confluencia")

                    elif df["status_close"][id_6] == "alta" and df["status_close"][id_5] == "baixa" and df["status_close"][id_4] == "alta" and df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "alta" and df["status_close"][id_1] == "alta":
                        if df["sup_res"][id_1] != "-":
                            direction = "put"
                            list_signs.append(direction)
                        else:
                            list_signs.append("put - sem confluencia")
                    else:
                        list_signs.append("-")
                else:
                        list_signs.append("-")
            except Exception as e:
                print(f"Erro 2: {e}")
                list_signs.append("---")
        
        df["signs"] = list_signs
        print(f"DataFrame Finalizado ->> V2")
        print(df[ ["from", "active_name", "status_close", "tm_frame", "signs", "tt_res", "tt_sup" ] ])
        prepare_msg_to_database(df, active_name, padrao, version, direction, status_alert)

        return direction
    except Exception as e:
        print(f"Erro durante processamento de: process_strategy_v2 | ERRO: {e}")
        return False

# list_minutes_v3 = [5, 20, 35, 50]
def process_strategy_v3(df, active_name, padrao, version, status_alert):
    try:
        list_signs = []
        direction = "-"

        confluencia_1 = "no"
        confluencia_2 = "no"
        confluencia_3 = "no"
        list_obs = []
        for current_id in df.index:
            try:
                if current_id == 2:
                    id_3 = current_id -2
                    id_2 = current_id -1
                    id_1 = current_id -0
                    if df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "alta" and df["status_close"][id_1] == "alta":
                        # if df["status_close"][current_id] == "baixa":
                        confluencia_1 = "yes"
                    elif df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "baixa" and df["status_close"][id_1] == "baixa":
                        # if df["status_close"][current_id] == "alta":
                        confluencia_1 = "yes"
                elif current_id == 5:
                    id_3 = current_id -2
                    id_2 = current_id -1
                    id_1 = current_id -0
                    if df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "alta" and df["status_close"][id_1] == "alta":
                        # if df["status_close"][current_id] == "baixa":
                        confluencia_2 = "yes" 
                    elif df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "baixa" and df["status_close"][id_1] == "baixa":
                        # if df["status_close"][current_id] == "alta":
                        confluencia_2 = "yes"
                elif current_id == 8:
                    id_3 = current_id -2
                    id_2 = current_id -1
                    id_1 = current_id -0
                    if df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "alta" and df["status_close"][id_1] == "alta":
                        # if df["status_close"][current_id] == "baixa":
                        confluencia_3 = "yes"
                    elif df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "baixa" and df["status_close"][id_1] == "baixa":
                        # if df["status_close"][current_id] == "alta":
                        confluencia_3 = "yes"
                # --------------
                if confluencia_1 == "no" and confluencia_2 == "no" and confluencia_3 == "no":
                    if current_id == 11:
                        id_3 = current_id -2
                        id_2 = current_id -1
                        id_1 = current_id -0

                        if df["status_close"][id_3] == "baixa" and df["status_close"][id_2] == "alta" and df["status_close"][id_1] == "alta":
                            if df["sup_res"][id_1] != "-":
                                direction = "put"
                                list_signs.append(direction)
                                list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
                            else:
                                list_signs.append("put - sem confluencia")
                                list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")

                        elif df["status_close"][id_3] == "alta" and df["status_close"][id_2] == "baixa" and df["status_close"][id_1] == "baixa":
                            if df["sup_res"][id_1] != "-":
                                direction = "call"
                                list_signs.append(direction)
                                list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
                            else:
                                list_signs.append("call - sem confluencia")
                                list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
                        else:
                            list_signs.append("-")
                            list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
                    else:
                        list_signs.append("-")
                        list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
                else:
                        list_signs.append("-")
                        list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
            
            except Exception as e:
                print(f"Erro 2: {e}")
                list_signs.append("---")
                list_obs.append(f"Q1: {confluencia_1} - Q2: {confluencia_2} - Q3: {confluencia_3}")
        
        obs_analysis = list_obs[len(list_obs)-1]
        df["signs"] = list_signs
        df["obs_list"] = list_obs
        print(f"DataFrame Finalizado ->> V3")
        print(df[ ["from", "active_name", "status_close", "tm_frame", "signs", "tt_res", "tt_sup", "obs_list" ] ])
        prepare_msg_to_database(df, active_name, padrao, version, direction, status_alert, obs_analysis)

        return
    except Exception as e:
        print(f"Erro durante processamento de: process_strategy_v3 | ERRO: {e}")
        return False

# list_minutes_v4 = [5, 20, 35, 50]
def process_strategy_v4(df, active_name, padrao, version, status_alert):
    try:
        list_signs = []
        direction = "-"
        for current_id in df.index:
            try:
                
                id_7 = current_id -6
                id_6 = current_id -5
                id_5 = current_id -4
                id_4 = current_id -3
                id_3 = current_id -2
                id_2 = current_id -1
                id_1 = current_id -0

                if df["status_close"][id_7] == "baixa" and df["status_close"][id_6] == "alta" and df["status_close"][id_5] == "alta" and df["status_close"][id_3] == "baixa":
                    if df["sup_res"][id_1] != "-":
                        direction = "put"
                        list_signs.append(direction)
                    else:
                        list_signs.append("put - sem confluencia")

                elif df["status_close"][id_7] == "alta" and df["status_close"][id_6] == "baixa" and df["status_close"][id_5] == "baixa" and df["status_close"][id_3] == "alta":
                    if df["sup_res"][id_1] != "-":
                        direction = "call"
                        list_signs.append(direction)
                    else:
                        list_signs.append("call - sem confluencia")
                else:
                    list_signs.append("-")

            except Exception as e:
                print(f"Erro 2: {e}")
                list_signs.append("---")
        
        df["signs"] = list_signs
        print(f"DataFrame Finalizado ->> V4")
        print(df[ ["from", "active_name", "status_close", "tm_frame", "signs", "tt_res", "tt_sup"] ])

        prepare_msg_to_database(df, active_name, padrao, version, direction, status_alert)
        
        return
    except Exception as e:
        print(f"Erro durante processamento de: process_strategy_v4 | ERRO: {e}")
        return False


def prepare_msg_to_database(df, active_name, padrao, version, direction, status_alert, obs_analysis="-"):
    
    max_idx = max(list(df.index))
    expiration_dataframe = df["from"][max_idx]
    expiration_obj = expiration_operation_M5(expiration_dataframe=expiration_dataframe)

    open_time                   = expiration_obj["open_time"]
    alert_datetime              = expiration_obj["alert_datetime"]
    expiration_alert            = expiration_obj["expiration_alert"]
    expiration_alert_timestamp  = expiration_obj["expiration_alert_timestamp"]
    alert_time_update           = expiration_obj["alert_time_update"]

    mercado = "aberto"
    if "OTC" in active_name:
        mercado = "otc"

    obj_to_database = {
        "open_time": open_time,
        "active": active_name,
        "direction": direction,
        "resultado": "process",
        "padrao": padrao,
        "alert_datetime": alert_datetime,
        "expiration_alert": expiration_alert,
        "expiration_alert_timestamp": expiration_alert_timestamp,
        "status_alert": status_alert,
        "name_strategy": f"{active_name}-{version}",
        "mercado": mercado,
        "alert_time_update": alert_time_update,
        "obs_analysis": obs_analysis
    }
    # for i, j in obj_to_database.items():
    #     print(i, j)
    print(obj_to_database)
    if direction == "call" or direction == "put":
        insert_to_database(obj_to_database=obj_to_database)








