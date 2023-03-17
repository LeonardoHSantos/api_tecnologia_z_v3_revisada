import time
import threading
import pandas as pd


from strategy.process.wss  import WSS_Client
from strategy.process.auth import auth_broker


from strategy.var_aux.var_aux import URL_WSS
from strategy.var_aux.actives import PARIDADES, TIMEFRAMES_NAME

from strategy.var_aux import var_list_minutes

from strategy.db.update_result import update_database_results

from strategy.process.datetime_exp import datetime_now
from strategy.process.process import process_pre_analysis
from strategy.process.channels import get_candles, get_actives_open
from strategy.process.convert_data import convert_to_dict_to_dumps, timesTemp_converter

class run_api:
    def __init__(self, identifier, password):
        self.identifier = identifier
        self.password = password
        self.obj_wss        = None
        self.threading_wss  = None
        self.tt_tmSync = 0
        self.status_connection = False
        
    def connect_wss(self):
        try:
            auth = auth_broker(identifier=self.identifier, password=self.password)
            if auth != False:
                if auth["code"] == "success":
                    self.obj_wss = WSS_Client(host=URL_WSS)
                    
                    self.threading_wss = threading.Thread(target=self.obj_wss.wss.run_forever).start()
                    
                    while True:
                        if self.obj_wss.status_conn == True and self.obj_wss.status_msg == True:
                            break
                    msg_ssid = convert_to_dict_to_dumps(name="ssid", msg=auth["ssid"], request_id="")
                    print(msg_ssid)
                    self.obj_wss.wss.send(msg_ssid)
                    
                    while True:
                        if self.obj_wss.run_status == True:
                            break
                    self.status_connection = True
                    # return True # {"code": "usuário autenticado com sucesso."} #auth["ssid"]
                else:
                    self.status_connection = False
                    # return {"code": "usuário ou senha inválidos."}
            else:
                self.status_connection = False
                self.reconnect()
        except Exception as e:
            print(e)
            self.status_connection = False
    
    def get_actives_open(self):
        # self.connect_wss()
        msg_actives_open = get_actives_open()
        self.obj_wss.wss.send(msg_actives_open)

        while True:
            if self.obj_wss.status_actives_open == True:
                break
        
        df = self.obj_wss.df_actives_open
        self.obj_wss.df_actives_open = None
        self.obj_wss.status_actives_open = False
        # self.obj_wss.wss.close()
        return df


    def get_candles(self, obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option):
        # self.connect_wss()

        lista_request_id = []
        amount_temp = 0

        try:
            option_list_actives = None
            if option == "auto":
                actives_open = self.get_actives_open()
                list_actives_open = list(actives_open["ticker"].values)
                option_list_actives = list_actives_open
            elif option == "manual":
                option_list_actives = obj_actives.keys()
            
            if option_list_actives != None:
                # for active in obj_actives.keys():
                for active in option_list_actives:
                    if active in PARIDADES.keys():
                        print(active)
                        # for tm in range(len(obj_actives[active]["timeframes"])):
                        for tm in range(len(list_timeframes_sup_res)):
                            # timeframe = obj_actives[active]["timeframes"][tm]
                            # amount = obj_actives[active]["amounts"][tm]
                            timeframe   = list_timeframes_sup_res[tm]
                            amount      = list_amounts[tm]
                            
                            self.obj_wss.ob_timeframes.update()
                            for tt in range(tt_loop):
                                    expiration = datetime_now(tzone="America/Sao Paulo")

                                    print(f"{tt}/{tt_loop} | active: {active} | timeframe: {timeframe} | amount: {amount} | expiration: {expiration}")
                                    
                                    request_id = f"{active} - {TIMEFRAMES_NAME[timeframe]}"
                                    if request_id not in lista_request_id:
                                        lista_request_id.append(request_id)
                                    

                                    msg_get_candles = get_candles(active=active, timeframe=timeframe, amount=amount, expiration=int(expiration.timestamp()))
                                    self.obj_wss.wss.send(msg_get_candles)
                                    print(msg_get_candles)
                                    
                                    amount_temp += 1
                                    while True:
                                        if self.obj_wss.status_process_candles == True:
                                            break
                                    
                                    self.obj_wss.status_process_candles = False

            while True:
                if len(self.obj_wss.list_dataframes) == amount_temp:
                    break
            
            time.sleep(5)
            dict_dataframes = pd.concat(self.obj_wss.list_dataframes)
            dict_dataframes.sort_values(by=["from", "active_name"], ascending=True, inplace=True)
            
            list_timestamp = list(map(lambda x: timesTemp_converter(x), dict_dataframes["from"].values))
            dict_dataframes["from"] = list_timestamp

            dict_dataframes["from"] = pd.to_datetime(dict_dataframes["from"], format="%Y/%m/%d %H:%M:%S")
            dict_dataframes.drop_duplicates(subset=["from", "active_name", "tm_frame"], keep="last")
            # print(dict_dataframes.info())
            # print(dict_dataframes)

            self.obj_wss.list_dataframes.clear()
            # self.obj_wss.wss.close()
            return dict_dataframes
        except Exception as e:
            print(f"Erro durante processo de get_candles: {e}")
            self.status_connection = False

    def check_results(self, dataframe, list_strategies_process):
        update_database_results(dataframe=dataframe, list_strategies_process=list_strategies_process)

        
    def process_analysis(self, obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option):
        
        
        self.connect_wss()
        while True:
            time.sleep(1)
            tm = self.obj_wss.timeSync
            self.obj_wss.list_timeSync.append(tm)
            tt_ts = self.obj_wss.list_timeSync.count(tm)
            tt_list_tm = len(self.obj_wss.list_timeSync)
            # print(f"\n\n\n\n\n tt_ts: {tt_ts} | tt_list_tm: {tt_list_tm}")
            if tt_ts >= 8:
                self.reconnect()
                self.obj_wss.list_timeSync.clear()
            elif tt_list_tm >= 25:
                self.obj_wss.list_timeSync.clear()
            else:
                try:
                    if tt_ts <= 4:
                        data_hora = datetime_now(tzone="America/Sao Paulo")
                        second = data_hora.second
                        minute = data_hora.minute

                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V1_CHECK_RESULTS:
                            if second >= 3 and second <= 5:
                                status_alert = "check-result"
                                list_timeframes_sup_res = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts             = [2, 1, 1, 1]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V4"] #["PADRAO-M5-V1", "PADRAO-M5-V4"]
                                self.check_results(dataframe=dataframe, list_strategies_process=list_strategies_process)
                                
                                print(f"Processo CHECK STRATEGIES FINALIZADO - API em andamento | Status da conexão: {self.status_connection}")

                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V1.keys():
                            if second >= 15 and second <= 17:
                                status_alert = var_list_minutes.LIST_MINUTES_STRATEGY_V1[minute]
                                list_timeframes_sup_res     = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts                = [7, 30, 30, 30]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V4"] #["PADRAO-M5-V1", "PADRAO-M5-V4"]
                                process_pre_analysis(dataframe=dataframe, list_strategies_process=list_strategies_process, status_alert=status_alert)
                                print(f"Processo em andamento | Status da conexão: {self.status_connection}") 

                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V1_10S.keys():
                            if second >= 43 and second <= 45:
                                status_alert = var_list_minutes.LIST_MINUTES_STRATEGY_V1_10S[minute]
                                list_timeframes_sup_res     = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts                = [7, 30, 30, 30]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V4"] #["PADRAO-M5-V1", "PADRAO-M5-V4"]
                                process_pre_analysis(dataframe=dataframe, list_strategies_process=list_strategies_process, status_alert=status_alert)
                                print(f"Processo em andamento | Status da conexão: {self.status_connection}")            
                        
                        
                        # -------------------------------------


                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V3_CHECK_RESULTS:
                            if second >= 3 and second <= 5:
                                status_alert = "check-result"
                                list_timeframes_sup_res = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts             = [2, 1, 1, 1]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V3"] #["PADRAO-M5-V2", "PADRAO-M5-V3"]
                                self.check_results(dataframe=dataframe, list_strategies_process=list_strategies_process)
                                
                                print(f"Processo CHECK STRATEGIES FINALIZADO - API em andamento | Status da conexão: {self.status_connection}")
                        
                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V3.keys():
                            if second >= 16 and second <= 18:
                                status_alert = var_list_minutes.LIST_MINUTES_STRATEGY_V3[minute]
                                list_timeframes_sup_res = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts             = [6, 30, 30, 30]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V3"] #["PADRAO-M5-V2", "PADRAO-M5-V3"]
                                process_pre_analysis(dataframe=dataframe, list_strategies_process=list_strategies_process, status_alert=status_alert)
                                print(f"Processo em andamento | Status da conexão: {self.status_connection}")
                        
                        if minute in var_list_minutes.LIST_MINUTES_STRATEGY_V3_10S.keys():
                            if second >= 43 and second <= 45:
                                status_alert = var_list_minutes.LIST_MINUTES_STRATEGY_V3_10S[minute]
                                list_timeframes_sup_res = [60*5, 60*15, 60*60, 60*(60*4)]
                                list_amounts             = [6, 30, 30, 30]
                                dataframe = self.get_candles(obj_actives, tt_loop, list_timeframes_sup_res, list_amounts, option)
                                dataframe.index = list(range(0, len(dataframe["id"])))
                                list_strategies_process = ["PADRAO-M5-V3"] #["PADRAO-M5-V2", "PADRAO-M5-V3"]
                                process_pre_analysis(dataframe=dataframe, list_strategies_process=list_strategies_process, status_alert=status_alert)
                                print(f"Processo em andamento | Status da conexão: {self.status_connection}") 

                    

                    
                    
                    else:
                        self.reconnect()
                except Exception as e:
                    print(f"Erro 1.3 - lopp principal | ERRO: {e}")
                    self.reconnect()
    
    def reconnect(self):
        try:
            self.obj_wss.wss.close()
        except Exception as e:
            print(f"Erro durante a encerramento da conexão com o websocket: {e}")
        try:
            time.sleep(5)
            cont = 0
            max_reconnect = 5
            time_reconnect = 5
            # self.connect_wss()
            while True:
                for i in range(max_reconnect):
                    print(f"Tentando a reconexão com a API. | Tentativas: {i}/{max_reconnect}")
                    self.connect_wss()
                    time.sleep(2)
                    if i == max_reconnect:
                        print(f"API finalizada. Atingido limite de reconexão. | Tentativas: {i}/{max_reconnect}")
                        quit()
                    elif self.obj_wss.run_status == True:
                        print("Conexão restabelecida com websocket.")
                        self.status_connection = True
                        time.sleep(2)
                        return
                    else:
                        for t in range(time_reconnect):
                            if self.obj_wss.run_status == True:
                                return
                            print(f"Tentando se reconectar em: {time_reconnect-t}")
                            time.sleep(1)
                        # self.connect_wss()
                        cont += 1
        
        except Exception as e:
            print(f"Erro 1 - durante a reconexão com o websocket: {e}")
            return False




# def option_process(self):
#         option_select = None
#         list_options = [1, 2, 3]
#         while list_options not in list_options:
            
#             print("\n--------------------------------------------")
#             print("Realmente deseja encerrar a API?\n")
#             print("1 - SIM | 2 - NÃO (reconectar a api)")
#             try:
#                 option_select = input("\n --> escolha uma opção: ")
#                 print(f"\n *** Opção escolhida: {option_select} *** ")
#                 try:
#                     option_select = int(option_select)
#                     if option_select in list_options:
#                         return option_select
#                 except:
#                     print("\nOpção inválida! Selecione uma das opções informadas.")
#                     return None
#             except:
#                 print("\nOpção inválida! Selecione uma das opções informadas.")
#                 return None

# op = self.option_process()
            #     while True:
            #         if op == 1:
            #             # self.obj_wss.wss.close()
            #             print(f"API Encerrada via KeyboardInterrupt - {datetime.now()}.")
            #             quit()
            #         elif op == 2:
            #             try:
            #                 # self.obj_wss.wss.close()
            #                 time.sleep(2)
            #                 self.reconnect()
            #                 break
            #             except Exception as e:
            #                 print(f"Erro 2 - durante reconexão com websocket: {e}")
            #                 break
            #         else:
            #             op = self.option_process()
            #             # print("\n\n **** Nenhum opção selecionada ****")
            # except Exception as e:
            #     print(f"Erro durante processo de: process_analysis | ERRO: {e}")