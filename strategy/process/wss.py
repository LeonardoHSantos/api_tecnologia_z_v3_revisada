import sys
import websocket

from strategy.process.process_actives_open import process_open_actives
from strategy.process.convert_data import convert_to_json, convert_to_dataframe

class WSS_Client:
    def __init__(self, host):
        
        self.wss = websocket.WebSocketApp(
            url=host,
            on_open=self.on_open,
            on_message=self.on_message,
            on_close=self.on_close,
        )

        self.timeSync = None
        self.status_conn    = False
        self.status_msg     = False
        self.run_status     = False
        self.list_dataframes = []

        self.list_timeSync = []


        self.status_process_candles = False
        self.expiration_base = None

        self.ob_timeframes = dict()

        self.df_actives_open = None
        self.status_actives_open = False
        
    
    def on_message(self, message):
        
        try:
            message = convert_to_json(data=message)
            
            # print(message)
            self.status_msg = True
            if message["name"] == "profile":
                self.run_status = True
            
            elif message["name"] == "candles":
                # obj_df = {f'{message["request_id"]}': convert_to_dataframe(data=message["msg"]["candles"], name=message["request_id"])}
                
                obj_df = convert_to_dataframe(data=message["msg"]["candles"], name=message["request_id"])
                self.list_dataframes.append(obj_df)
                self.status_process_candles = True
                
                exp = int(message["msg"]["candles"][0]["from"])-1
                req = message["request_id"].split()
                name = f"{req[0]}-{req[2]}"
                obj = {name:exp}
                self.ob_timeframes.update(obj)
                print(self.ob_timeframes)
            
            elif message["name"] == "initialization-data":
                print(" ********************* Mensagem actives open ********************* ")
                self.df_actives_open = process_open_actives(message["msg"])
                self.status_actives_open = True

            elif message["name"] == "timeSync":
                self.timeSync = int(message["msg"])
                self.list_timeSync.append(self.timeSync)
                # print(message)

        
        except Exception as e:
            print(e)
    
    def on_open(self):
        self.status_conn = True
        print(f"### OPEN CONNECTION - WEBSOCKET ### | STATUS CONN: {self.status_conn}")

    def on_close(self):
        # self.wss.close()
        self.status_conn    = False
        self.status_msg     = False
        self.run_status     = False
        
        print(f"### CLOSE CONNECTION - WEBSOCKET ### | status_conn: {self.status_conn} | status_msg: {self.status_msg} | run_status: {self.run_status}")
        # quit(0)

        
