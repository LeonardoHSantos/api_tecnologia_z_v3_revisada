from datetime import datetime
from config_auth import IDENTIFIER_IQOPTION, PASSWORD_IQOPTION
from strategy.start_process import run_api

h_inicio = datetime.now()

RUN = run_api(identifier=IDENTIFIER_IQOPTION, password=PASSWORD_IQOPTION)
# RUN.connect_wss()


list_timeframes_sup_res = [60*5, 60*15, 60*60, 60*(60*4)]
list_amounts    = [10, 30, 30, 30]

obj_actives = {
    "EURJPY": {
        "timeframes": list_timeframes_sup_res,
        "amounts": list_amounts,
        },
    "GBPUSD": {
        "timeframes": list_timeframes_sup_res,
        "amounts": list_amounts,
        },
    "USDJPY": {
        "timeframes": list_timeframes_sup_res,
        "amounts": list_amounts,
        },
    "AUDJPY": {
        "timeframes": list_timeframes_sup_res,
        "amounts": list_amounts,
        },
    "AUDCAD": {
        "timeframes": list_timeframes_sup_res,
        "amounts": list_amounts,
        },
}

# check_conn = RUN.connect_wss()
# print(f"CHECK: {check_conn}")


print("**************************************")
data = RUN.process_analysis(
    tt_loop=1,
    option="auto",
    obj_actives=obj_actives,
    list_timeframes_sup_res=list_timeframes_sup_res,
    list_amounts=list_amounts,
    )

# h_fim = datetime.now()
# t_total = h_fim - h_inicio
# print(f"Tempo Total: {t_total}")
# print(f" ---------------------------------------------- Tempo Total: {t_total}")


