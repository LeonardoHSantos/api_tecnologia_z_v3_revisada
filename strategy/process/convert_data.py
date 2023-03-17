import json
import pandas as pd

from dateutil import tz
import datetime
from datetime import datetime

def convert_to_json(data):
    return json.loads(data)


def timesTemp_converter (x):
    hora = datetime.strptime(datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    hora = hora.replace(tzinfo=tz.gettz('GMT'))
    return str(hora.astimezone(tz.gettz('America/Sao Paulo')))[:-6]

def convert_to_dataframe(data, name):
    df = pd.DataFrame(data)
    
    names_temp = name.split()
    name = names_temp[0]
    tm_frame = names_temp[2]
    
    lista_active_name   = list(map(lambda x: name, df["from"].values))
    lista_tm_frame      = list(map(lambda x: tm_frame, df["from"].values))
    
    df["active_name"]       = lista_active_name
    df["tm_frame"]          = lista_tm_frame

    lista_status_close = []
    for i in df.index:
        if float(df["close"][i]) > float(df["open"][i]):
            lista_status_close.append("alta")
        elif float(df["close"][i]) < float(df["open"][i]):
            lista_status_close.append("baixa")
        else:
            lista_status_close.append("sem mov.")

    df["active_name"] = lista_active_name
    df["status_close"] = lista_status_close
    # df = df.drop_duplicates(subset="id", keep="last")
    return df

def convert_to_dict_to_dumps(name, msg, request_id):
    return json.dumps(dict(name=name, msg=msg, request_id=request_id)).replace("'", '"')


