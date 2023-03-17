from dateutil import tz
from datetime import datetime, timedelta

def datetime_now():
    return datetime.now()

def datetime_now(tzone):
    return datetime.now(tz=tz.gettz(tzone))

def expiration_operation_M5(expiration_dataframe):

    expiration = datetime.strptime(expiration_dataframe, "%Y-%m-%d %H:%M:%S") + timedelta(minutes=10)
    
    dtime = datetime_now(tzone="America/Sao Paulo")
    return {
        "open_time": int(dtime.timestamp()),
        "alert_datetime": dtime.strftime("%Y-%m-%d %H:%M:%S"),
        "expiration_alert": expiration.strftime("%Y-%m-%d %H:%M:%S"),
        "expiration_alert_timestamp": int(expiration.timestamp()),
        "alert_time_update": dtime.strftime("%Y-%m-%d %H:%M:%S"),
        "resultado": "process",
        "status_alert": "------------------",
        }


