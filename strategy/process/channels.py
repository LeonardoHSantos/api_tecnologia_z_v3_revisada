from strategy.var_aux.actives import PARIDADES, TIMEFRAMES_NAME
from strategy.process.convert_data import convert_to_dict_to_dumps

def get_candles(active, timeframe, amount, expiration):
    name = 'sendMessage'
    # mercado = actives_open[actives_open["ticker"]==active_name]["mercado"].values[0]
    message = {
        'name': 'get-candles',
        'version': '2.0',
        'body': {
            'active_id': PARIDADES[active],
            'size': timeframe,
            'to': expiration,
            'count': amount,
        }
    }
    request_id = f"{active} - {TIMEFRAMES_NAME[timeframe]}"
    print(f"\n\n\n\n ########### request_id: {request_id}")
    return convert_to_dict_to_dumps(name=name, msg=message, request_id=request_id)

def get_actives_open():
    name = 'sendMessage'
    message = {'name': 'get-initialization-data', 'version': '3.0', 'body': {}}
    request_id = 'get-underlying-list'
    return convert_to_dict_to_dumps(name=name, msg=message, request_id=request_id)