from sqlalchemy import create_engine
import pandas as pd
from data_extraction import *
from binance_keys import api_key,secret_key
from binance.client import Client
from datetime import datetime,timedelta
from functions import *
from functions_volatile import *
engine = create_engine('sqlite:///CryptoDB.db')
import warnings
warnings.filterwarnings('ignore')
import websocket
import json 
import time
import pickle
import os



file_path  = 'in_trade_details.pkl'

# Check if the file exists
if os.path.exists(file_path):
    # Load the file
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    print("Data loaded:", data)

else:
    # If the file doesn't exist, create new data and save it
    data = {
        "first_trend_coin": None,
        "second_trend_coin": None
    }
    
    save_to_pkl(data, file_path)
    print("New .pkl file created with data:", data)


timeframe ='1h'
period = 14
atr1 = 1

str_date = (datetime.now()- timedelta(days=10)).strftime('%b %d,%Y')
end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')
client=Client(api_key,secret_key)

volatile_df = get_volatile_dataframe()

current_trend_coin_idx = -1
found_coin_trend_1 = False
while found_coin_trend_1 == False:
    coin = volatile_df.iloc[current_trend_coin_idx]['s'][:-4]
    print(f'Currently checking gor coin {coin}')
    
    df=dataextract(coin,str_date,end_str,timeframe,client)
    super_df=supertrend(coin,df, period,atr1)
    trade_df=create_signal_df(super_df,df,coin,timeframe,atr1,period,100,100)
    
    current_trend_coin_signal = trade_df.iloc[-1]['signal']
    if current_trend_coin_signal == "Buy":
        if trade_df.iloc[-1]['max_log_return'] > 0.0441:
            print('Missed Buy, looking for another')
            current_trend_coin_idx -= 1
        else:
            print('Take trade')
            found_coin_trend_1 = True
    else:
        if trade_df.iloc[-1]['max_log_return'] > 0.0414:
            print('Missed Sell , looking for another')
            current_trend_coin_idx -= 1        
        else:
            print('Take trade')
            found_coin_trend_1 = True
    print(current_trend_coin_idx)   
    
first_trend_coin = coin


data["first_trend_coin"] = first_trend_coin  
save_to_pkl(data, file_path)


timeframe ='1h'
stream = f"wss://fstream.binance.com/ws/{str.lower(first_trend_coin)}usdt@kline_{timeframe}"


def on_message(ws, message):
    msg = json.loads(message)
    print(msg)
    symbols = [x for x in msg if x['s'].endswith('USDT')]
    frame = pd.DataFrame(symbols)[['E', 's', 'o', 'h', 'l', 'c']]
    frame.E = pd.to_datetime(frame.E, unit='ms')
    frame[['o', 'h', 'l', 'c']] = frame[['o', 'h', 'l', 'c']].astype(float)
    print(frame)


def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed. Attempting to reconnect...")
    ws.close()
    ws.run_forever()

ws = websocket.WebSocketApp(stream, on_message=on_message, on_error=on_error, on_close=on_close)
while True:
    try:
        print('stream started')
        ws.run_forever()
    except Exception as e:
        print(f"Error: {e}. Retrying in 10 seconds...")
        time.sleep(10)