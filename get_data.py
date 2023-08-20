import pandas as pd
import websocket
import json
from sqlalchemy import create_engine
import time


engine = create_engine("sqlite:///CryptoDB.db")
stream = "wss://fstream.binance.com/ws/!ticker@arr"

def on_message(ws, message):
    msg = json.loads(message)
    symbols = [x for x in msg if x['s'].endswith('USDT')]
    frame = pd.DataFrame(symbols)[['E', 's', 'o', 'h', 'l', 'c']]
    frame.E = pd.to_datetime(frame.E, unit='ms')
    frame[['o', 'h', 'l', 'c']] = frame[['o', 'h', 'l', 'c']].astype(float)

    for _, row in frame.iterrows():
        symbol = row['s']
        data = pd.DataFrame([row], columns=['E', 's', 'o', 'h', 'l', 'c'])
        data.to_sql(symbol, engine, index=False, if_exists='replace')  # This will replace the entire table each time

def on_error(ws, error):
    print(f"WebSocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("WebSocket connection closed. Attempting to reconnect...")
    ws.close()
    ws.run_forever()

ws = websocket.WebSocketApp(stream, on_message=on_message, on_error=on_error, on_close=on_close)
while True:
    try:
        ws.run_forever()
    except Exception as e:
        print(f"Error: {e}. Retrying in 10 seconds...")
        time.sleep(10)

