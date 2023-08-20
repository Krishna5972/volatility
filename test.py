from sqlalchemy import create_engine
import pandas as pd
from data_extraction import *
from binance_keys import api_key,secret_key
from binance.client import Client
from datetime import datetime,timedelta
from functions import *

timeframe ='1h'
coin = 'LEVER'

str_date = (datetime.now()- timedelta(days=6)).strftime('%b %d,%Y')
end_str = (datetime.now() +  timedelta(days=3)).strftime('%b %d,%Y')
client=Client(api_key,secret_key)
df=dataextract(coin,str_date,end_str,timeframe,client)

period = 14
atr1 = 1
super_df=supertrend(coin,df, period,atr1)

trade_df=create_signal_df(super_df,df,coin,timeframe,atr1,period,100,100)

print(trade_df['ema_81'])



