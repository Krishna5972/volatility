from datetime import datetime,timedelta
from time import sleep
from numba import njit
import numpy as np
import pandas as pd
import shutil
import os
import talib

def tr(data,coin):
    data['previous_close'] = data[f'close'].shift(1)
    data['high-low'] = abs(data[f'high'] - data[f'low'])
    data['high-pc'] = abs(data[f'high']- data['previous_close'])
    data['low-pc'] = abs(data[f'low'] - data['previous_close'])

    tr = data[['high-low', 'high-pc', 'low-pc']].max(axis=1)

    return tr

def candle_size(x,coin):
    return abs(((x[f'close']-x[f'open'])/x[f'open'])*100)


def atr(data, period,coin):
    data['tr'] = tr(data,coin)
    atr = data['tr'].rolling(period).mean()

    return atr



def supertrend(coin,df, period, atr_multiplier):
    hl2 = (df[f'high'] + df[f'low']) / 2
    df['atr'] = atr(df, period,coin)
    df['upperband'] = hl2 + (atr_multiplier * df['atr'])
    df['lowerband'] = hl2 - (atr_multiplier * df['atr'])
    df['in_uptrend'] = True
    
    df['OpenTime']=pd.to_datetime(df['OpenTime'])
    
    df['size']=df.apply(candle_size,axis=1,coin=coin)
    
    df['ma_7']=talib.MA(df[f'close'], timeperiod=7)
    df['ma_25']=talib.MA(df[f'close'], timeperiod=25)
    df['ma_99']=talib.MA(df[f'close'], timeperiod=99)
    df['ma_100']=talib.MA(df[f'close'], timeperiod=100)
    df['ma_200']=talib.MA(df[f'close'], timeperiod=200)

    

    df['ema_55']=talib.EMA(df[f'close'],55)
    df['ema_5']=talib.EMA(df[f'close'],5)
    df['ema_10']=talib.EMA(df[f'close'],10)
    df['ema_20']=talib.EMA(df[f'close'],20)
    df['ema_33']=talib.EMA(df[f'close'],33)
    df['ema_81']=talib.EMA(df[f'close'],81)
    df['ema_100']=talib.EMA(df[f'close'],100)
    df['ema_200']=talib.EMA(df[f'close'],200)
    df['rsi'] = talib.RSI(df[f'close'], timeperiod=14)
    df['macd'],df['macdsignal'],df['macdhist']=talib.MACD(df[f'close'], fastperiod=12, slowperiod=26, signalperiod=9)


    df['slowk'], df['slowd'] = talib.STOCH(df[f'high'],df[f'low'],df[f'close'], fastk_period=5, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)

    for current in range(1, len(df.index)):
        previous = current - 1

        if df[f'close'][current] > df['upperband'][previous]:
            df['in_uptrend'][current] = True
        elif df[f'close'][current] < df['lowerband'][previous]:
            df['in_uptrend'][current] = False
        else:
            df['in_uptrend'][current] = df['in_uptrend'][previous]

            if df['in_uptrend'][current] and df['lowerband'][current] < df['lowerband'][previous]:
                df['lowerband'][current] = df['lowerband'][previous]

            if not df['in_uptrend'][current] and df['upperband'][current] > df['upperband'][previous]:
                df['upperband'][current] = df['upperband'][previous]
        
    return df





@njit
def cal_numba(opens,highs,lows,closes,in_uptrends,profit_perc,sl_perc,upper_bands,lower_bands):
    entries=np.zeros(len(opens))
    signals=np.zeros(len(opens))  #characters  1--> buy  2--->sell
    tps=np.zeros(len(opens))
    trades=np.zeros(len(opens))  #characters   1--->w  0---->L
    close_prices=np.zeros(len(opens))
    time_index=np.zeros(len(opens))
    candle_count=np.zeros(len(opens))
    local_max=np.zeros(len(opens))
    local_min=np.zeros(len(opens))
    upper=np.zeros(len(opens))
    lower=np.zeros(len(opens))
    
    local_max_bar=np.zeros(len(opens))
    local_min_bar=np.zeros(len(opens))
    
    indication = 0
    buy_search=0
    sell_search=1
    change_index=0
    i=-1
    while(i<len(opens)):
        i=i+1
        
        if (indication == 0) & (sell_search == 1) & (buy_search == 0) & (change_index == i):
            
            sell_search=0
            flag=0
            trade= 5
            while (indication == 0):
                
                entry = closes[i]
                tp = entry - (entry * profit_perc)
                sl = entry + (entry * sl_perc)
                
                upper[i]=upper_bands[i]
                lower[i]=lower_bands[i]
                
                
                entries[i]=entry
                tps[i]=tp
                signals[i]=2
                local_max[i]=highs[i+1]
                local_min[i]=lows[i+1]
                for j in range(i+1,len(opens)):
                    candle_count[i]=candle_count[i]+1
                    if lows[j] < local_min[i]:
                        local_min[i]=lows[j]
                        local_min_bar[i]=candle_count[i]
                    if highs[j]>local_max[i]:
                        local_max[i]=highs[j]
                        local_max_bar[i]=candle_count[i]

                    if lows[j] < tp and flag==0:

                        trades[i] = 1
                        close_prices[i]=tp
                        time_index[i]=i
                        
                        indication=1
                        buy_search=1
                        flag=1
                        
                        
                    elif (highs[j] > sl and flag==0) or (in_uptrends[j] == 'True'):
                        if highs[j] > sl and flag==0:
                            trades[i] = 0
                            close_prices[i]=sl
                            time_index[i]=i

                            indication=1
                            buy_search=1
                            flag=1
                            
                        if in_uptrends[j] == 'True':
                            

                            if trades[i] ==1:
                                change_index=j
                            elif trades[i] == 0 and flag ==1:
                                change_index=j
                            else:
                                trades[i] = 0
                                close_prices[i]=closes[j]
                                time_index[i]=i
                                change_index=j
                            
                            indication=1
                            buy_search=1
                            break
                    else:
                        pass
                break
        elif (indication == 1 ) & (sell_search == 0) & (buy_search == 1) & (change_index==i):
            
            buy_search= 0
            flag=0

            while (indication == 1):


                entry = closes[i]
                tp = entry + (entry * profit_perc)
                sl = entry - (entry * sl_perc)
                
                upper[i]=upper_bands[i]
                lower[i]=lower_bands[i]
                
                entries[i]=entry
                tps[i]=tp
                signals[i]=1
                local_max[i]=highs[i+1]  
                local_min[i]=lows[i+1]
                for j in range(i+1,len(opens)):
                    if lows[j] < local_min[i]:
                        local_min[i]=lows[j]
                        local_min_bar[i]=candle_count[i]
                    if highs[j]>local_max[i]:
                        local_max[i]=highs[j]
                        local_max_bar[i]=candle_count[i]
                        
                    candle_count[i]=candle_count[i]+1
                    if highs[j] > tp and flag==0 :
                        trades[i]  = 1
                        sell_search=1
                        close_prices[i]=tp
                        time_index[i]=i
                        

                        flag=1
                        indication=0
                    elif (lows[j] < sl and flag==0) or (in_uptrends[j] == 'False'):
                        if lows[j] < sl and flag==0:

                            trades[i]= 0
                            close_prices[i]=sl
                            time_index[i]=i
                            indication=0
                            sell_search=1
                            flag=1
                            
                        if in_uptrends[j] == 'False':
                            
                            if trades[i] ==1:
                                change_index=j
                            elif trades[i] == 0 and flag ==1:
                                change_index=j
                            else:
                                trades[i] = 0
                                close_prices[i]=closes[j]
                                time_index[i]=i
                                change_index=j
                            
                            indication=0
                            sell_search=1
                            break

                    
                        
                    else:
                        pass
                break
        else:
            continue
        
    return entries,signals,tps,trades,close_prices,time_index,candle_count,local_max,local_min,local_max_bar,local_min_bar,upper,lower
    
    
# def df_perc_cal(trade_df,profit):
#     for i in trade_df.index:
#         if trade_df['trade'].loc[i]=='W':
#             trade_df.at[i,'percentage']=profit
#         else:
#             close=trade_df['close_price'].loc[i]
#             entry=trade_df['entry'].loc[i]
#             trade_df.at[i,'percentage']=-abs((close-entry)/entry)
#     return trade_df


@njit
def df_perc_cal(entries,closes,signals,percentages):
    for i in range(0,len(entries)):
        if signals[i]=='Buy':
            percentages[i]=(closes[i]-entries[i])/entries[i]
        else:
            percentages[i]=-(closes[i]-entries[i])/entries[i]
    return percentages


# def histroy(trade_df):
#     total_amount=1000
#     amount=1000
#     for i in trade_df.index:
#         PNL=amount*trade_df['percentage'].loc[i]
#         date=trade_df['open_time'].loc[i]
#         signal=trade_df['signal'].loc[i]
#         total_amount=total_amount+PNL
#     return total_amount

@njit
def histroy(percentages):
    total_amount=1000
    amount=1000
    for i in range(0,len(percentages)) :
        PNL=amount*percentages[i]
        total_amount=total_amount+PNL
    return total_amount-1000


def signal_decoding(x):
    if x == 1:
        return 'Buy'
    else:
        return 'Sell'
    
def trade_decoding(x):
    if x > 0:
        return 'W'
    else:
        return 'L'
    

def moving(coin):
    print(coin)
    category_=['best','safe','safe_n_max']  #safe_n_max : above 95% 
    for cat in category_:
        if cat=='best':
            df=pd.read_csv('combined_best.csv')
        elif cat=='safe':
            df=pd.read_csv('combined_safe.csv')
        else:
            df=pd.read_csv('combined_max.csv')
        
        
        df_coin=df[df['coin']==coin].reset_index()
        for i,tf in enumerate(df_coin['timeframe']):
            if not os.path.exists(f'data/individual/{coin}/final_{tf}/{cat}'):
                                    os.makedirs(f'data/individual/{coin}/final_{tf}/{cat}')
            atr1=df_coin[df_coin['timeframe']==tf]['atr1'][i]
            period=df_coin[df_coin['timeframe']==tf]['period'][i]
            profit=df_coin[df_coin['timeframe']==tf]['profit'][i]
            if tf =='1h' or tf == '4h' or tf == '2h':
                SL_list=np.linspace(0.005,0.1,25).tolist()
            else:
                SL_list=np.linspace(0.005,0.2,25).tolist()
                
            SL_list.append(100)
            for sl in SL_list:
                try:
                    print(f'copying data/individual/{coin}/percentages_{tf}/{coin}_atr{atr1}_preriod{int(period)}_tp{profit}_sl{round(sl,3)}.csv')
                    shutil.copy2(f'data/individual/{coin}/percentages_{tf}/{coin}_atr{atr1}_preriod{int(period)}_tp{profit}_sl{round(sl,3)}.csv', f'data/individual/{coin}/final_{tf}/{cat}')
                except FileNotFoundError:
                    shutil.copy2(f'data/individual/{coin}/percentages_{tf}/{coin}_atr{int(atr1)}_preriod{int(period)}_tp{profit}_sl{round(sl,3)}.csv', f'data/individual/{coin}/final_{tf}/{cat}')
def calculate_min_max(x):
    if x['signal'] == 'Buy':
        return (((x['local_max'] - x['entry'])/x['entry']),
                (x['entry'] - x['local_min'])/x['entry'])
    else:
        return ((x['entry'] - x['local_min'])/x['entry'],
               ( x['local_max'] - x['entry'])/x['entry'])
                                    
def create_signal_df(super_df,df,coin,timeframe,atr1,period,profit,sl):
    opens=super_df[f'open'].to_numpy(dtype='float64')
    highs=super_df[f'high'].to_numpy(dtype='float64')
    lows=super_df[f'low'].to_numpy(dtype='float64')
    closes=super_df[f'close'].to_numpy(dtype='float64')
    in_uptrends=super_df['in_uptrend'].to_numpy(dtype='U5')
    upper_bands=super_df['upperband'].to_numpy(dtype='float64')
    lower_bands=super_df['lowerband'].to_numpy(dtype='float64')
    entries,signals,tps,trades,close_prices,time_index,candle_count,local_max,local_min,local_max_bar,local_min_bar,upper,lower=cal_numba(opens,highs,lows,closes,in_uptrends,profit,sl,upper_bands,lower_bands)
    trade_df=pd.DataFrame({'signal':signals,'entry':entries,'tp':tps,'trade':trades,'close_price':close_prices,'candle_count':candle_count,
                           'local_max':local_max,'local_min':local_min,'local_max_bar':local_max_bar,'local_min_bar':local_min_bar,'upper_band':upper,'lower_band':lower})
    # before_drop=trade_df.shape[0]
    # print(f'Number of columns before drop : {before_drop}')
    total_rows = trade_df.shape[0]
    
    trade_df_index=trade_df[trade_df['entry']!=0]
    
    indexes=trade_df_index.index.to_list()
    
    for i in indexes:
        try:
            trade_df.at[i,'TradeOpenTime']=df[df.index==i+1]['OpenTime'][(i+1)]
        except KeyError:
            trade_df.at[i,'TradeOpenTime']=(df[df.index==i]['OpenTime'][(i)]) 
    for i in indexes:
        try:
            trade_df.at[i,'signalTime']=df[df.index==i]['OpenTime'][(i)]
        except KeyError:
            trade_df.at[i,'signalTime']=(df[df.index==i]['OpenTime'][(i)])
            
    trade_df['signal']=trade_df['signal'].apply(signal_decoding)
    total_rows = trade_df.shape[0]
    trade_df.dropna(inplace=True)
    total_rows = trade_df.shape[0]                    
    entries=trade_df['entry'].to_numpy(dtype='float64')
    closes=trade_df['close_price'].to_numpy(dtype='float64')
    # trades=trade_df['trade'].to_numpy(dtype='U1')
    signals=trade_df['signal'].to_numpy(dtype='U5')
    outputs=np.zeros(len(entries))
    percentages=df_perc_cal(entries,closes,signals,outputs)
    trade_df['percentage'] = percentages.tolist()
    trade_df['trade']=trade_df['percentage'].apply(trade_decoding)
    # after_drop=trade_df.shape[0]
    # print(f'Number of columns after drop : {after_drop}')
    total_rows = trade_df.shape[0]
    trade_df=trade_df.reset_index(drop=True)
    total_rows = trade_df.shape[0]
    trade_df['signalTime']=pd.to_datetime(trade_df['signalTime'])
    super_df['OpenTime']=pd.to_datetime(super_df['OpenTime'])
    total_rows = trade_df.shape[0]
    trade_df=pd.merge(trade_df, super_df, how='left', left_on=['signalTime'], right_on = ['OpenTime'])
    total_rows = trade_df.shape[0]
    trade_df=trade_df[['signal',
    'entry',
    'tp',
    'trade',
    'close_price',
    'TradeOpenTime',
    'percentage',
    'OpenTime',
    'hour',
    'minute','day',
    'month',
    'size','ma_7','ma_25','ma_99',
    'ma_100','ma_200','ema_81','ema_100','ema_100','ema_200',
    'ema_55',
    'ema_5',
    'ema_10',
    'ema_20',
    'ema_33',
    'rsi',
    'macd',
    'macdsignal',
    'macdhist',
    'slowk',
    'slowd',
    'candle_count',
    'local_max','local_min',
    'local_max_bar','local_min_bar',
    'upper_band','lower_band']]
    
    total_rows = trade_df.shape[0]
    trade_df['max_log_return'], trade_df['min_log_return'] = zip(*trade_df.apply(calculate_min_max, axis=1))
    trade_df['prev_max_log_return'] = trade_df['max_log_return'].shift(1)
    trade_df['prev_min_log_return'] = trade_df['min_log_return'].shift(1)
    trade_df['prev_local_max_bar'] = trade_df['local_max_bar'].shift(1)
    trade_df['prev_local_min_bar'] = trade_df['local_min_bar'].shift(1)
    trade_df['prev_percentage'] = trade_df['percentage'].shift(1)

    
    
    
    total_rows = trade_df.shape[0]


    trade_df=trade_df[2:]

    return trade_df


import pickle


def save_to_pkl(data, path):
    with open(path, 'wb') as file:
        pickle.dump(data, file)



