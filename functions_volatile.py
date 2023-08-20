from sqlalchemy import create_engine, inspect
import pandas as pd
from data_extraction import *
from functions import *


def get_volatile_dataframe():
    engine = create_engine('sqlite:///CryptoDB.db')

    # Fetch all table names
    inspector = inspect(engine)
    table_names = inspector.get_table_names()

    rows = []

    # Fetch row from each table and append it to rows
    for table in table_names:
        try:
            row = pd.read_sql(table, engine)
            rows.append(row.iloc[0])
        except Exception as e:
            print(f'No data available for {table}')

    # Convert the list of rows into a DataFrame
    df = pd.concat(rows, axis=1).T
    df.reset_index(drop=True , inplace = True)
    
    df['PctRange'] = (df['h'] - df['l']) / abs(df['l']) * 100
    
    df[f'Volatility'] = df['PctRange'].rolling(window=1).mean()
    
    df_volatility = df.sort_values(by='Volatility')
    
    return df_volatility
    

def get_trend_coins(str_date,end_str,timeframe,client,period,atr1,in_trade_coins):
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
            elif coin == in_trade_coins['first_trend_coin']:
                print('Already in trade with this coin, this is the first_trend_coin')
                current_trend_coin_idx -= 1
            elif coin == in_trade_coins['second_trend_coin']:
                print('Already in trade with this coin, this is the second_trend_coin')
                current_trend_coin_idx -= 1
            else:
                print('Take trade')
                found_coin_trend_1 = True
        else:
            if trade_df.iloc[-1]['max_log_return'] > 0.0414:
                print('Missed Sell , looking for another')
                current_trend_coin_idx -= 1  
            elif coin == in_trade_coins['first_trend_coin']:
                print('Already in trade with this coin, this is the first_trend_coin')
                current_trend_coin_idx -= 1
            elif coin == in_trade_coins['second_trend_coin']:
                print('Already in trade with this coin, this is the second_trend_coin')
                current_trend_coin_idx -= 1      
            else:
                print('Take trade')
                found_coin_trend_1 = True
        print(current_trend_coin_idx)   
    
    
    first_trend_coin = coin

    print(f'First trend coin is {first_trend_coin}')

    current_trend_coin_idx -= 1
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
            elif coin == in_trade_coins['first_trend_coin']:
                print('Already in trade with this coin, this is the first_trend_coin')
                current_trend_coin_idx -= 1
            elif coin == in_trade_coins['second_trend_coin']:
                print('Already in trade with this coin, this is the second_trend_coin')
                current_trend_coin_idx -= 1
            else:
                print('Take trade')
                found_coin_trend_1 = True
        else:
            if trade_df.iloc[-1]['max_log_return'] > 0.0414:
                print('Missed Sell , looking for another')
                current_trend_coin_idx -= 1  
            elif coin == in_trade_coins['first_trend_coin']:
                print('Already in trade with this coin, this is the first_trend_coin')
                current_trend_coin_idx -= 1
            elif coin == in_trade_coins['second_trend_coin']:
                print('Already in trade with this coin, this is the second_trend_coin')
                current_trend_coin_idx -= 1      
            else:
                print('Take trade')
                found_coin_trend_1 = True
        print(current_trend_coin_idx) 
    second_trend_coin = coin
    print(f'Found the second coin to trade(Ride the trend wave) : {second_trend_coin}')

    consolidation_coin_idx = 5

    found_consolidation_coin = False
    while found_consolidation_coin == False:
        consolidation_coin = volatile_df.iloc[consolidation_coin_idx]['s'][:-4]
        print(f'Currently checking gor coin {consolidation_coin}')
        if volatile_df.iloc[consolidation_coin_idx]['Volatility'] < 5:
            coin = consolidation_coin
            df=dataextract(coin,str_date,end_str,timeframe,client)
            super_df=supertrend(coin,df, period,atr1)
            trade_df=create_signal_df(super_df,df,coin,timeframe,atr1,period,100,100)
            found_consolidation_coin = True
        else:
            consolidation_coin -= 1

    consolidation_coin = coin
    print(f'Found the consolidation coin to trade : {consolidation_coin}')



