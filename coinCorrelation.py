import json
import pickle
import pandas as pd

import stockManager

if __name__ == '__main__':
    with open('key.json', 'r') as f:
        fileread = f.read()
    key = json.loads(fileread)
    
    config = dict()
    config['key'] = key
    
    stock_mg = stockManager.stockManager(config)

    tickers = stock_mg.getCoinTicker(filter='KRW')
    
    startDay = '2021-01-01T09:00:05Z'
    endDay = '2022-01-01T09:00:05Z'
    
    # valid_tickers = []
    # for ticker in tickers:
    #     data = stock_mg.getData(ticker, startDay, tick=stockManager._day)
    #     # print(dataSet)
    #     # print(f'Ticker:{ticker} valid:{len(dataSet)}')
    #     if len(data) > 0:
    #         valid_tickers.append(ticker)
    
    with open("corr_valid_ticker.pickle","rb") as fr:
        valid_tickers = pickle.load(fr)
    
    dataSet = pd.DataFrame()
    for ticker in valid_tickers:
        data = stock_mg.getData(ticker, [startDay, endDay], tick=stockManager._day)
        column_name = f'{ticker}_change'
        data[column_name] = (data['close'] - data['close'].shift(1) / data['close']) * 100
        dataSet = pd.concat([dataSet, data[column_name].to_frame()], axis=1)
    
    corr_matrix = dataSet.corr(method='pearson')
    corr_matrix.to_csv('corr_matrix.csv')
    
    print('='*30)
    print('- Date')
    print(f'{startDay} ~ {endDay}')
    print('-'*30)
    print('- Tickers')
    print(valid_tickers)
    print('-'*30)
    print('- Corr Matrix')
    print(corr_matrix)
    print('='*30)
    
    print(corr_matrix)
    