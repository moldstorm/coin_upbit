import json
import pandas as pd
import datetime

import pickle

EVENT_WAIT = 0
EVENT_BUY = 1
EVENT_SELL = 2
EVENT_HOLD = 3

class strategyAlgo():
    def __init__(self):
        pass
    
    def calcMACD(self, data, range0, range1, signal_range): # MACD
        df = pd.DataFrame()
        df['EMAfast'] = data['close'].ewm( span = range0, min_periods = range0).mean()
        df['EMAslow'] = data['close'].ewm( span = range1, min_periods = range1).mean()
        df['MACD'] = df['EMAfast'] - df['EMAslow']
        df['MACDsignal'] = df['MACD'].ewm( span = signal_range, min_periods = signal_range).mean()
        df['MACDhist'] = df['MACD'] - df['MACDsignal']
        return df
    
    def calcMovingAvg(self, data, target, range): # Moving Average
        field_name = f'smv{range}'
        df = pd.DataFrame()
        df[field_name] = data[target].rolling(window=range).mean()
        return df
    
    def calcVolatilityBreakOut(self, data, k): # VOLATILITY BREAK-OUT
        df = pd.DataFrame()
        df['breakout'] = (data['high'] - data['low']) * k
        return df

class strategy():
    def __init__(self, config):
        self.config = config
        self.algo = strategyAlgo()
        self.resetData()        
    
    def run(self, data=None):
        self.insertData(data)
        
    def insertData(self,data):
        if self.data is None:
            self.resetData()
            
        if self.config['max_tick'] <= self.data_cnt:
            self.data = self.data.drop(self.data.index[0])
            
        self.data = pd.concat([self.data, data])
        self.data_cnt = len(self.data)
        
    def resetData(self):
        self.data_cnt = 0
        self.data = pd.DataFrame(columns=self.config['column_set'])

class volatilityMACD(strategy):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.column_set = config['column_set']
        self.buy_price = 0
        self.resetData()

    def run(self, data, prv_state):
        self.insertData(data)
        macd = self.algo.calcMACD(self.data, self.config['MACDtick'][0], self.config['MACDtick'][1], self.config['MACDtick'][2])
        breakout = self.algo.calcVolatilityBreakOut(self.data, self.config['k'])

        self.data['MACD'] = macd['MACD']
        self.data['MACDsignal'] = macd['MACDsignal']
        self.data['MACDhist'] = macd['MACDhist']
        self.data['breakout'] = breakout

        if len(self.data) == self.config['max_tick']:
            # print(self.data.iloc[-2:])
            if prv_state == EVENT_WAIT:
                state = self.checkBuy()
            elif prv_state == EVENT_HOLD:
                state = self.checkSell()
        else:
            state = EVENT_WAIT

        return state

    def checkBuy(self):
        prv_data = self.data.iloc[-2]
        cur_data = self.data.iloc[-1]
        
        pricerate = 1 - prv_data['close']/prv_data['open']

        breakout = prv_data['breakout']
        breakout_price = cur_data['open'] + breakout

        if pricerate >= self.config['down_rate'] and cur_data['MACDhist'] < 0 and cur_data['close'] > breakout_price:
            self.buy_price = cur_data['close']
            return EVENT_BUY
        else:
            return EVENT_WAIT

    def checkSell(self):
        change_prv_hist = abs(self.data.iloc[-2]['MACDhist'] - self.data.iloc[-3]['MACDhist'])
        cur_data = self.data.iloc[-1]

        rate = cur_data['close'] / self.buy_price

        if cur_data['MACDhist'] > change_prv_hist or rate <= (1-volatilityMACD_config['down_margin_rate']):
            return EVENT_SELL
        else:
            return EVENT_HOLD


class volatility(strategy):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.column_set = config['column_set']
        self.buy_price = 0
        self.resetData()

    def run(self, data, prv_state):
        self.insertData(data)
        breakout = self.algo.calcVolatilityBreakOut(self.data, self.config['k'])
        self.data['breakout'] = breakout

        if len(self.data) == self.config['max_tick']:
            # print(self.data.iloc[-2:])
            if prv_state == EVENT_WAIT:
                state = self.checkBuy()
            elif prv_state == EVENT_HOLD:
                state = self.checkSell()
        else:
            state = EVENT_WAIT

        return state

    def checkBuy(self):
        prv_data = self.data.iloc[-2]
        cur_data = self.data.iloc[-1]

        breakout = prv_data['breakout']
        breakout_price = cur_data['open'] + breakout

        if cur_data['close'] > breakout_price and prv_data['open'] > prv_data['close']:
            self.buy_price = cur_data['close']
            return EVENT_BUY
        else:
            return EVENT_WAIT

    def checkSell(self):
        return EVENT_SELL

strategy_set = {'default':strategy, 'volatilityMACD':volatilityMACD, 'volatility':volatility}

class strategyManager():
    def __init__(self, config=None):
        self.loadConfig(config=config)    
        self.setStrategy()
        
    def loadConfig(self, filename='strategy.json', config=None):        
        if config is None:
            with open(filename, 'r') as f:
                fileread = f.read()
            config = json.loads(fileread)
            self.config = config
        else:
            self.config = config
        print(self.config)
        
    def setStrategy(self):
        if self.config['strategy'] in self.config['strategy_list']:
            self.strategy = strategy_set[self.config['strategy']](self.config[self.config['strategy']])
        else:
            self.strategy = strategy_set['default'](self.config[self.config['default']])
        print(f'Strategy algoritm is set {self.config["strategy"]}')
        self.state = EVENT_WAIT
        
    def run(self, data):
        rate = 1
        state = EVENT_WAIT
        self.state = self.strategy.run(data, self.state)
        if self.state == EVENT_BUY:
            self.buydata = self.strategy.data.iloc[-1].to_frame()
            self.buydata = self.buydata.transpose()    
            self.state = EVENT_HOLD
            state = EVENT_BUY
        elif self.state == EVENT_SELL:
            self.selldata = self.strategy.data.iloc[-1].to_frame()
            self.selldata = self.selldata.transpose()   
            self.state = EVENT_WAIT
            state = EVENT_SELL
            rate = float(self.selldata['close']) / float(self.buydata['close'])
            print(f'!! Event rate : {rate}')
            eventdata = self.buydata.append(self.selldata)
            print(eventdata)
        return rate, state
        
if __name__ == '__main__':
    import stockManager
    with open('key.json', 'r') as f:
        fileread = f.read()
    key = json.loads(fileread)
    
    config = dict()
    config['key'] = key
    
    stock_mg = stockManager.stockManager(config)

    config = dict()
    
    default_config = dict()
    default_config['base_tick'] = 15
    default_config['max_tick'] = 15
    default_config['column_set'] = ['open', 'high', 'low', 'close', 'volume']
    
    fee = 0.0005

    volatilityMACD_config = dict()
    volatilityMACD_config['base_tick'] = 15
    volatilityMACD_config['k'] = 0.6
    volatilityMACD_config['MACDtick'] = [12, 26, 9]
    volatilityMACD_config['max_tick'] = volatilityMACD_config['MACDtick'][1] + volatilityMACD_config['MACDtick'][2] + 1
    volatilityMACD_config['down_rate'] = 0.003
    volatilityMACD_config['down_margin_rate'] = 0.01
    volatilityMACD_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'MACD', 'MACDsignal', 'MACDhist', 'breakout']
        
    volatility_config = dict()
    volatility_config['base_tick'] = 15
    volatility_config['max_tick'] = 2
    volatility_config['k'] = 0.8
    volatility_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'breakout']        


    config['default'] = default_config
    config['volatilityMACD'] = volatilityMACD_config
    config['volatility'] = volatility_config
    config['strategy_list'] = ['default', 'volatilityMACD', 'volatility']
    
    config['strategy'] = 'volatility'
        
    st_mg = strategyManager(config)

    # dataSet = stock_mg.getData('KRW-BTC', ['2020-01-01T09:00:00Z', '2021-01-01T09:00:00Z'], tick=15)

    # with open("data.pickle","wb") as fw:
    #     pickle.dump(dataSet, fw)

    with open("data.pickle","rb") as fr:
        dataSet = pickle.load(fr)
    
    total_rate = 1
    event_cnt = 0
    for index, data in dataSet.iterrows():
        data = data.to_frame()
        data = data.transpose()        
        rate, state = st_mg.run(data)
        if state == EVENT_SELL:
            total_rate *= (rate - fee*2)
            event_cnt += 1
            print(total_rate)

    print(event_cnt, total_rate)
    
    

