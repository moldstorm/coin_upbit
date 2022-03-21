import json
import pandas as pd
import datetime
import numpy as np

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
        df['MACDosc'] = df['MACD'] - df['MACDsignal']
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
        return self.data.iloc[-1].copy()
        
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
        self.data['MACDosc'] = macd['MACDosc']
        self.data['breakout'] = breakout

        if len(self.data) == self.config['max_tick']:
            # print(self.data.iloc[-2:])
            if prv_state == EVENT_WAIT:
                state = self.checkBuy()
            elif prv_state == EVENT_HOLD:
                state = self.checkSell()
        else:
            state = EVENT_WAIT

        result_data = self.data.iloc[-1].copy()
        if state == EVENT_BUY:
            result_data['buy'] = result_data['close']
        if state == EVENT_SELL:
            result_data['sell'] = result_data['close']
        result_data['state'] = state
        
        return result_data

    def checkBuy(self):
        prv_data = self.data.iloc[-2]
        cur_data = self.data.iloc[-1]
        
        pricerate = 1 - prv_data['close']/prv_data['open']

        breakout = prv_data['breakout']
        breakout_price = cur_data['open'] + breakout
        
        if pricerate >= self.config['down_rate'] and cur_data['close'] > breakout_price and cur_data['MACDosc'] < 0: 
            if cur_data['MACDosc'] - prv_data['MACDosc'] > 0 and \
                (np.sign(cur_data['MACD']) == np.sign(cur_data['MACDsignal']) == np.sign(prv_data['MACD']) == np.sign(prv_data['MACDsignal'])):
                if abs(cur_data['MACD']) >= abs(cur_data['MACDsignal']):
                    macd_margin = cur_data['MACD']*self.config['MACD_margin_rate']
                else:
                    macd_margin = cur_data['MACDsignal']*self.config['MACD_margin_rate']
            
                if cur_data['MACD'] < -macd_margin and cur_data['MACDsignal'] < -macd_margin:
                    self.buy_price = cur_data['close']
                    return EVENT_BUY
                elif cur_data['MACD'] > macd_margin and cur_data['MACDsignal'] > macd_margin and cur_data['MACD'] > prv_data['MACD'] and cur_data['MACDsignal'] > prv_data['MACDsignal']:
                    self.buy_price = cur_data['close']
                    return EVENT_BUY
                else:
                    return EVENT_WAIT
            else:
                return EVENT_WAIT
        else:
            return EVENT_WAIT

    def checkSell(self):
        prv_data = self.data.iloc[-2]
        cur_data = self.data.iloc[-1]
        # change_prv_osc = abs(self.data.iloc[-2]['MACDosc'] - self.data.iloc[-3]['MACDosc'])

        rate = cur_data['close'] / self.buy_price

        # if cur_data['MACDosc'] > change_prv_osc or rate <= (1-self.config['down_margin_rate']):
        if cur_data['MACDosc'] > 0:
            return EVENT_SELL
        elif prv_data['MACD'] > 0 and prv_data['MACDsignal'] > 0 \
            and cur_data['MACD'] > 0 and cur_data['MACDsignal'] > 0:
            return EVENT_SELL
        elif rate <= (1-self.config['down_margin_rate']):
            return EVENT_SELL
        elif self.data.iloc[-3]['MACDosc'] > self.data.iloc[-2]['MACDosc'] > self.data.iloc[-1]['MACDosc']:
            return EVENT_SELL
        else:
            return EVENT_HOLD


class volatility(strategy):
    def __init__(self, config):
        super().__init__(config)
        self.config = config
        self.column_set = config['column_set']
        self.buy_price = 0
        self.start_wait = True
        self.sell_time = None
        self.last_time = None
        self.resetData()
        self.merge_data = pd.DataFrame(columns=self.config['column_set'])
        
    def insertData(self,data):
        if self.data is None:
            self.resetData()
            
        if self.config['max_tick'] <= self.data_cnt:
            self.data = self.data.drop(self.data.index[0])
        
        self.data = pd.concat([self.data, data])
        self.data_cnt = len(self.data)
        
        self.last_time = data['date'].iloc[0]
        
        if (self.last_time.minute+1) % self.config['merge_tick'] == 0 and self.data_cnt > 0:
            data_list = [[self.data['open'].iloc[0], self.data['high'].max(), self.data['low'].min(), self.data['close'].iloc[-1], self.data['volume'].sum(), 0]]
            self.merge_data = pd.DataFrame(data_list, columns=self.config['column_set'])
            breakout = self.algo.calcVolatilityBreakOut(self.merge_data, self.config['k'])
            self.merge_data['breakout'] = breakout
            self.merge_data['date'] = self.data['date'].iloc[0]
            # self.merge_data['date'] += datetime.timedelta(minutes=1)
            
            # self.data = self.data.drop(self.data.index[-1])
            
        
    def resetData(self):
        self.data_cnt = 0
        self.data = pd.DataFrame(columns=self.config['column_set'])

    def run(self, data, prv_state):
        if self.start_wait == True:
            date = data['date'].iloc[0]
            if date.minute % self.config['merge_tick'] != 0:
                data['state'] = EVENT_WAIT
                self.start_wait = True
                return data.squeeze()
            else:
                self.start_wait = False
        
        self.insertData(data)        
        if prv_state == EVENT_WAIT:
            state = self.checkBuy()
        elif prv_state == EVENT_HOLD:
            state = self.checkSell()
            
        result_data = self.data.iloc[-1].copy()
        if state == EVENT_BUY:
            result_data['buy'] = result_data['close']
        if state == EVENT_SELL:
            result_data['sell'] = result_data['close']
        result_data['state'] = state
        # if len(self.merge_data) > 0:            
            # result_data['merge_high'] = self.merge_data['high'][0]
            # result_data['merge_low'] = self.merge_data['low'][0]
            # result_data['merge_open'] = self.merge_data['open'][0]
            # result_data['merge_close'] = self.merge_data['close'][0]
            # result_data['merge_date'] = self.merge_data['date'][0]
            
        result_data['merge_data'] = self.merge_data
        result_data['merge_tick'] = self.config['merge_tick']
                
        return result_data

    def checkBuy(self):
        if len(self.merge_data) == 0 or len(self.data) == 0:
            return EVENT_WAIT
        
        cur_data = self.data.iloc[-1]
        merge_data = self.merge_data.iloc[0]
        breakout_price = merge_data['close'] + merge_data['breakout']
        
        change_rate = abs(merge_data['close']/merge_data['open']-1)

        if cur_data['close'] > breakout_price and merge_data['open'] > merge_data['close'] and change_rate >= self.config['down_rate']:
        # if cur_data['close'] > breakout_price and change_rate >= self.config['down_rate']:
            self.buy_price = cur_data['close']
            time_offset = self.config['merge_tick'] - cur_data['date'].minute % self.config['merge_tick'] - 1
            self.sell_time = cur_data['date'] + datetime.timedelta(minutes=time_offset)
            # print('Buy,',cur_data['date'])
            return EVENT_BUY
        else:
            return EVENT_WAIT

    def checkSell(self):
        if self.last_time >= self.sell_time:
            return EVENT_SELL
        else:
            return EVENT_HOLD

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
        state = EVENT_WAIT
        ret_data = self.strategy.run(data, self.state)
        self.state = ret_data['state']
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

        # ret_data.replace({'state': state})
        ret_data['state'] = state
        return ret_data
                
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
    volatilityMACD_config['down_rate'] = 0.002
    volatilityMACD_config['down_margin_rate'] = 0.01
    volatilityMACD_config['MACD_margin_rate'] = 0.01
    volatilityMACD_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'MACD', 'MACDsignal', 'MACDosc', 'breakout']
        
    volatility_config = dict()
    volatility_config['base_tick'] = 1  # 1 minute
    volatility_config['merge_tick'] = 5    # 15 minutes
    volatility_config['max_tick'] = volatility_config['merge_tick']*1
    volatility_config['k'] = 0.6
    volatility_config['k_margin'] = 0.1
    volatility_config['down_rate'] = 0.002
    volatility_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'breakout']        


    config['default'] = default_config
    config['volatilityMACD'] = volatilityMACD_config
    config['volatility'] = volatility_config
    config['strategy_list'] = ['default', 'volatilityMACD', 'volatility']
    
    config['strategy'] = 'volatility'
        
    st_mg = strategyManager(config)

    # dataSet = stock_mg.getData('KRW-BTC', ['2020-01-01T09:00:00Z', '2020-01-08T09:00:00Z'], tick=1)

    # with open("data.pickle","wb") as fw:
    #     pickle.dump(dataSet, fw)

    with open("data.pickle","rb") as fr:
        dataSet = pickle.load(fr)
    
    total_rate = 1
    event_cnt = 0
    
    total_data = pd.DataFrame()
    for index, data in dataSet.iterrows():
        data = data.to_frame()
        data = data.transpose()        
        ret_data = st_mg.run(data)
        total_data = pd.concat([total_data, ret_data.to_frame().transpose()])
        if 'buy' in ret_data.keys():
            print(f"- Buy  {ret_data['date']} {ret_data['buy']}")
        if 'sell' in ret_data.keys():
            print(f"  Sell {ret_data['date']} {ret_data['sell']}")
