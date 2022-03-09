import json
import pandas as pd
import datetime

import upbitAPI

_DAY = 0
_WEEK = 1
_MONTH = 2
    
TIMEDIFF_KST_TO_UTC = datetime.timedelta(hours=-9)
API_DATA_MAX = 200
MARKET_PRICE = -1
class stockManager():
    def __init__(self, config):
        self.config = config
        self.upbit_api = upbitAPI.upbitAPI(self.config['key'])
    
    def getCoinTicker(self, filter=None):
        coin_list = self.upbit_api.req_coinlist()
        coin_list = coin_list['market']
            
        if filter is not None:
            tmp = coin_list[coin_list.str.contains('KRW')]
            tmp = tmp.reset_index()
            coin_list = tmp['market']

        return coin_list
    
    def getCoinCode(self):
        coin_list = self.upbit_api.req_coinlist()
        return
    
    def getData(self, ticker, timeset=None, tick=_DAY):
        if tick == _DAY:
            data = pd.DataFrame(columns=['market', 'candle_date_time_utc', 'candle_date_time_kst', \
                                        'opening_price', 'high_price', 'low_price', 'trade_price', 'timestamp', \
                                        'candle_acc_trade_price', 'candle_acc_trade_volume', \
                                        'prev_closing_price', 'change_price', 'change_rate'])
        else:
            data = pd.DataFrame(columns=['market', 'candle_date_time_utc', 'candle_date_time_kst', \
                                        'opening_price', 'high_price', 'low_price', 'trade_price', 'timestamp', \
                                        'candle_acc_trade_price', 'candle_acc_trade_volume', 'unit'])

        if timeset is not None:
            if type(timeset) == list:
                timebuf = []
                for t in timeset:
                    t = _correctTimezone(t)
                    if t is not None:
                        timebuf.append(t)
                    else:
                        return None
                timeset = timebuf
            else:
                timeset = _correctTimezone(timeset)
        
        if timeset is None:
            data = self._getData(ticker, datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'), tick=tick)
            
        elif type(timeset) == list:
            if type(timeset[0]) == str:
                datetimeset = []
                for t in timeset:
                    datetimeset.append(datetime.datetime.strptime(t, '%Y-%m-%dT%H:%M:%SZ'))
                    
            if datetimeset[0] > datetimeset[1]:
                timetmp = datetimeset[1]
                datetimeset[1] = datetimeset[0]
                datetimeset[0] = timetmp
            timediff = datetimeset[1] - datetimeset[0]
            
            if tick == _DAY:
                timelist = _sliceTimeset(datetimeset[0], datetimeset[1], tick)
                left_count = timediff.days+1
                
            else:
                timelist = _sliceTimeset(datetimeset[0], datetimeset[1], tick)
                left_count = int(timediff.total_seconds()/60/tick)+1
                
            cur_count = 0                
            for time in timelist:
                if left_count > API_DATA_MAX:
                    cur_count = API_DATA_MAX
                else:
                    cur_count = left_count
                data0 = self._getData(ticker, time, count=cur_count, tick=tick)                    
                left_count -= len(data0)
                data = data.append(data0)
            
            data.sort_values(by='candle_date_time_kst', inplace = True, ignore_index = True)
            data.reset_index(inplace = True)      
            data = data.drop('index', axis=1)              
                
        else:
            data = self._getData(ticker, timeset, tick=tick)

        if len(data) > 0:
            if tick == _DAY:
                data = data.drop(['market', 'candle_date_time_utc', 'timestamp', \
                                'candle_acc_trade_price', 'prev_closing_price', 'change_price', 'change_rate'], axis=1) 
            else:
                data = data.drop(['market', 'candle_date_time_utc', 'timestamp', \
                                    'candle_acc_trade_price', 'unit'], axis=1) 

            data = data.rename(columns={'candle_date_time_kst':'date', 'opening_price':'open', 'high_price':'high', 'low_price':'low', 'trade_price':'close', 'candle_acc_trade_volume':'volume'}) 
            data["open"]=data["open"].apply(pd.to_numeric,errors="coerce") 
            data["high"]=data["high"].apply(pd.to_numeric,errors="coerce") 
            data["low"]=data["low"].apply(pd.to_numeric,errors="coerce") 
            data["close"]=data["close"].apply(pd.to_numeric,errors="coerce") 
            data["volume"]=data["volume"].apply(pd.to_numeric,errors="coerce") 
            
            data['date'] = pd.to_datetime(data['date'])
            # data = data.set_index('date')
        
        return data
        
    def _getData(self, ticker, time, count=1, tick=_DAY):        
        if type(time) == str:
            timestr = time                
        elif type(time) == datetime.datetime:
            timestr = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            print('Not support time type')
            return None
        
        if tick == _DAY:
            return self.upbit_api.req_candle_DAYs(ticker, count, timestr)
        else:
            return self.upbit_api.req_candle_minutes(ticker, count, timestr, tick)
        
    def buy(self, ticker, price=MARKET_PRICE, volume=None, money=None):
        if money is not None:
            pass
        elif volume is not None:
            pass
            
        return True
        
    def sell(self, ticker, price=MARKET_PRICE, volume=None, money=None):
        if money is not None:
            pass
        elif volume is not None:
            pass            
        
        return True

def _sliceTimeset(time0, time1, tick, slice=API_DATA_MAX):
    if time0 > time1:
        time = time1
        time1 = time0
        time1 = time
    
    timeset = [time1]
    timetmp = time1
    if tick == _DAY:
        timetmp -= datetime.timedelta(days=slice)
    else:
        deltatime = slice*tick
        timetmp -= datetime.timedelta(minutes=deltatime)
    while timetmp > time0:        
        timeset.append(timetmp)
        if tick == _DAY:
            timetmp -= datetime.timedelta(days=slice)
        else:
            timetmp -= datetime.timedelta(minutes=deltatime)
        
    return timeset
        
def _correctTimezone(time):
    if type(time) == str:
        datetimeset = datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
        datetimeset += TIMEDIFF_KST_TO_UTC
        time = datetimeset.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif type(time) == datetime.datetime:
        time += TIMEDIFF_KST_TO_UTC
    else:
        print('Not support time type')
        return None
        
    return time

    
if __name__ == '__main__':
    with open('key.json', 'r') as f:
        key = f.read()
    key = json.loads(key)
    
    config = dict()
    config['key'] = key
    
    stock_mg = stockManager(config)
    coin_list = stock_mg.getCoinTicker(filter='KRW')
    print(coin_list)
    
    data = stock_mg.getData(coin_list[0], tick=_DAY)
    print(data)
    
    data = stock_mg.getData(coin_list[0], tick=10)
    print(data)
    
    data = stock_mg.getData(coin_list[0], '2022-02-24T02:10:00Z', tick=10)
    print(data)
    
    data = stock_mg.getData(coin_list[0], ['2020-05-17T19:16:00Z', '2020-05-05T17:06:00Z'], tick=30)
    print(data)
    
    data = stock_mg.getData(coin_list[0], ['2021-05-17T19:16:00Z', '2020-05-05T17:06:00Z'], tick=_DAY)
    print(data)