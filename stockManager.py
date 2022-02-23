import json
import pandas as pd
import datetime

import upbitAPI

_day = 0
TIMEDIFF_KST_TO_UTC = datetime.timedelta(hours=-9)
API_DATA_MAX = 200
MARKET_PRICE = -1

def _sliceTimeset(time0, time1, tick, slice=API_DATA_MAX):
    if time0 > time1:
        time = time1
        time1 = time0
        time1 = time
    
    timeset = [time1]
    timetmp = time1
    if tick == _day:
        timetmp -= datetime.timedelta(days=slice)
    else:
        deltatime = slice*tick
        timetmp -= datetime.timedelta(minutes=deltatime)
    while timetmp > time0:        
        timeset.append(timetmp)
        if tick == _day:
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

class stockManager():
    def __init__(self, config):
        self.config = config
        self.upbit_api = upbitAPI.upbitAPI(self.config['key'])
        
        pass
    
    def getCoinTicker(self, filter=None):
        coin_list = self.upbit_api.req_coinlist()
        coin_list = coin_list['market']
            
        if filter is not None:
            tmp = coin_list[coin_list.str.contains('KRW')]
            tmp = tmp.reset_index()
            coin_list = tmp['market']

        return coin_list
    
    def getData(self, ticker, timeset=None, tick=_day):
        if tick == _day:
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
            
            if tick == _day:
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
                
        else:
            data = self._getData(ticker, timeset, tick=tick)
        
        return data
        
    def _getData(self, ticker, time, count=1, tick=_day):        
        if type(time) == str:
            timestr = time                
        elif type(time) == datetime.datetime:
            timestr = time.strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            print('Not support time type')
            return None
        
        if tick == _day:
            return self.upbit_api.req_candle_days(ticker, count, timestr)
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
    
if __name__ == '__main__':
    with open('key.json', 'r') as f:
        key = f.read()
    key = json.loads(key)
    
    config = dict()
    config['key'] = key
    
    stock_mg = stockManager(config)
    coin_list = stock_mg.getCoinTicker(filter='KRW')
    print(coin_list)
    
    # data = stock_mg.getData(coin_list[0], tick=_day)
    # print(data)
    
    # data = stock_mg.getData(coin_list[0], tick=10)
    # print(data)
    
    # data = stock_mg.getData(coin_list[0], '2022-02-24T02:10:00Z', tick=10)
    # print(data)
    
    # data = stock_mg.getData(coin_list[0], ['2020-05-17T19:16:00Z', '2020-05-05T17:06:00Z'], tick=30)
    # print(data)
    
    # data = stock_mg.getData(coin_list[0], ['2021-05-17T19:16:00Z', '2020-05-05T17:06:00Z'], tick=_day)
    # print(data)