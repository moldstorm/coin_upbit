import sys
import os

import json
import threading
import time
import numpy as np

import upbitAPI
from pymongo import MongoClient
import datetime

class collectorCoinInfo():
    def __init__(self, key_set, db_set):

        self.upbit_api = upbitAPI.upbitAPI(key)

        # self.param_set = param_set

        self.num_test = 0

        self.coin_info = []
        self.createInitData()

        self.update_info_thread_running = threading.Event()
        self.update_info_thread_running.clear()
        self.update_info_thread = threading.Thread(target=self.updateInfoThread, args=())
        self.update_info_thread.daemon = True

        # self.observe_coinstate_thread_running = threading.Event()
        # self.observe_coinstate_thread_running.clear()
        # self.observe_coinstate_thread = threading.Thread(target=self.observeCoinStateThread, args=())
        # self.observe_coinstate_thread.daemon = True

    def createInitData(self):

        self.account_info = self.upbit_api.req_account()

        coin_list = self.upbit_api.req_coinlist()

        for coin_name in coin_list:
            if coin_name['market'].find('KRW') < 0:
                continue
            coin_set = dict()
            coin_set['market_id'] = coin_name['market']
            coin_set['volume_avr'] = 0
            coin_set['change_avr'] = 0
            coin_set['change_trade'] = 0
            coin_set['screening'] = 1
            coin_set['price'] = 0
            try:
                coin_set['warning'] = coin_name['market_warning']
            except:
                coin_set['warning'] = 'NONE'
            self.coin_info.append(coin_set)

    def start(self):
        self.update_info_thread_running.set()
        self.update_info_thread.start()

        self.observe_coinstate_thread_running.set()
        self.observe_coinstate_thread.start()

    def stop(self):
        self.update_info_thread_running.clear()

        self.observe_coinstate_thread_running.clear()

    def join(self):
        self.update_info_thread.join()
        self.observe_coinstate_thread.join()

    def updateInfoThread(self):

        print('start updateInfoThread')

        while self.update_info_thread_running.is_set(): 
            self.updateBaseData()
            time.sleep(self.param_set['info_update_timer_sec'])

    def updateBaseData(self):
        
        volume_set = []
        for i in range(len(self.coin_info)):
            coin_info = self.coin_info[i]
            if coin_info['warning'] == 'CAUTION':
                continue

            try:
                coin_data = self.upbit_api.req_candle_days(coin_info['market_id'], self.param_set['num_get_info_days'])
            except:
                print('read fail', coin_info['market_id'])
                time.sleep(1.0)
                coin_data = self.upbit_api.req_candle_days(coin_info['market_id'], self.param_set['num_get_info_days'])
                print('read data', coin_data[0]['market'])

            avg_volume = 0
            avg_change_rate = 0
            for j in range(self.param_set['num_get_info_days']):
                avg_volume += coin_data[j]['candle_acc_trade_volume']
                avg_change_rate += coin_data[j]['change_rate']

            avg_volume /= self.param_set['num_get_info_days']
            avg_change_rate /= self.param_set['num_get_info_days']

            self.coin_info[i]['volume_avr'] = avg_volume
            self.coin_info[i]['change_avr'] = avg_change_rate
            self.coin_info[i]['change_trade'] = coin_data[0]['trade_price']/coin_data[self.param_set['num_get_info_days']-1]['trade_price'] - 1
            self.coin_info[i]['price'] = coin_data[0]['trade_price']

            print(self.coin_info[i])
            volume_set.append(avg_volume)

        print(min(volume_set))
 

if __name__ == '__main__':

    with open('dbinfo.json', 'r') as f:
        dbinfo = f.read()
    dbinfo = json.loads(dbinfo)

    with open('key.json', 'r') as f:
        key = f.read()
    key = json.loads(key)

    days_collect = 90

    db_client = MongoClient(dbinfo['addr'])
    db_set = db_client[dbinfo['name']]
    db_collector = db_set[dbinfo['collector']]

    upbit_api = upbitAPI.upbitAPI(key)

    coin_list = upbit_api.req_coinlist()

    startDay = '2020-09-18'
    endDay = '2020-12-17'

    # get day datas
    print(f'Get {days_collect} Days Data')

    coin_list_set = []
    coin_name_set = []
    for coin_set in coin_list:
        if coin_set['market'].find('KRW') < 0:
            continue

        print(coin_set['market'], coin_set['korean_name'])
        coin_list_set.append(coin_set['market'])
        coin_name_set.append(coin_set['korean_name'])

        num_days = days_collect
        coin_data = []

        if num_days <= 200:
            coin_data = upbit_api.req_candle_days(coin_set['market'], num_days)
        else:
            while num_days > 0:
                if num_days > 200:
                    read_count = 200
                else:
                    read_count = num_days

                read_data  = upbit_api.req_candle_days(coin_set['market'], read_count)
                coin_data.extend(read_data)

                num_days -= len(read_data)

        for i in range(len(coin_data)):
            coin_data[i]['unit'] = 'day'
        db_collector.insert_many(coin_data)

    simulinfo = dict()
    simulinfo['coinlist'] = coin_list_set
    simulinfo['coinname'] = coin_name_set
    simulinfo['type'] = 'simulinfo'
    simulinfo['startDay'] = startDay
    simulinfo['endDay'] = endDay

    db_collector.insert_one(simulinfo)

    # get day datas
    end_date_str = startDay
    end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d')
    print('Get 5 min Data to ', end_date)

    for coin_set in coin_list:
        if coin_set['market'].find('KRW') < 0:
            continue
        
        print(coin_set['market'])
        start_time = datetime.datetime.utcnow()

        coin_data = []
        read_date_str = endDay
        read_date = datetime.datetime.strptime(read_date_str, '%Y-%m-%d')
        while read_date >= end_date:

            start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')
            read_data  = upbit_api.req_candle_minutes(coin_set['market'], 200, start_time_str, 5)

            if len(read_data) == 0:
                print('reach end date ', coin_data[-1]['candle_date_time_utc'])
                break

            read_date_end = read_data[-1]['candle_date_time_utc']
            read_date_end_date = datetime.datetime.strptime(read_date_end, '%Y-%m-%dT%H:%M:%S')
            start_time = read_date_end_date
            read_date_str = read_date_end[0:10]
            read_date = datetime.datetime.strptime(read_date_str, '%Y-%m-%d')

            coin_data.extend(read_data)

        db_collector.insert_many(coin_data)

    sys.exit()

