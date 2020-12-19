import sys
import os
import jwt
import uuid
import hashlib
from urllib.parse import urlencode

import requests

import json
import time

_UPBIT_API_URL = "https://api.upbit.com"
_UPBIT_SET_TIMEUNIT_MINUTE = [1, 3, 5, 15, 10, 30, 60, 240]
_UPBIT_DEFAULT_TIMEUNIT_MINUTE = 5

_UPBIT_LIMIT_ORDER_SECONDS = 5
_UPBIT_LIMIT_ORDER_MINUTES = 100
_UPBIT_LIMIT_API_SECONDS = 10
_UPBIT_LIMIT_API_MINUTES = 600

class upbitAPI():
    def __init__(self, key):

        self.access_key = key['access_key']
        self.secret_key = key['secret_key']

        self.remind_api_min = _UPBIT_LIMIT_API_MINUTES
        self.remind_api_sec = _UPBIT_LIMIT_API_SECONDS
        self.remind_order_min = _UPBIT_LIMIT_ORDER_MINUTES
        self.remind_order_min = _UPBIT_LIMIT_ORDER_SECONDS


    def req_account(self):
        ret = self.request("/v1/accounts")
        self.update_remind_api_num(ret.headers)
        return ret.json()

    def req_coinlist(self):
        query = {
            "isDetails":"false",
        }
        ret = self.request("/v1/market/all", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()

    def req_market_coininfo(self, coin_code):
        query = {
            'market': coin_code,
        }
        ret = self.request("/v1/orders/chance", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()

    def req_coininfo(self, coin_code):
        query = {
            'markets': coin_code,
        }
        ret = self.request("/v1/ticker", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()

    def req_candle_minutes(self, coin_code, count = 1, start_time = None, time_unit = _UPBIT_DEFAULT_TIMEUNIT_MINUTE):

        if start_time is not None:
            query = {
                'market': coin_code,
                "to": start_time,
                "count" : str(count)
            }
        else:
            query = {
                'market': coin_code,                
                "count" : str(count)
            }
        
        if (time_unit not in _UPBIT_SET_TIMEUNIT_MINUTE):
            print('error req_candle_minute, not exist time_unit in set:', _UPBIT_SET_TIMEUNIT_MINUTE)
            return []

        ret = self.request("/v1/candles/minutes/" + str(time_unit), query)
        self.update_remind_api_num(ret.headers)
        return ret.json()      

    def req_candle_days(self, coin_code, count = 1, start_time = None):

        if start_time is not None:
            query = {
                'market': coin_code,
                "to": start_time,
                "count" : str(count)
            }
        else:
            query = {
                'market': coin_code,                
                "count" : str(count)
            }

        ret = self.request("/v1/candles/days", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()    

    def req_candle_weeks(self, coin_code, count = 1, start_time = None):

        if start_time is not None:
            query = {
                'market': coin_code,
                "to": start_time,
                "count" : str(count)
            }
        else:
            query = {
                'market': coin_code,                
                "count" : str(count)
            }

        ret = self.request("/v1/candles/weeks", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()    

    def req_candle_months(self, coin_code, count = 1, start_time = None):

        if start_time is not None:
            query = {
                'market': coin_code,
                "to": start_time,
                "count" : str(count)
            }
        else:
            query = {
                'market': coin_code,                
                "count" : str(count)
            }

        ret = self.request("/v1/candles/months", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()   

    def req_orderbook(self, coin_code):

        query = {
            'markets': coin_code
        }

        ret = self.request("/v1/orderbook", query)
        self.update_remind_api_num(ret.headers)
        return ret.json()   

    def request(self, api, query = None):

        if api.find('order') >= 0:
            if self.remind_order_sec == 0:
                time.sleep(1)
            elif self.remind_order_min == 0:
                time.sleep(60)
        else:
            if self.remind_api_sec == 0:
                time.sleep(1)
            elif self.remind_api_min == 0:
                time.sleep(60)

        headers = self.create_header(query)
        res = requests.get(_UPBIT_API_URL + api, params=query, headers=headers)

        return res

    def update_remind_api_num(self, header):
        remind_str = header['Remaining-Req']
        remind_str = remind_str.replace(' ', '')
        remind_str_set = remind_str.split(';')
        remind_min = int(remind_str_set[1][4])
        remind_sec = int(remind_str_set[2][4])
        self.remind_api_min = remind_min
        self.remind_api_sec = remind_sec

    def update_remind_order_num(self, header):
        remind_str = header['Remaining-Req']
        remind_str = remind_str.replace(' ', '')
        remind_str_set = remind_str.split(';')
        remind_min = int(remind_str_set[1][4])
        remind_sec = int(remind_str_set[2][4])
        self.remind_order_min = remind_min
        self.remind_order_sec = remind_sec        

    def create_header(self, query = None):

        if query != None:
            query_string = urlencode(query).encode()
            hash_val = hashlib.sha512()
            hash_val.update(query_string)
            query_hash = hash_val.hexdigest()
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }            
        else:
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
            }

        jwt_token = jwt.encode(payload, self.secret_key).decode('utf8')
        authorization_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorization_token}

        return headers


if __name__ == '__main__':
    with open('key.json', 'r') as f:
        key = f.read()

    key = json.loads(key)
    upbit_api = upbitAPI(key)
    account_info = upbit_api.req_account()
    # print(account_info)

    # coin_list = upbit_api.req_coinlist()
    # print(coin_list)

    # coin_info = upbit_api.req_coininfo('KRW-ETH')
    # print(coin_info)

    # cur_time = datetime.datetime.utcnow()
    # start_time = cur_time - datetime.timedelta(days=3)
    # start_time_str = start_time.strftime('%Y-%m-%d %H:%M:%S')

    # cur_time_kst = cur_time + datetime.timedelta(hours=8)
    # start_time_kst = start_time + datetime.timedelta(hours=8)

    # print(start_time_kst)
    # candle_min = upbit_api.req_candle_minutes(coin_code='KRW-BTC', count = 5, start_time = start_time_str, time_unit = 5)
    # print(candle_min)

    # candle_week = upbit_api.req_candle_months(coin_code='KRW-BTC', count = 1, start_time = start_time_str)
    # print(candle_week)

    order_book = upbit_api.req_orderbook('KRW-BTC')
    print(order_book)

    # url = "https://api.upbit.com/v1/candles/minutes/5"

    # querystring = {"market":"KRW-BTC","to":"2018-12-08 08:05:10","count":"5"}

    # response = requests.request("GET", url, params=querystring)
    # print(response.json())

    # url = "https://api.upbit.com/v1/orderbook"

    # querystring = {"markets":"KRW-BTC,KRW-ADA"}

    # response = requests.request("GET", url, params=querystring)

    # url = "https://api.upbit.com/v1/ticker"
    # response = requests.request("GET", url, params=querystring)

    # headers = upbit_api.create_header(querystring)
    # res = requests.get(_UPBIT_API_URL + "/v1/ticker", params=querystring, headers=headers)
    # print(response)

    # market_coin_info = upbit_api.req_market_coininfo('KRW-ETH')
    # print(market_coin_info)

    # access_key = key['access_key']
    # secret_key = key['secret_key']

    # server_url = _UPBIT_API_URL

    # query = {
    #     'market': 'KRW-ETH',
    # }
    # query_string = urlencode(query).encode()

    # m = hashlib.sha512()
    # m.update(query_string)
    # query_hash = m.hexdigest()

    # payload = {
    #     'access_key': access_key,
    #     'nonce': str(uuid.uuid4()),
    #     'query_hash': query_hash,
    #     'query_hash_alg': 'SHA512',
    # }
    # print('payload:',payload)
    # jwt_token = jwt.encode(payload, secret_key).decode('utf-8')
    # authorize_token = 'Bearer {}'.format(jwt_token)
    # headers = {"Authorization": authorize_token}

    # res = requests.get(server_url + "/v1/orders/chance", params=query, headers=headers)

    # print(res.json())

    sys.exit()

