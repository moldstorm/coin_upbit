import sys
import os

import json
import threading
import time
import numpy as np

from pymongo import MongoClient
import datetime
from openpyxl import Workbook

import upbitAPI

from matplotlib import pyplot as plt
from collections import deque

#     write_wb = Workbook()
#     for i in range(n_days+1):
#         query_date = start_datetime_date + datetime.timedelta(days=i)
#         if i != 0:
#             zerotime = datetime.time(0,0,0)
#             query_start_time = datetime.datetime.combine(query_date.date(), zerotime)
#         else:
#             query_start_time = query_date

#         sheet_name = query_date.strftime('%Y.%m.%d')
#         write_ws = write_wb.create_sheet(sheet_name)
#         wb_time_row = 3
#         wb_lane_col = 2

#         total_lane = sum(self.spot_lane) + self.num_spot*_DIR_OFFSET
#         lane_sum = [0 for i in range(total_lane)]

#         while query_start_time.day == query_date.day:
#             # print(query_start_time)
            
#             if num_loop == 0:
#                 progress = 100
#             else:
#                 progress = cnt_loop/num_loop
#             cnt_loop += 1
#             progress_val = int(progress*100 + 0.5)
#             if progress_val < 100:
#                 self.exportTool_UI.textEdit_Output.append(str(progress_val) + '%')
#             time.sleep(0.01)

#             timestr = query_start_time.strftime('%H:%M:%S')
#             write_ws.cell(row=wb_time_row, column=1).value = timestr
#             query_end_time = query_start_time + datetime.timedelta(minutes=time_unit)

#             prv_lanenum = 0
#             lane_offset_col = 0
#             for j in range(self.num_spot):
#                 spotid = self.spot_list[j]
#                 try:
#                     dir_name = spotid.split('-')[1]
#                 except:
#                     dir_name = spotid
#                 lane_num = self.spot_lane[j]
#                 write_ws.cell(row=1, column=wb_lane_col+lane_offset_col).value = dir_name

#                 right_cnt = 0
#                 left_cnt = 0
#                 # print(query_start_time)
#                 # print('offset:', lane_offset_col)
#                 for lane in range(lane_num):
#                     write_ws.cell(row=2, column=wb_lane_col+lane+lane_offset_col).value = 'lane ' + str(lane+1)
#                     straight_list = self.findQueryTime(spotid, query_start_time, query_end_time, lane+1, -1, 0)
#                     left_list = self.findQueryTime(spotid, query_start_time, query_end_time, lane+1, _DIR_LEFT, 0)
#                     right_list = self.findQueryTime(spotid, query_start_time, query_end_time, lane+1, _DIR_RIGHT, 0)
                    
#                     left_cnt += len(left_list)
#                     right_cnt += len(right_list)
#                     lane_count = len(straight_list) - (len(left_list) + len(right_list))

#                     # print(spotid, lane, lane_count)

#                     write_ws.cell(row=wb_time_row, column=wb_lane_col+lane+lane_offset_col).value = lane_count
#                     lane_sum[lane+lane_offset_col] += lane_count
                
#                 write_ws.cell(row=2, column=wb_lane_col+lane_num+lane_offset_col+0).value = 'left'
#                 write_ws.cell(row=2, column=wb_lane_col+lane_num+lane_offset_col+1).value = 'right'
#                 write_ws.cell(row=wb_time_row, column=wb_lane_col+lane_num+lane_offset_col + 0).value = left_cnt
#                 write_ws.cell(row=wb_time_row, column=wb_lane_col+lane_num+lane_offset_col + 1).value = right_cnt
#                 # print(spotid, 'left', left_cnt)
#                 # print(spotid, 'right', right_cnt)

#                 lane_sum[lane_num+lane_offset_col+0] += left_cnt
#                 lane_sum[lane_num+lane_offset_col+1] += right_cnt
#                 lane_offset_col += lane_num+_DIR_OFFSET
#                 # print(lane_sum)
            
#             wb_time_row += 1
#             query_start_time = query_end_time
#             if query_end_time > end_datetime_date:
#                 break

#         write_ws.cell(row=wb_time_row, column=1).value = "sum"
#         for lane in range(total_lane):
#             write_ws.cell(row=wb_time_row, column=wb_lane_col+lane).value = lane_sum[lane]

#     write_wb.remove(write_wb['Sheet'])
#     excel_name = self.region_name +'-'+ spot_name +'_'+ start_datetime_date.strftime('%Y%m%d-%H%M%S') +'_'+ end_datetime_date.strftime('%Y%m%d-%H%M%S') + '.xlsx'
#     write_wb.save(excel_name)
#     write_wb.close()

#     self.exportTool_UI.textEdit_Output.append('100%')
#     # self.exportTool_UI.textEdit_Output.setOverwriteMode(False)
#     self.exportTool_UI.textEdit_Output.append('Done creation.')
#     self.exportTool_UI.textEdit_Output.append(excel_name)
    
def printData(prefix, data):
    print(prefix, end=' ')
    print(f"vol:{data['candle_acc_trade_volume']:.2f}", end='\t')
    print(f"price:{data['trade_price']:.2f}", end='\t')
    print(f"high:{data['high_price']:.2f}", end='\t')
    print(f"low:{data['low_price']:.2f}", end='\t')
    print(f"open price:{data['opening_price']:.2f}")

if __name__ == '__main__':

    with open('dbinfo.json', 'r') as f:
        dbinfo = f.read()
    dbinfo = json.loads(dbinfo)

    with open('key.json', 'r') as f:
        key = f.read()
    key = json.loads(key)

    db_client = MongoClient(dbinfo['addr'])
    db_set = db_client[dbinfo['name']]
    db_collector = db_set[dbinfo['collector']]

    upbit_api = upbitAPI.upbitAPI(key)

    coin_list = upbit_api.req_coinlist()

    simul_info = db_collector.find({ "type":"simulinfo"})
    simul_info = simul_info[0]

    # for i in range(len(simul_info['coinlist'])):
        # print(simul_info['coinlist'][i], simul_info['coinname'][i])

    test_coin = 'KRW-SBD'
    avg_day = 5
    th_benefit_ratio = 1.1


    print(test_coin)

    min_data_set = list(db_collector.find({"market":test_coin, "unit": 5}).sort('candle_date_time_utc'))
    day_data_set = list(db_collector.find({"market":test_coin, "unit": 'day'}).sort('candle_date_time_utc'))

    price_set = []
    volume_set = deque()
    price_set = deque()
    print_next = 0

    # write_wb = Workbook()

    for i in range(1,len(min_data_set)-1):
        min_data = min_data_set[i]
        min_data_prev = min_data_set[i-1]
        min_data_next = min_data_set[i+1]
        date_str = min_data['candle_date_time_utc']
        date = datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        date = date - datetime.timedelta(days=1)
        date_str = date.strftime('%Y-%m-%dT%H:%M:%S')
        date_str = date_str[0:11] + '00:00:00'
        
        finded = 0
        idx = 0
        day_data = dict()
        while finded == 0 and idx < len(day_data_set):
            if day_data_set[idx]['candle_date_time_utc'] == date_str:
                finded = 1
                day_data = day_data_set[idx]
            idx += 1

        if len(day_data) <= 0:
            continue

        price = min_data['trade_price']
        high_price = min_data['high_price']
        low_price = min_data['low_price']
        open_price = min_data['opening_price']
        volume = min_data['candle_acc_trade_volume']

        if len(volume_set) < avg_day:
            volume_set.append(volume)
            price_set.append(price)
            continue
        else:
            volume_set.rotate(1)
            volume_set[0] = volume
            price_set.rotate(1)
            price_set[0] = price

        avr_volume = np.mean(volume_set)
        avr_price = np.mean(price_set)
        day_volume = day_data['candle_acc_trade_volume']
        day_price = day_data['trade_price']
        high_price_ratio = high_price/price

        benefit_ratio = min_data['trade_price']/min_data['opening_price']
        benefit_ratio_est = min_data_next['trade_price']/min_data_next['opening_price']
        if benefit_ratio > th_benefit_ratio:
            print('-'*5, date_str, '-'*5)
            print(f'benefit ratio:{benefit_ratio:.2f} next:{benefit_ratio_est:.2f}')
            printData("PREV", min_data_prev)
            printData("CUR ", min_data)
            printData("NEXT", min_data_next)
            print(f"ETC  avr volume:{avr_volume:.2f}\tavr price:{avr_price:.2f}\tday vol:{day_data['candle_acc_trade_volume']:.2f}\tday price:{day_data['trade_price']}")        

        # if print_next == 1:
        #     # print(f'Next avr_v:{avr_volume} day_v:{day_volume} p:{price} avr_p:{avr_price} high_p:{high_price} day_p:{day_price}')
        #     benefit_ratio = price/open_price
        #     if benefit_ratio > 1.10:
        #         print("-"*20, date_str)
        #         print(f'benefit ratio{benefit_ratio}')
        #         print(f'avr_v:{avr_volume} day_v:{day_volume} p:{price} avr_p:{avr_price} high_p:{high_price} day_p:{day_price}')
        #         print(prv_data)
        #     print_next = 0

        # if volume > day_volume*ratio_th_volume:
        #     # print("-"*20)
        #     # print(date_str)
        #     # print(f'avr_v:{avr_volume} day_v:{day_volume} p:{price} avr_p:{avr_price} high_p:{high_price} day_p:{day_price}')
        #     # print(prv_data)
        #     print_next = 1

        # benefit_ratio = price/open_price
        # if benefit_ratio > 1.10:
        #     print("-"*20, date_str)
        #     print(f'benefit ratio:{benefit_ratio:.2f}')
        #     print(f'day volume:{day_volume} day price:{day_price}')
        #     print(f'volume:{volume:.2f} / {prv_v:.2f}')
        #     print(f'avr volume:{avr_volume:.2f} / {prv_avr_v:.2f}')
        #     print(f'price:{price} / {prv_p}')
        #     print(f'avr price:{avr_price:.2f} / {prv_avr_p:.2f}')
        #     print(f'high price:{high_price} / {prv_high_p}')
        #     print(f'low price:{low_price} / {prv_low_p}')

        price_set.append(min_data['trade_price'])


    # plt.plot(price_set)
    # plt.show()

