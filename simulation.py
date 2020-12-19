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

# def findQueryData(self, start_date, end_date, start_time, end_time, time_unit):

#     start_datetime = start_date + '-' + start_time
#     end_datetime = end_date + '-' + end_time

#     start_datetime_date = datetime.datetime.strptime(start_datetime, '%Y.%m.%d-%H:%M:%S')
#     end_datetime_date = datetime.datetime.strptime(end_datetime, '%Y.%m.%d-%H:%M:%S')

#     timedelta_term = end_datetime_date - start_datetime_date
#     datedelta_term = end_datetime_date.date() - start_datetime_date.date()
#     n_days = datedelta_term.days
#     days_term = timedelta_term.days
#     minutes_term = int(timedelta_term.seconds/60) + (days_term*24*60)
#     num_loop = int(minutes_term/time_unit + 0.5)
#     cnt_loop = 0

#     print('days', n_days, days_term, 'mins', timedelta_term.seconds/60)

#     spot_name = self.spot_list[0].split('-')[0]

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
    

# def findQueryTime(self, spot_id, start_time, end_time, lane, dir_type, class_type):
#     datestr = start_time.strftime('%Y.%m.%d')
#     if dir_type == -1:
#         query_set = {"date":datestr,"spotid":spot_id, "lane":int(lane), "type":int(class_type)}
#     else:
#         query_set = {"date":datestr,"spotid":spot_id, "lane":int(lane), "dir":int(dir_type), "type":int(class_type)}
#     doc_set = list(self.db_datacollector.find(query_set))

#     doc_time_set = []
#     for doc in doc_set:
#         doc_time = datetime.datetime.strptime((datestr+'-'+doc['time']), '%Y.%m.%d-%H:%M:%S')
#         if (doc_time >= start_time) and (doc_time < end_time):
#             doc_time_set.append(doc)

#     return doc_time_set


# def getInfrainfo(self):
#     infra_info = self.db_infracollector.find({},{ "_id": 0, "spotid": 1, "lanenum": 1 })
#     return list(infra_info)


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
    ratio_th_volume = 0.3

    print(test_coin)

    min_data_set = list(db_collector.find({"market":test_coin, "unit": 5}).sort('candle_date_time_utc'))
    day_data_set = list(db_collector.find({"market":test_coin, "unit": 'day'}).sort('candle_date_time_utc'))

    price_set = []
    volume_set = deque()
    price_set = deque()
    for min_data in min_data_set:
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

        if volume > day_volume*ratio_th_volume:
            print(date_str)
            print(f'avr_v:{avr_volume} day_v:{day_volume}, min_p:{price}, avr_p:{avr_price}, day_p:{day_price}')

        price_set.append(min_data['trade_price'])


    plt.plot(price_set)
    plt.show()

