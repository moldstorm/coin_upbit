import json
import pandas as pd
import sys
import datetime
import numpy as np

import pickle
    
from plotly import tools 
import plotly.offline as offline
import plotly.graph_objs as go

from plotly import tools 
import plotly.offline as offline 
import plotly.graph_objs as go
import plotly.graph_objects as go 
import plotly.subplots as ms 

import stockManager
import strategyManager

# CSS color:
#                 aliceblue, antiquewhite, aqua, aquamarine, azure,
#                 beige, bisque, black, blanchedalmond, blue,
#                 blueviolet, brown, burlywood, cadetblue,
#                 chartreuse, chocolate, coral, cornflowerblue,
#                 cornsilk, crimson, cyan, darkblue, darkcyan,
#                 darkgoldenrod, darkgray, darkgrey, darkgreen,
#                 darkkhaki, darkmagenta, darkolivegreen, darkorange,
#                 darkorchid, darkred, darksalmon, darkseagreen,
#                 darkslateblue, darkslategray, darkslategrey,
#                 darkturquoise, darkviolet, deeppink, deepskyblue,
#                 dimgray, dimgrey, dodgerblue, firebrick,
#                 floralwhite, forestgreen, fuchsia, gainsboro,
#                 ghostwhite, gold, goldenrod, gray, grey, green,
#                 greenyellow, honeydew, hotpink, indianred, indigo,
#                 ivory, khaki, lavender, lavenderblush, lawngreen,
#                 lemonchiffon, lightblue, lightcoral, lightcyan,
#                 lightgoldenrodyellow, lightgray, lightgrey,
#                 lightgreen, lightpink, lightsalmon, lightseagreen,
#                 lightskyblue, lightslategray, lightslategrey,
#                 lightsteelblue, lightyellow, lime, limegreen,
#                 linen, magenta, maroon, mediumaquamarine,
#                 mediumblue, mediumorchid, mediumpurple,
#                 mediumseagreen, mediumslateblue, mediumspringgreen,
#                 mediumturquoise, mediumvioletred, midnightblue,
#                 mintcream, mistyrose, moccasin, navajowhite, navy,
#                 oldlace, olive, olivedrab, orange, orangered,
#                 orchid, palegoldenrod, palegreen, paleturquoise,
#                 palevioletred, papayawhip, peachpuff, peru, pink,
#                 plum, powderblue, purple, red, rosybrown,
#                 royalblue, saddlebrown, salmon, sandybrown,
#                 seagreen, seashell, sienna, silver, skyblue,
#                 slateblue, slategray, slategrey, snow, springgreen,
#                 steelblue, tan, teal, thistle, tomato, turquoise,
#                 violet, wheat, white, whitesmoke, yellow,
#                 yellowgreen


class plottool():
    def __init__(self):
        pass
        
    def plotVolatilityMACD(self, data, title_name=None):        
        candle = go.Candlestick(x=data['date'],open=data['open'],high=data['high'],low=data['low'],close=data['close'], increasing_line_color = 'red',decreasing_line_color = 'blue', showlegend=False) 
        volume = go.Bar(x=data['date'], y=data['volume'], marker_color='red', legendgroup='group1', name='Volume', showlegend=False) 
        macd = go.Scatter(x=data['date'], y=data['MACD'], mode='markers+lines', line=dict(color='blue', width=2), name='MACD', legendgroup='group2', legendgrouptitle_text='MACD', marker=dict(size=3)) 
        macdSignal = go.Scatter(x=data['date'], y=data['MACDsignal'], mode='markers+lines', line=dict(dash='dashdot', color='green', width=2), name='MACD Signal', marker=dict(size=3)) 
        macdOSC = go.Bar(x=data['date'], y=data['MACDosc'], marker_color='purple', name='MACD OSC') 
                
        buy_event = go.Scatter(x=data['date'][data['buy'].notnull()], y=data['high'][data['buy'].notnull()]+30000, legendgroup='group1', name='Buy',  \
                            mode='markers', marker=dict(size=12, color='forestgreen', symbol='triangle-down', line=dict(color='black',width=1))) 
        
        sell_event = go.Scatter(x=data['date'][data['sell'].notnull()], y=data['high'][data['sell'].notnull()]+30000, legendgroup='group1', name='Sell',  \
                            mode='markers', marker=dict(size=12, color='firebrick', symbol='triangle-down', line=dict(color='black',width=1))) 

        # fig = ms.make_subplots(rows=5, cols=2, specs=[[{'rowspan':2},{}],[None,{}],[None,{}],[None,{}],[{},{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        fig = ms.make_subplots(rows=4, cols=1, specs=[[{'rowspan':2}], [{}], [{}], [{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        # fig = ms.make_subplots(rows=4, cols=1, specs=[[{'rowspan':2},{}],[None,{}],[None,{}],[None,{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        fig.add_trace(candle,row=1,col=1)     
        fig.add_trace(buy_event,row=1,col=1)
        fig.add_trace(sell_event,row=1,col=1)
        fig.add_trace(volume,row=3,col=1)
        fig.add_trace(macd,row=4,col=1) 
        fig.add_trace(macdSignal,row=4,col=1) 
        fig.add_trace(macdOSC,row=4,col=1) 

        if title_name == None:
            title_name = ''
        fig.update_layout(autosize=True, xaxis1_rangeslider_visible=False, xaxis2_rangeslider_visible=False, margin=dict(l=50,r=50,t=50,b=50), template='seaborn', title=title_name) 
        fig.update_xaxes(tickformat='%y%m%d-%H%M%S', zeroline=True, zerolinewidth=1, zerolinecolor='black', showgrid=True, gridwidth=2, gridcolor='lightgray', showline=True,linewidth=2, linecolor='black', mirror=True) 
        fig.update_yaxes(tickformat=',d', zeroline=True, zerolinewidth=1, zerolinecolor='black', showgrid=True, gridwidth=2, gridcolor='lightgray',showline=True,linewidth=2, linecolor='black', mirror=True) 
        fig.update_traces(xhoverformat='%y%m%d-%H%M%S') 
        config = dict({'scrollZoom': True}) 
        fig.show(config=config)

    def plotVolatility(self, data, title_name=None):       
        data.reset_index(inplace = True)
            
        buy_event = data[data['state'] == strategyManager.EVENT_BUY]
        sell_event = data[data['state'] == strategyManager.EVENT_SELL]
        
        buy_event_index = data.index[data['state'] == strategyManager.EVENT_BUY].tolist()[0]
        # sell_event_index = data.index[data['state'] == strategyManager.EVENT_SELL].tolist()[0]
        
        buy_date = buy_event['date'].iloc[0]
        # sell_date = sell_event['date'].iloc[0]
        
        start_idx = buy_event_index - buy_date.minute % buy_event.iloc[0]['merge_tick']
        data = data.loc[start_idx:len(data)]
        data.drop(['merge_data'], axis=1, inplace=True)
        data.reset_index(inplace=True)
        breakout_price = float(data['open'].iloc[0]) + float(buy_event['merge_data'].iloc[0]['breakout'])
        data.insert(0,'breakout_price',value=[np.nan]*len(data))
        # data['breakout_price'] = np.nan
        for idx in range(start_idx):
            data.loc[idx,'breakout_price'] = breakout_price
        # data.loc[]
        data = pd.concat([buy_event['merge_data'].iloc[0], data])
        
        # candle_data = self.data['merge_data'] 
        candle = go.Candlestick(x=data['date'],open=data['open'],high=data['high'],low=data['low'],close=data['close'], increasing_line_color = 'red',decreasing_line_color = 'blue', showlegend=False) 
        volume = go.Bar(x=data['date'], y=data['volume'], marker_color='red', legendgroup='group1', name='Volume', showlegend=False) 
                        
        buy_event = go.Scatter(x=data['date'][data['buy'].notnull()], y=data['high'][data['buy'].notnull()]+30000, legendgroup='group1', name='Buy',  \
                            mode='markers', marker=dict(size=12, color='forestgreen', symbol='triangle-down', line=dict(color='black',width=1))) 
        
        sell_event = go.Scatter(x=data['date'][data['sell'].notnull()], y=data['high'][data['sell'].notnull()]+30000, legendgroup='group1', name='Sell',  \
                            mode='markers', marker=dict(size=12, color='firebrick', symbol='triangle-down', line=dict(color='black',width=1))) 

        breakout = go.Scatter(x=data['date'], y=data['breakout_price'], mode='lines', line=dict(color='black', width=2), name=f'Breakout {int(data.iloc[0]["breakout"])}') 
        # fig = ms.make_subplots(rows=5, cols=2, specs=[[{'rowspan':2},{}],[None,{}],[None,{}],[None,{}],[{},{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        fig = ms.make_subplots(rows=3, cols=1, specs=[[{'rowspan':2}], [{}], [{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        # fig = ms.make_subplots(rows=4, cols=1, specs=[[{'rowspan':2},{}],[None,{}],[None,{}],[None,{}]], shared_xaxes=True, horizontal_spacing=0.03, vertical_spacing=0.01) 
        fig.add_trace(candle,row=1,col=1)     
        fig.add_trace(buy_event,row=1,col=1)
        fig.add_trace(sell_event,row=1,col=1)
        fig.add_trace(breakout,row=1,col=1)
        fig.add_trace(volume,row=3,col=1)

        if title_name == None:
            title_name = ''
        fig.update_layout(autosize=True, xaxis1_rangeslider_visible=False, xaxis2_rangeslider_visible=False, margin=dict(l=50,r=50,t=50,b=50), template='seaborn', title=title_name) 
        fig.update_xaxes(tickformat='%y%m%d-%H%M%S', zeroline=True, zerolinewidth=1, zerolinecolor='black', showgrid=True, gridwidth=2, gridcolor='lightgray', showline=True,linewidth=2, linecolor='black', mirror=True) 
        fig.update_yaxes(tickformat=',d', zeroline=True, zerolinewidth=1, zerolinecolor='black', showgrid=True, gridwidth=2, gridcolor='lightgray',showline=True,linewidth=2, linecolor='black', mirror=True) 
        fig.update_traces(xhoverformat='%y%m%d-%H%M%S') 
        config = dict({'scrollZoom': True}) 
        fig.show(config=config)


class dataManager():
    def __init__(self, config=None):
        self.loadConfig(config=config)    
        self.resetData()
        
    def loadConfig(self, config=None):        
        self.config = config
        
    def insertData(self, data):
        if self.data is None:
            self.resetData()
        
        if type(data) == pd.core.series.Series:
            data = data.to_frame().transpose()
            
        self.data = pd.concat([self.data, data])
        if int(data['state']) == strategyManager.EVENT_BUY and len(self.event_tmp) == 0:
            self.event_tmp['buy_date'] = data['date'][data.index[0]]
            self.event_tmp['buy_price'] = data['close'][data.index[0]]
            self.event_tmp['buy_idx'] = data.index[0]  
        elif int(data['state']) == strategyManager.EVENT_SELL and len(self.event_tmp) > 0:
            self.event_tmp['sell_date'] = data['date'][data.index[0]]
            self.event_tmp['sell_price'] = data['close'][data.index[0]]
            self.event_tmp['sell_idx'] = data.index[0]  
            self.event_tmp['rate'] = self.event_tmp['sell_price']/self.event_tmp['buy_price']
            self.event_tmp['real_rate'] = self.event_tmp['rate'] - self.config['fee']*2
            event_data = pd.DataFrame.from_dict(self.event_tmp, orient='index').transpose()
            self.event_data = pd.concat([self.event_data, event_data], ignore_index=True)
            self.event_cnt += 1
            self.event_tmp = dict()
            
        self.data_cnt = len(self.data)
    
    def resetData(self):
        self.data = pd.DataFrame()
        self.event_data = pd.DataFrame()
        self.event_cnt = 0
        self.event_tmp = dict()
        
    def getData(self, rangelist=None):
        if rangelist is None:
            return self.data
        else:
            if len(rangelist) != 2:
                print('Range is not set')
                return self.data
            else:
                return self.data.iloc[rangelist[0]:rangelist[1]]
    
    def getEvent(self, rangelist=None):
        if rangelist is None:
            return self.event_data
        else:
            if len(rangelist) != 2:
                print('Range is not set')
                return self.event_data
            else:
                return self.event_data.iloc[rangelist[0]:rangelist[1]]
    
    def getRate(self, rangelist=None, real=False):
        if rangelist is None:
            rangelist = [0, self.event_cnt]
            
        if real == False:
            field = 'rate'
        else:
            field = 'real_rate'
            
        rate = 1
        for r in event_data.iloc[rangelist[0]:rangelist[1]][field]:
            rate *= r
            
        return rate
    
def printProgress (iteration, total, prefix = '', suffix = '', decimals = 1, barLength = 100, starttime=None, timeprefix = ''): 
    formatStr = "{0:." + str(decimals) + "f}" 
    percent = formatStr.format(100 * (iteration / float(total))) 
    filledLength = int(round(barLength * iteration / float(total))) 
    bar = '#' * filledLength + '-' * (barLength - filledLength) 
    if starttime is not None:
        elapsed_time = str(datetime.datetime.now() - starttime)
        elapsed_time = elapsed_time[:-7]
        sys.stdout.write('\r%s |%s| %s%s %s %s: %s' % (prefix, bar, percent, '%', suffix, timeprefix, elapsed_time)), 
    else:
        sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix)), 
    if iteration == total: 
        sys.stdout.write('\n') 
        sys.stdout.flush()

        
if __name__ == '__main__':
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
    volatilityMACD_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'MACD', 'MACDsignal', 'MACDhist', 'breakout']
        
    volatility_config = dict()
    volatility_config['base_tick'] = 1  # 1 minute
    volatility_config['merge_tick'] = 5    # 15 minutes
    volatility_config['max_tick'] = volatility_config['merge_tick']*1
    volatility_config['k'] = 0.5
    volatility_config['k_margin'] = 0.1
    volatility_config['down_rate'] = 0.002
    volatility_config['column_set'] = ['open', 'high', 'low', 'close', 'volume', 'breakout']        


    config['default'] = default_config
    config['volatilityMACD'] = volatilityMACD_config
    config['volatility'] = volatility_config
    config['strategy_list'] = ['default', 'volatilityMACD', 'volatility']
    
    config['strategy'] = 'volatility'
    base_tick = volatility_config['base_tick']
    merge_tick = volatility_config['merge_tick']
        
    st_mg = strategyManager.strategyManager(config)

    config = dict()
    config['fee'] = 0.0005
    config['strategy'] = 'volatility'
    data_mg = dataManager(config)

    dateset = ['2021-01-01T09:00:00Z', '2021-01-02T09:00:00Z']
    # dateset = ['2021-11-01T09:00:00Z', '2022-01-01T09:00:00Z']
    ticker = 'KRW-BTC'
    
    datefile = []
    for datestr in dateset:
        datestr = datestr.replace('-', '')
        datestr = datestr.replace(':', '')
        datefile.append(datestr)
    pickle_file = f'data_{datefile[0]}-{datefile[1]}_{base_tick}min.pickle'
    
    # dataSet = stock_mg.getData(ticker, dateset, tick=base_tick)

    # with open(pickle_file,"wb") as fw:
    #     pickle.dump(dataSet, fw)

    with open(pickle_file,"rb") as fr:
        dataSet = pickle.load(fr)
    
    total_rate = 1
    event_cnt = 0
    
    print('- Start Backtest ', ticker, dateset)
    starttime = datetime.datetime.now()
    for index, data in dataSet.iterrows():
        data = data.to_frame()
        data = data.transpose()        
        ret_data = st_mg.run(data)
        data_mg.insertData(ret_data)
        printProgress(index, len(dataSet), '', '', 1, 20, starttime)

    print()
    total_data = data_mg.getData()
    
    event_data = data_mg.getEvent()
    
    total_rate = data_mg.getRate()
    total_real_rate = data_mg.getRate(real=True)

    plot_tool = plottool()    
    plot_margin = merge_tick
    
    max_rate = -1
    max_drawdown = -1
    avr_uprate = 0
    avr_downrate = 0
    down_cnt = 0
    up_cnt = 0
    for idx, event in event_data.iterrows():
        if event['rate'] > 1:
            up_cnt += 1
            rate = event['rate'] - 1
            avr_uprate += rate
            if max_rate < rate:
                max_rate = rate
        else: # event['rate'] <= 1:
            down_cnt += 1
            rate = 1 - event['rate']
            avr_downrate += rate
            if max_drawdown < rate:
                max_drawdown = rate
        start_idx = event['buy_idx']
        end_idx = event['sell_idx']
        start_idx -= plot_margin
        if start_idx < 0:
            start_idx = 0
            
        end_idx += plot_margin
        if end_idx > len(total_data):
            end_idx = len(total_data)
            
        data = total_data.loc[start_idx:end_idx]
        plot_tool.plotVolatility(data)
        
    avr_uprate /= up_cnt
    avr_downrate /= down_cnt
    total_cnt = up_cnt + down_cnt
    
    print("- Result")
    print(f"# of Trades\t\t: {data_mg.event_cnt}")    
    print(f"  %winning\t: {up_cnt/total_cnt*100:.4f}%")
    print(f"  %losing\t: {down_cnt/total_cnt*100:.4f}%")
    print(f"Net Profit\t: {(total_rate-1)*100:.4f}%")    
    print(f"Net Profit fee\t: {(total_real_rate-1)*100:.4f}%")
    print(f"Avr Profit\t: {avr_uprate*100:.4f}%")
    print(f"Avr Losing\t: {-avr_downrate*100:.4f}%")
    print(f"Best Winning\t: {max_rate*100:.4f}%")
    print(f"Max Drawdown\t: {-max_drawdown*100:.4f}%")
    
