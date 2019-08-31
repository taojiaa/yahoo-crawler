import pandas as pd
import re
import numpy as np

from datetime import datetime
from urllib import request
from concurrent import futures
from bs4 import BeautifulSoup as bs
from functools import partial

class YahooCrawler:
    def __init__(self):
        
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}
        self.workers = 100
        self.freq_maps = {'monthly': 'mo', 'weekly': 'wk', 'daily': 'd'}
        
    def read_eod_data(self, symbols, start_time, end_time, freq='daily'):
        start_time = int(datetime.timestamp(datetime.strptime(start_time, '%Y-%m-%d')))
        end_time = int(datetime.timestamp(datetime.strptime(end_time, '%Y-%m-%d')))
        func = partial(self.__get_data, start_time, end_time, self.freq_maps[freq])
        with futures.ThreadPoolExecutor(self.workers) as executor:
            result = executor.map(func, symbols)
        return dict(zip(symbols, result))
        
    def __get_data(self, start_time, end_time, freq, symbol):
        url = f'https://finance.yahoo.com/quote/{symbol}/history?period1={start_time}&period2={end_time}&interval=1{freq}&filter=history&frequency=1{freq}'
        html = self.__fetch_data(url)
        data = self.__parse_data(html)
        return data
    
    def __fetch_data(self, url):
        link = request.Request(url=url, headers=self.headers)
        resp = request.urlopen(link)
        html = resp.read().decode('utf-8')
        return html

    def __parse_data(self, html):
        data = re.findall('"prices":\[(.+)\],"isPending"', html)
        if data:
            data = re.sub('null', 'np.nan', data[0])
            data = re.findall('\{("date".+?)\},', data)
            data = [eval('{' + i + '}') for i in data]
            data = [{'splitratio': float(item['denominator']) / float(item['numerator']),
                     'date': item['date']} if 'splitRatio' in item else item for item in data][::-1]
            data = pd.DataFrame(data)
            data['date'] = data['date'].apply(lambda x: datetime.fromtimestamp(x).replace(hour=0, minute=0))
            data = data.set_index('date')
            if 'splitratio' in data.columns:
                data['splitratio'] = data['splitratio'].fillna(1)
                data['splitratio'] = data['splitratio'][::-1].cumprod()[::-1]
                data['volume'] = data['volume'] / data['splitratio']
                data[['close', 'high', 'low', 'open']] = data[['close', 'high', 'low', 'open']].multiply(data['splitratio'], axis=0)
                data = data.drop('splitratio', axis=1).dropna(how='all')
            data = data.rename(columns={'adjclose': 'adjusted close'})
            ratio = data['adjusted close'] / data['close']
            data['adjusted open'] = data['open'] * ratio
            data['adjusted high'] = data['high'] * ratio
            data['adjusted low'] = data['low'] * ratio
        return data

# Run the crawler when executed directly.
if __name__ == '__main__':
    yh = YahooCrawler()
    data = yh.read_eod_data(symbols=['AAPL', 'GOOG'], 
                            start_time='2016-01-01', 
                            end_time='2019-06-30', 
                            freq='daily')
    data['AAPL'].head()