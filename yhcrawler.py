import re
import pandas as pd
import numpy as np
import requests

from datetime import datetime
from concurrent import futures
from functools import partial


class YahooCrawler:

    def __init__(self, workers=20):
        """
        Args:
            workers (int, optional): The maximum number of threads that can be used to execute the given calls. Defaults to 20.
        """
        self.headers = {
            'User-Agent':
            'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
        }
        self.workers = workers
        self.main_url = 'https://finance.yahoo.com/'
        self.freq_maps = {'monthly': 'mo', 'weekly': 'wk', 'daily': 'd'}

    def read_eod_data(self, symbols, start_time, end_time, freq='daily'):
        """The function for users to read the eod data from Yahoo Finance.

        Args:
            symbols (list): Symbols of requested data
            start_time (str): Start date of requested data
            end_time (str): End date of requested data
            freq (str, optional): The frequency of requested data, can be 'daily', 'weekly' or 'monthly'. Defaults to 'daily'.

        Returns:
            A dict of pd.DataFrame
        """
        st = datetime.timestamp(datetime.strptime(start_time, '%Y-%m-%d'))
        ed = datetime.timestamp(datetime.strptime(end_time, '%Y-%m-%d'))

        func = partial(self.__get_data,
                       start_time=int(st),
                       end_time=int(ed),
                       freq=self.freq_maps[freq])

        with futures.ThreadPoolExecutor(self.workers) as executor:
            result = executor.map(func, symbols)
        return dict(zip(symbols, result))

    def __get_data(self, symbol, start_time, end_time, freq):
        """Get single symbol data.

        Args:
            start_time (str): Start date of requested data
            end_time (str): End date of requested data
            freq (str, optional): The frequency of requested data, can be 'daily', 'weekly' or 'monthly'. Defaults to 'daily'.
            symbols (list): Symbols of requested data

        Returns:
            pd.DataFrame
        """
        url = self.main_url + f'quote/{symbol}/history?period1={start_time}&period2={end_time}&interval=1{freq}&filter=history&frequency=1{freq}'
        html = self.__fetch_data(url)
        data = self.__parse_data(html)
        return data

    def __fetch_data(self, url):
        """Fetch the html from url

        Args:
            url (str)

        Returns:
            str
        """
        resp = requests.get(url=url, headers=self.headers)
        html = str(resp.content, encoding='utf-8')
        return html

    def __parse_data(self, html):
        """Get the price data from html string.

        Args:
            html (str)

        Returns:
            pd.DataFrame
        """
        data = re.findall('"prices":\[(.+)\],"isPending"', html)
        if data:
            data = re.sub('null', 'np.nan', data[0])
            data = re.findall('\{("date".+?)\},', data)
            data = [eval('{' + i + '}') for i in data]
            data = [{
                'splitratio':
                float(item['denominator']) / float(item['numerator']),
                'date':
                item['date']
            } if 'splitRatio' in item else item for item in data][::-1]
            data = pd.DataFrame(data)
        data = self.__process_data(data)
        return data

    def __process_data(self, data):
        """Recalculate the original price without splits and dividends adjusted.

        Args:
            data (pd.DataFrame)

        Returns:
            pd.DataFrame
        """
        data['date'] = data['date'].apply(
            lambda x: datetime.fromtimestamp(x).replace(hour=0, minute=0))
        data = data.set_index('date')
        p_cols = ['close', 'high', 'low', 'open']

        if 'splitratio' in data.columns:
            data['splitratio'] = data['splitratio'].fillna(1)
            data['splitratio'] = data['splitratio'][::-1].cumprod()[::-1]
            data['volume'] = data['volume'] / data['splitratio']
            data[p_cols] = data[p_cols].multiply(data['splitratio'], axis=0)
            data = data.drop('splitratio', axis=1).dropna(how='all')

        ratio = data['adjclose'] / data['close']
        for col in p_cols:
            data[f'adj{col}'] = data[col] * ratio
        return data


# Run the crawler when executed directly.
if __name__ == '__main__':
    yh = YahooCrawler()
    data = yh.read_eod_data(symbols=['AAPL', 'GOOG'],
                            start_time='2016-01-01',
                            end_time='2019-06-30',
                            freq='daily')
    print(data['AAPL'].head())
    print(data['GOOG'].head())
