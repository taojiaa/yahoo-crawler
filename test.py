from yhcrawler import YahooCrawler

yh = YahooCrawler()
data = yh.read_eod_data(symbols=['AAPL', 'GOOG', 'GE'],
                        start_time='2016-01-01',
                        end_time='2019-06-30',
                        freq='daily')
print(data['AAPL'].head())
print(data['GOOG'].head())
