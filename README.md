# Yahoo Crawler

## The Basics

- A very simple crawler script that gets End of Day data from Yahoo Finance.

- Multithreading makes the batch downloader much faster.

## The Features

- This crawler only allows to download EOD data.
  
- It would be extended to scrape other information on each stock page (Profile, Statistics, Analysis, etc.).

- The Close price in Yahoo Finance is adjusted for splits, while Adjusted price is adjusted for both dividends and splits. In this module, the adjusted price doesn't change but the price data is recalculated to the non-split and non-dividends adjusted version.

## Usage

- Just pass a list of symbols that you would like to download into the function, as well as the date range and frequency. The module supports the monthly, weekly and daily EOD data.

  ~~~python
  yh = YahooCrawler()
  data = yh.read_eod_data(symbols=['AAPL', 'GOOG'],
                          start_time='2016-01-01',
                          end_time='2019-06-30',
                          freq='daily')
  ~~~

- Once the crawler finishes its job, it returns a dict of pandas.DataFrame, in which the key is the corresponding symbol.

## Installation

- The recommended installation method is [pipenv](https://docs.pipenv.org/en/latest/):

  ~~~sh
  pipenv install yhcrawler
  ~~~
