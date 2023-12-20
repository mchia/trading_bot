# Downloads a list of tickers daily data using Yahoo's API.
# This is only for daily timeframe as yfinance only allows the most recent 60days unless the time frame is Daily and above.

import datetime as dt
import os

import yfinance as yf

tickers_input = input("Enter a list of tickers separated by commas: ")
ticker_list = tickers_input.split(',')

print(ticker_list)

# list = ['WTC.AX','XRO.AX']
# output = '/Users/michaelchia/Documents/Python/Trading Bot/Data/ASX'

# for ticker in list:
#     data = yf.download(ticker)
#     start_date = data.index.min().date()
#     today = dt.date.today()
#     historical_data = yf.download(ticker, start = start_date, end = today, interval = '1d')
#     ticker = ticker.replace(".AX", "")
#     csv_filename = os.path.join(output, f"{ticker}_D1.csv")
#     historical_data.to_csv(csv_filename)

# print("Complete")
