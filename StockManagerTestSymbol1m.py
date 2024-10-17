import os
import multiprocessing
import pandas as pd
import time
from datetime import datetime, timedelta
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
from a2.StockManager import StockManager
from dto.OrderDTO import OrderDTO

# list of error symbols
error_symbols = []

# time format
time_format = '%H:%M:%S'

def run_stock_manager_test(symbol, date_text, prev_date_text, start_time):

    # create StockManager of symbol
    prev_date_close_pip = stockeod.get_prev_date_close_pip(symbol, prev_date_text)
    stock_manager = StockManager(symbol, prev_date_close_pip, start_time)

    # now loading ticker data for this symbol and date
    ticker_df = ttm.get_ticker_df_from_ticker_table_of_date_text(symbol, date_text)


    # loop of date start from stock_market_start_time, iter every 1 minute

    for i in range(100):
        end_time = start_time + timedelta(minutes=1)
        ticker_df_1m = ticker_df[start_time:end_time]
        start_time = end_time

        # append ticker data to stock_manager
        stock_manager.append_df1m_ticker_data(end_time, ticker_df_1m)

        # print start_time in hh:mm:ss format
        print("time: {0} - {1} tickerCount={2}".format(start_time.strftime(time_format), end_time.strftime(time_format), len(ticker_df_1m)))

    print("hi")





if __name__ == "__main__":
    dateList = ['2024-09-16']

    for date_text in dateList:
        print("Start with date_text: {0}".format(date_text))
        stock_market_start_time = ttm.get_ticker_time(date_text)

        # find prev_date, don't need each thread to find prev_date again
        prev_date = stockeod.get_prev_date(date_text)
        prev_date_text = prev_date.strftime('%Y-%m-%d')
        symbols = ['EA']


        # creating a pool object, initializing worker function, limit to 5 workers
        pool = multiprocessing.Pool(processes=15)

        # result = pool.map(run_strategy_01, symbols)
        result = pool.starmap(run_stock_manager_test, [(symbol, date_text, prev_date_text, stock_market_start_time) for symbol in symbols])

        # Clean result, remove None and buy_price = 0
        result = [order for order in result if order is not None]
        result = [order for order in result if order['buy_price'] != 0]

        # append all order row in result to everyDayResults, to show as DataFrame that have ohlc fields as columns
        new_data = pd.DataFrame(result)

        # Append the new data to everyDayResults
        everyDayResults = pd.concat([everyDayResults, new_data], ignore_index=True)

    # change everyDayResults to DataFrame
    everyDayResults = pd.DataFrame(everyDayResults)

    # export everyDayResults as Excel files
    everyDayResults.to_excel('everyDayResults.xlsx', index=False)

    # convert result(list of dict) to dataframe
    orders = pd.DataFrame(result)
    # remove orders where buy_price is 0, or not contain any buy_price

    # if orders size is 0
    if orders.size == 0:
        print("No orders found")
        exit()

    orders = orders[orders['buy_price'] != 0]

    # print all orders, that show all columns
    pd.set_option('display.max_columns', None)
    print(orders)

    end_time = time.perf_counter()
    duration = end_time - stock_market_start_time
    print(f"Total execution time: {duration:.2f} seconds")
