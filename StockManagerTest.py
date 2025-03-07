import os
import multiprocessing
import warnings
from decimal import Decimal

import pandas as pd
import time
from datetime import datetime, timedelta
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.symbol_1m_man as s1man
import utils.pip_utils as pipu
from a2.StockManager import StockManager
from dto.OrderDTO import OrderDTO
from tabulate import tabulate

# Suppress FutureWarning
warnings.filterwarnings("ignore", category=FutureWarning)

# list of error symbols
error_symbols = []

# time format
time_format = '%H:%M:%S'

def run_stock_manager_test(symbol, date_text, prev_date_text, start_time):

    # create StockManager of symbol
    prev_date_close_pip = stockeod.get_prev_date_close_pip(symbol, prev_date_text)
    stock_manager = StockManager(date_text=date_text, symbol=symbol, prev_day_close_pip=prev_date_close_pip, start_time=start_time)

    # now loading ticker data for this symbol and date
    df_1m = s1man.get_by_date_symbol(date_text, symbol)


    start_time = timedelta(hours=start_time.hour, minutes=start_time.minute)
    for i in range(500):
        end_time = start_time + timedelta(minutes=i)
        df_1m_this = df_1m[df_1m['time'] == end_time]

        # append ticker data to stock_manager
        stock_manager.append_df1m_ticker_data(end_time, df_1m_this)

    return stock_manager.get_order()

def find_opportunities_risks(order):
    s1man.find_opportunities_risks(symbol, time, buy_pip_no)

if __name__ == "__main__":
    everyDayResults = pd.DataFrame()
    dateList = ['2024-10-22']
    # dateList = ['2024-10-15', '2024-10-16', '2024-10-17', '2024-10-18']
    start_time = time.perf_counter()
    for date_text in dateList:
        print("Start with date_text: {0}".format(date_text))
        stock_market_start_time = ttm.get_ticker_time(date_text)

        # find prev_date, don't need each thread to find prev_date again
        prev_date = stockeod.get_prev_date(date_text)
        prev_date_text = prev_date.strftime('%Y-%m-%d')
        symbols = stockeod.get_stock_list_in_date_text(prev_date_text)
        # symbols = ['STEC', 'proen', 'scn', 'medeze']

        # creating a pool object, initializing worker function, limit to 5 workers
        pool = multiprocessing.Pool(processes=15)

        # result = pool.map(run_strategy_01, symbols)
        result = pool.starmap(run_stock_manager_test, [(symbol, date_text, prev_date_text, stock_market_start_time) for symbol in symbols])

        # # Clean result, remove None and buy_price = 0
        result = [order for order in result if order is not None]

        # append all order row in result to everyDayResults, to show as DataFrame that have ohlc fields as columns
        new_data = pd.DataFrame(result)


        # Append the new data to everyDayResults
        everyDayResults = pd.concat([everyDayResults, new_data], ignore_index=True)

    # # change everyDayResults to DataFrame
    # everyDayResults = pd.DataFrame(everyDayResults)
    #
    # ## Comment for faster, no need for now because we have tabulate
    # # export everyDayResults as Excel files
    # # everyDayResults.to_excel('everyDayResults.xlsx', index=False)
    #
    # # convert result(list of dict) to dataframe
    orders = pd.DataFrame(everyDayResults)
    try:
        for index, order in orders.iterrows():
            symbol = order['symbol']
            date_text = order['date']
            buy_time = order['buy_time']
            buy_price = order['buy_price']
            buy_pip_no = order['buy_pip_no']
            stop_loss_pip_no = order['sl_pip_no']
            # convert buy_time from timedelta to str hh:mm:ss
            buy_time = (datetime.min + buy_time).strftime(time_format)
            oprisk = s1man.find_opportunities_risks(date_text, symbol, buy_time, buy_pip_no)

            orders.at[index, 'min_pip_close'] = oprisk['min_pip_close']
            orders.at[index, 'min_time_for_min_pip_close'] = ':'.join(str(oprisk['min_time_for_min_pip_close']).split(':')[:2])
            orders.at[index, 'max_pip_close'] = oprisk['max_pip_close']
            orders.at[index, 'min_time_for_max_pip_close'] = ':'.join(str(oprisk['min_time_for_max_pip_close']).split(':')[:2])

            priceMin = pipu.get_price_by_pip(orders.at[index, 'min_pip_close'])
            orders.at[index, 'priceMin'] = priceMin
            priceMax = pipu.get_price_by_pip(orders.at[index, 'max_pip_close'])
            orders.at[index, 'priceMax'] = priceMax
            buy_price_decimal = buy_price
            orders.at[index, 'SL%'] = f"{((buy_price_decimal - priceMin) / buy_price_decimal * 100):.2f}%"
            orders.at[index, 'TP%'] = f"{((priceMax - buy_price_decimal) / buy_price_decimal * 100):.2f}%"
    except Exception as e:
        print(f"Error: {symbol}: {e}")
        error_symbols.append(symbol)
        # print stacktrace
        import traceback
        traceback.print_exc()


    # Check for any ambiguous Series and handle them
    orders = orders.map(lambda x: x if not isinstance(x, pd.Series) else x.tolist())

    # if orders size is 0
    if orders.size == 0:
        print("No orders found")
        exit()

    # Assuming `orders` is your DataFrame
    columns_to_unlist = ['buy_price', 'buy_pip_no', 'sl_pip_no', 'sell_price', 'sell_pip_no', 'sl_price']

    for column in columns_to_unlist:
        orders[column] = orders[column].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else x)

    # Convert buy_time to hh:mm format
    orders['buy_time'] = orders['buy_time'].apply(
        lambda x: (datetime.min + x).strftime('%H:%M') if isinstance(x, pd.Timedelta) else x)

    # Convert buy_time to hh:mm format
    orders['sell_time'] = orders['sell_time'].apply(
        lambda x: (datetime.min + x).strftime('%H:%M') if isinstance(x, pd.Timedelta) else x)

    # Define the desired column order
    desired_order = ['date', 'buy_time', 'symbol', 'buy_price', 'buy_pip_no', 'sl_price', 'sl_pip_no', 'min_pip_close',
                        'min_time_for_min_pip_close', 'max_pip_close', 'min_time_for_max_pip_close', 'priceMin', 'priceMax', 'SL%', 'TP%',
                        'sell_time', 'sell_price', 'sell_pip_no', 'sell_condition']

    # Define the custom headers as a dictionary
    custom_headers = {
        'date': 'Date',
        'buy_time': 'BuyTime',
        'symbol': 'Symbol',
        'buy_price': 'price',
        'buy_pip_no': 'pip',
        'sl_price': 'SLPrice',
        'sl_pip_no': 'PipSL',
        'min_pip_close': 'PipMin',
        'min_time_for_min_pip_close': 'MinTime',
        'max_pip_close': 'PipMax',
        'min_time_for_max_pip_close': 'MaxTime',
        'priceMin': 'PriceMin',
        'priceMax': 'PriceMax',
        'SL%': 'SL%',
        'TP%': 'TP%',
        'sell_time': 'SellTime',
        'sell_price': 'SellPrice',
        'sell_pip_no': 'SellPip',
        'sell_condition': 'SellCondition'
    }

    # Reorder the DataFrame columns
    orders = orders[desired_order]

    # Convert DataFrame to list of dictionaries
    orders_list = orders.to_dict('records')

    # Print the updated DataFrame with custom headers
    print(tabulate(orders_list, headers=custom_headers, tablefmt='psql'))

    end_time = time.perf_counter()
    duration = end_time - start_time
    print(f"Total execution time: {duration:.2f} seconds")