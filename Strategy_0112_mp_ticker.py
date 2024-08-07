import os
import multiprocessing
import pandas as pd
import time
from datetime import datetime
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
from dto.OrderDTO import OrderDTO

date_text = '2024-06-24'

def run_strategy_0112(symbol, date_text, prev_date_text, prev_date_ticker_table_name):
    #mean_of_prev_buy_volume = ttm.get_mean_buy_volume_from_ticker_table_of_date_text(symbol, prev_date_text)

    df1m = ttm.get_df1m_from_ticker_table_of_date_text(symbol, date_text)

    try:
        # add net and total columns for tickQty, volume, id. in CamelCase
        df1m['netTickQty'] = df1m['buyTickQty'] - df1m['sellTickQty']
        df1m['totalTickQty'] = df1m['buyTickQty'] + df1m['sellTickQty']
        df1m['netVolume'] = df1m['buyVolume'] - df1m['sellVolume']
        df1m['totalVolume'] = df1m['buyVolume'] + df1m['sellVolume']
        df1m['netId'] = df1m['buyId'] - df1m['sellId']
        df1m['totalId'] = df1m['buyId'] + df1m['sellId']

        # add EMA5, EMA10 of 'close' column
        df1m['close_EMA5'] = df1m['close'].ewm(span=5, adjust=False).mean()
        df1m['close_EMA10'] = df1m['close'].ewm(span=10, adjust=False).mean()
        df1m['pip_close_EMA5'] = df1m['pip_close'].ewm(span=5, adjust=False).mean()
        df1m['pip_close_EMA10'] = df1m['pip_close'].ewm(span=10, adjust=False).mean()
        df1m['netVolume_EMA5'] = df1m['netVolume'].ewm(span=5, adjust=False).mean()
        df1m['netVolume_EMA10'] = df1m['netVolume'].ewm(span=10, adjust=False).mean()
        df1m['netId_EMA5'] = df1m['netId'].ewm(span=5, adjust=False).mean()
        df1m['netId_EMA10'] = df1m['netId'].ewm(span=10, adjust=False).mean()
        df1m['netTickQty_EMA5'] = df1m['netTickQty'].ewm(span=5, adjust=False).mean()
        df1m['netTickQty_EMA10'] = df1m['netTickQty'].ewm(span=10, adjust=False).mean()
    except Exception as e:
        return None

    # add and calculate column that was required by this strategy
    # df1m['vol_signal'] = df1m['buyVolume'] > mean_of_prev_buy_volume * 10
    # df1m['vol_signal'].rolling(window=2).sum()
    # find first index that vol_signal is True
    # handle IndexError: index 0 is out of bounds for axis 0 with size 0
    # if len(df1m[df1m['vol_signal']]) == 0:
    #     # print(f"run_strategy_01 [pid:{os.getpid()}] symbol:{symbol}, df1m.size: {len(df1m)}, mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
    #     return {'buy_price': 0}
    #
    # vol_signal_start_time = df1m[df1m['vol_signal']].index[0]
    # orderStatus used for keep record of order status, 0: no order, 1: buy order, 2: sell order
    df1m['order_status'] = 0

    # last_index: (time) used for keep record of last index of df1m
    last_index = df1m.index[-1]
    buy_price = 0

    # change orders to OrderDTO like Strategy_0102_orderdto.py
    order: OrderDTO = {'buy_price': 0}

    for index in range(2, len(df1m)):
        row = df1m.iloc[index]
        this_index = row.name

        if row['order_status'] == 0:

            # find EMA5 crossover EMA10 from below
            # add condition only 1 order allow per each symbol
            # add condition from strategy 02
            # if (df1m.iloc[-1]['net_tick_qty_EMA5'] > df1m.iloc[-1]['net_tick_qty_EMA10']
            #         and df1m.iloc[-2]['net_tick_qty_EMA5'] > df1m.iloc[-2]['net_tick_qty_EMA10']
            #         and df1m.iloc[-1]['net_tick_qty_EMA5'] > 10
            if (1==1
                # and this_index >= vol_signal_start_time
                and row['close_EMA5'] > row['close_EMA10']
                and order["buy_price"] == 0
                and df1m.iloc[index - 1]['close_EMA5'] < df1m.iloc[index - 1]['close_EMA10']
                and row['netTickQty_EMA5'] > row['netTickQty_EMA10']
                and df1m.iloc[index - 1]['netTickQty_EMA5'] > df1m.iloc[index - 1]['netTickQty_EMA10']
                and row['netTickQty_EMA5'] > 10
                and df1m.iloc[index - 1]['netTickQty_EMA5'] > 10
                and row['netId_EMA5'] > 10
                and row['netId_EMA10'] > 10
            ):
                # Stop loss calculation, by pip_no, now pip_no is mean of pip_no at that minute
                # stop_loss = row['close'] - 0.0005
                print(f"BUY {symbol}: @{row.name}: EMA5 crossover EMA10 at {index}")
                # set orderStatus to 1, start from current row to last_index
                df1m.loc[this_index:last_index, 'order_status'] = 1
                # add buy record to orders dataframe
                order = {
                    'symbol': symbol,
                    'buy_price': row.high,
                    'max_price': row.high,
                    'buy_time': row.name,
                    'buy_pip_no': row.pip_close,
                    'stop_loss_pip_no': row.pip_high - 3
                }

        else:  # row['order_status'] == 1
            # increase stop loss
            if row['high'] > order['max_price']:
                # set value of orders last row ['stop_loss_pip_no] to 80% of pip_no
                # and update maxPrice of Order
                order['stop_loss_pip_no'] = row['pip_low'] - 3
                order['max_price'] = row['high']
                order['max_pip_no'] = row['pip_high']

                # print(f"{row.name}, row.high: {row.high}, orders.iloc[-1]['max_price']: {orders.iloc[-1]['max_price']}, increase stop loss")

            if row['pip_close'] < order['stop_loss_pip_no']:
                # print(f"SELL: @{row.name}: Stop loss at {index}")
                df1m.loc[this_index:last_index, 'order_status'] = 0
                sell_price = row['low']
                buy_price = 0
                # add order's sell data to orders dataframe
                order['sell_price'] = sell_price
                order['sell_time'] = row.name

    # Finally before exit, print out the result
    # print (f"run_strategy_01 [pid:{os.getpid()}] symbol:{symbol}, df1m.size: {len(df1m)}, mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
    return order

if __name__ == "__main__":
    print("Start with date_text: {0}".format(date_text))
    start_time = time.perf_counter()

    # find prev_date, don't need each thread to find prev_date again
    prev_date = stockeod.get_prev_date(date_text)
    prev_date_text = prev_date.strftime('%Y-%m-%d')
    prev_date_ticker_table_name = f"ticker_{prev_date.strftime('%Y%m%d')}"

    # symbols = for easily or controlled testing use the list ex: ['SABUY', 'MCA']
    symbols = stockeod.get_stock_list_in_date_text(prev_date_text)
    # symbols = ['SABUY']

    # creating a pool object, initializing worker function, limit to 5 workers
    pool = multiprocessing.Pool(processes=15)

    # result = pool.map(run_strategy_01, symbols)
    result = pool.starmap(run_strategy_0112, [(symbol, date_text, prev_date_text, prev_date_ticker_table_name) for symbol in symbols])

    everyDayResults = pd.DataFrame()

    # loop of above command, for dates in date_list
    dateList = ['2024-07-01', '2024-07-02', '2024-07-03', '2024-07-04', '2024-07-05', '2024-07-08']
    # dateList = ['2024-07-05', '2024-07-08']

    for date_text in dateList:
        print("Start with date_text: {0}".format(date_text))
        start_time = time.perf_counter()

        # find prev_date, don't need each thread to find prev_date again
        prev_date = stockeod.get_prev_date(date_text)
        prev_date_text = prev_date.strftime('%Y-%m-%d')
        prev_date_ticker_table_name = f"ticker_{prev_date.strftime('%Y%m%d')}"

        # symbols = for easily or controlled testing use the list ex: ['SABUY', 'MCA']
        symbols = stockeod.get_stock_list_in_date_text(prev_date_text)
        # symbols = ['SABUY']

        # creating a pool object, initializing worker function, limit to 5 workers
        pool = multiprocessing.Pool(processes=15)

        # result = pool.map(run_strategy_01, symbols)
        result = pool.starmap(run_strategy_0112, [(symbol, date_text, prev_date_text, prev_date_ticker_table_name) for symbol in symbols])

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
    # remove orders where buy_price is 0
    orders = orders[orders['buy_price'] != 0]

    end_time = time.perf_counter()
    duration = end_time - start_time
    print(f"Total execution time: {duration:.2f} seconds")