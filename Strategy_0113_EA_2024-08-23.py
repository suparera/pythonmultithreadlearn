import os
import multiprocessing
import pandas as pd
import time
from datetime import datetime
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
from dto.OrderDTO import OrderDTO

# list of error symbols
error_symbols = []

def run_strategy_0112(symbol, date_text, prev_date_text, prev_date_ticker_table_name, e=None):
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

    df1m['order_status'] = 0

    df1m['totalIdCondition'] = 0
    # in df1m find first totalId > 40, then get get that index.
    # from that index, set all row after that index to totalIdCondition = 1
    # check size > 0, because if no totalId > 40, then df1m[df1m['totalId'] > 40].index[0] will raise error
    try:
        first_index = df1m[df1m['totalId'] > 40].index[0]
        df1m.loc[first_index:, 'totalIdCondition'] = 1

        # find 2nd totalId > 40, then get get that index.
        # from that index, set all row after that index to totalIdCondition = 2
        second_index = df1m[df1m['totalId'] > 40].index[1]
        df1m.loc[second_index:, 'totalIdCondition'] = 2
    except:
        error_symbols.append(symbol)

    # last_index: (time) used for keep record of last index of df1m
    last_index = df1m.index[-1]
    buy_price = 0

    # change orders to OrderDTO like Strategy_0102_orderdto.py
    order: OrderDTO = {'buy_price': 0}

    for index in range(2, len(df1m)):
        row = df1m.iloc[index]
        this_index = row.name

        # Buy Condition
        if row['order_status'] == 0:
            prevRow = df1m.iloc[index - 1]
            # print index + all buy Condition below, in the same line
            if (
                row['totalIdCondition'] == 2
                and row['pip_high'] > prevRow['pip_high']
                and row['pip_low'] > prevRow['pip_low']
                and row['totalId'] > 40
                # and second_index more than 5 minutes after first_index, and not over 20 minutes
                and 20*60> (second_index-first_index).seconds > 5 * 60

            ):
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

            # Order Close conditions. maybe StopLoss, TakeProfit, or EndOfDay
            # orderCloseCondition is integer flag to show the condition
            # 0: not close
            # 1: stop loss
            # 2: take profit
            # 3: end of day
            orderCloseCondition = 0
            if row['pip_close'] < order['stop_loss_pip_no']:  # Stop Loss
                sell_price = row['close']
                orderCloseCondition = 1
                sell_price = row['close']
                sell_condition = 'stop_loss'
            elif row['pip_high'] - order['buy_pip_no'] > 3:  # Take Profit
                orderCloseCondition = 2
                sell_price = pipu.get_price_by_pip(row['pip_high']-1)
                sell_condition = 'take_profit'
            elif row.name == df1m.index[-2]:  # End of Day
                sell_price = row['close']
                orderCloseCondition = 3
                sell_condition = 'end_of_day'

            if (orderCloseCondition > 0):
                df1m.loc[this_index:last_index, 'order_status'] = 0
                buy_price = 0
                order['sell_price'] = sell_price
                order['sell_time'] = row.name
                order['max_pip_no'] = row['pip_low']
                order['sell_condition'] = sell_condition
                print(f"SELL {symbol}: @{row.name}: Stop loss at {index}")
                break

    # Finally before exit, print out the result
    # print (f"run_strategy_01 [pid:{os.getpid()}] symbol:{symbol}, df1m.size: {len(df1m)}, mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
    return order


if __name__ == "__main__":
    everyDayResults = pd.DataFrame()

    # loop of above command, for dates in date_list
    # dateList = [ '2024-07-01'
    #     #   '2024-07-01', '2024-07-02', '2024-07-03', '2024-07-04', '2024-07-05'
    #     # , '2024-07-08', '2024-07-09', '2024-07-10', '2024-07-11', '2024-07-12'
    #     , '2024-07-12', '2024-07-16', '2024-07-17', '2024-07-18', '2024-07-19'
    # ]
    dateList = ['2024-08-23']

    for date_text in dateList:
        print("Start with date_text: {0}".format(date_text))
        start_time = time.perf_counter()

        # find prev_date, don't need each thread to find prev_date again
        prev_date = stockeod.get_prev_date(date_text)
        prev_date_text = prev_date.strftime('%Y-%m-%d')
        prev_date_ticker_table_name = f"ticker_{prev_date.strftime('%Y%m%d')}"

        # symbols = for easily or controlled testing use the list ex: ['SABUY', 'MCA']
        # symbols = stockeod.get_stock_list_in_date_text(prev_date_text)
        symbols = ['EA']


        # creating a pool object, initializing worker function, limit to 5 workers
        pool = multiprocessing.Pool(processes=15)

        # result = pool.map(run_strategy_01, symbols)
        result = pool.starmap(run_strategy_0112,
                              [(symbol, date_text, prev_date_text, prev_date_ticker_table_name) for symbol in symbols])

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

    # if orders size is 0, then print out error_symbols, and exit
    if len(orders) == 0:
        print(f"error_symbols: {error_symbols}")
        exit()

    orders = orders[orders['buy_price'] != 0]

    # print all orders, that show all columns
    pd.set_option('display.max_columns', None)
    print(orders)

    end_time = time.perf_counter()
    duration = end_time - start_time
    print(f"Total execution time: {duration:.2f} seconds")
