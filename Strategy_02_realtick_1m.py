import os
import multiprocessing
import pandas as pd
import time
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
from dto.OrderDTO import OrderDTO
from dto.ohlc import OhlcDTO

date_text = '2024-06-13'

# simulate real raw ticker
def run_strategy(symbol, date_text, prev_date_text, prev_date_ticker_table_name):
    mean_of_prev_buy_volume = ttm.get_mean_buy_volume_from_ticker_table_of_date_text(symbol, prev_date_text)


    rawTicketDf = ttm.get_ticker_df_from_ticker_table_of_date_text(symbol, date_text)

    # get startTime and endTime
    startTime, endTime = ttm.get_start_and_end_time_of_date_and_symbol_in(date_text, symbol)

    # from startTime to endoTime, loop in 1 minute interval
    range = pd.date_range(start=startTime, end=endTime, freq='1min')

    # create df1m DataFrame, use sniffTime as index
    df1m = pd.DataFrame(columns=['sniffTime', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'qty', 'pip_open', 'pip_high', 'pip_low', 'pip_close', 'buy_tick_qty', 'sell_tick_qty'])
    df1m.set_index('sniffTime', inplace=True)

    # order object to keep track of Order, at this MVP, 1 order per symbol, only 1 time.
    order: OrderDTO = {'buy_price': 0}

    for loopStartTime in range:
        # loop endTime is 1 minute after loopStartTime
        loopEndTime = loopStartTime + pd.Timedelta(minutes=1)

        # get sub DataFrame where sniffTime is between loopStartTime and loopEndTime
        thisMinTickerDf = rawTicketDf.between_time(loopStartTime.time(), loopEndTime.time())

        ohlc = OhlcDTO(None, None, None, None, 0, 0, 0, None, None, None, None, 0, 0)

        # if thisMinTickerDf len >=1 then update ohlc data
        if len(thisMinTickerDf) >= 1:
            ohlc.open = thisMinTickerDf.iloc[0]['price']
            ohlc.high = thisMinTickerDf.iloc[0]['price']
            ohlc.low = thisMinTickerDf.iloc[0]['price']
            ohlc.close = thisMinTickerDf.iloc[0]['price']
            ohlc.pip_open = thisMinTickerDf.iloc[0]['pipNo']
            ohlc.pip_high = thisMinTickerDf.iloc[0]['pipNo']
            ohlc.pip_low = thisMinTickerDf.iloc[0]['pipNo']
            ohlc.pip_close = thisMinTickerDf.iloc[0]['pipNo']
            # check side, then we can know
            if thisMinTickerDf.iloc[0]['side'] == 'B':
                ohlc.buy_volume = thisMinTickerDf.iloc[0]['volume']
                ohlc.buy_tick_qty = thisMinTickerDf.iloc[0]['qty']

            else:
                ohlc.sell_volume = thisMinTickerDf.iloc[0]['volume']
                ohlc.sell_tick_qty = thisMinTickerDf.iloc[0]['qty']
            # volume increase

        if(len(thisMinTickerDf) > 1):
            # loop in  thisMinTickerDf.iloc[1:]
            for index, row in thisMinTickerDf.iloc[1:].iterrows():
                # update value to ohlc
                ohlc.high = max(ohlc.high, row['price'])
                ohlc.low = min(ohlc.low, row['price'])
                ohlc.close = row['price']
                ohlc.pip_high = max(ohlc.pip_high, row['pipNo'])
                ohlc.pip_low = min(ohlc.pip_low, row['pipNo'])
                ohlc.pip_close = row['pipNo']

                # check side, then we can know
                if row['side'] == 'B':
                    ohlc.buy_volume += row['volume']
                    ohlc.buy_tick_qty += row['qty']

                else:
                    ohlc.sell_volume += row['volume']
                    ohlc.sell_tick_qty += row['qty']

        # add ohlc to df1m, use loopStartTime as index snifftime
        loop1mOhlcDf = pd.DataFrame([ohlc.__dict__], index=[loopStartTime])

        # to prevent FutureWarning: The behavior of array concatenation with empty entries is deprecated. In a future version, this will no longer exclude empty items when determining the result dtype. To retain the old behavior, exclude the empty entries before the concat operation.
        #   df1m = pd.concat([df1m, loop1mOhlcDf])

        if len(df1m) == 0:
            df1m = loop1mOhlcDf
        else:
            df1m = pd.concat([df1m, loop1mOhlcDf])

        # when df1m len > 5 add EMA5 of pip_close columns
        df1m['pip_close_EMA5'] = df1m['pip_close'].ewm(span=5, adjust=False).mean()
        df1m['pip_close_EMA10'] = df1m['pip_close'].ewm(span=10, adjust=False).mean()

        # find net_tick_qty of buy_tick_qty - sell_tick_qty, then create EMA5, EMA10 of net_tick_qty
        df1m['net_tick_qty'] = df1m['buy_tick_qty'] - df1m['sell_tick_qty']
        df1m['net_tick_qty_EMA5'] = df1m['net_tick_qty'].ewm(span=5, adjust=False).mean()
        df1m['net_tick_qty_EMA10'] = df1m['net_tick_qty'].ewm(span=10, adjust=False).mean()

        # BUY Signal check
        if (    df1m.iloc[-1]['net_tick_qty_EMA5'] > df1m.iloc[-1]['net_tick_qty_EMA10']
            and df1m.iloc[-2]['net_tick_qty_EMA5'] > df1m.iloc[-2]['net_tick_qty_EMA10']
            and df1m.iloc[-1]['net_tick_qty_EMA5'] > 10
            and order["buy_price"] == 0
        ):
            print(f"BUY: @{loopStartTime} symbol: {symbol} price: {df1m.iloc[-1]['close']}, net_tick_qty_EMA5: {df1m.iloc[-1]['net_tick_qty_EMA5']}, net_tick_qty_EMA10: {df1m.iloc[-1]['net_tick_qty_EMA10']}")
            order = {
                'symbol': symbol,
                'buy_price': df1m.iloc[-1]['high'],
                'max_price': df1m.iloc[-1]['high'],
                'buy_time': loopStartTime,
                'buy_pip_no': df1m.iloc[-1]['pip_close'],
                'stop_loss_pip_no': df1m.iloc[-1]['pip_high'] - 3
            }

    # end of for loopStartTime in range: =============================

    # Clean up unused row, stock market was closed between 12:30 to 14:00, that both sell and buy volume are 0
    df1m = df1m[~((df1m.index >= f'{date_text} 12:30:00') & (df1m.index <= f'{date_text} 14:00:00') & (df1m['buy_volume'] == 0))]

    # add and calculate column that was required by this strategy
    df1m['vol_signal'] = df1m['buyvolume'] > mean_of_prev_buy_volume * 10
    df1m['vol_signal'].rolling(window=2).sum()
    # find first index that vol_signal is True
    # handle IndexError: index 0 is out of bounds for axis 0 with size 0
    if len(df1m[df1m['vol_signal']]) == 0:
        print(f"run_strategy_01 [pid:{os.getpid()}] symbol:{symbol}, df1m.size: {len(df1m)}, mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
        return {'buy_price': 0}

    vol_signal_start_time = df1m[df1m['vol_signal']].index[0]
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
            if (this_index >= vol_signal_start_time
                    and row['close_EMA5'] > row['close_EMA10']
                    and order["buy_price"] == 0
                    and df1m.iloc[index - 1]['close_EMA5'] < df1m.iloc[index - 1]['close_EMA10']):
                # Stop loss calculation, by pip_no, now pip_no is mean of pip_no at that minute
                # stop_loss = row['close'] - 0.0005
                print(f"BUY: @{row.name}: EMA5 crossover EMA10 at {index}")
                # set orderStatus to 1, start from current row to last_index
                df1m.loc[this_index:last_index, 'order_status'] = 1
                # add buy record to orders dataframe
                order = {
                    'symbol': symbol,
                    'buy_price': row.high,
                    'max_price': row.high,
                    'buy_time': row.name,
                    'buy_pip_no': row.pip_no,
                    'stop_loss_pip_no': row.pip_high - 3
                }

        else:  # row['order_status'] == 1
            # increase stop loss
            if row['high'] > order['max_price']:
                # set value of orders last row ['stop_loss_pip_no] to 80% of pip_no
                # and update maxPrice of Order
                order['stop_loss_pip_no'] = row['pip_no'] - 3
                order['max_price'] = row['high']

                # print(f"{row.name}, row.high: {row.high}, orders.iloc[-1]['max_price']: {orders.iloc[-1]['max_price']}, increase stop loss")

            if row['pip_no'] < order['stop_loss_pip_no']:
                # print(f"SELL: @{row.name}: Stop loss at {index}")
                df1m.loc[this_index:last_index, 'order_status'] = 0
                sell_price = row['low']
                buy_price = 0
                # add order's sell data to orders dataframe
                order['sell_price'] = sell_price
                order['sell_time'] = row.name

    # Finally before exit, print out the result
    print (f"run_strategy_01 [pid:{os.getpid()}] symbol:{symbol}, df1m.size: {len(df1m)}, mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
    return order

if __name__ == "__main__":
    print("Start with date_text: {0}".format(date_text))
    start_time = time.perf_counter()

    # find prev_date, don't need each thread to find prev_date again
    prev_date = stockeod.get_prev_date(date_text)
    prev_date_text = prev_date.strftime('%Y-%m-%d')
    prev_date_ticker_table_name = f"ticker_{prev_date.strftime('%Y%m%d')}"

    # get stock list of date_text
    symbols = stockeod.get_stock_list_in_date_text(date_text)

    # get start time, end time of date_text, with symbol in stock_list
    startTime, endTime = ttm.get_start_and_end_time_of_date_and_symbol_in(date_text, symbols)
    print(f"startTime: {startTime}, endTime: {endTime}")

    # get raw ticker for symbol
    # symbols = ['SABUY']

    # creating a pool object, initializing worker function, limit to 5 workers
    pool = multiprocessing.Pool(processes=1)

    result = pool.starmap(run_strategy, [(symbol, date_text, prev_date_text, prev_date_ticker_table_name) for symbol in symbols])






