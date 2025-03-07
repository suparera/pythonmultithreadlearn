import math
import utils.pip_utils as pipu


def open_order(date_text, symbol, end_sniff_time, df1m, current_row):

    # current_row: is current time row, we can get from ohlc_df at index = end_sniff_time

    current_row['order_status'] = 1
    buy_price = float(current_row['close'])
    # sl_price = 2% down from buy_price
    no_of_sl_pip_change = math.ceil(
        0.025 / ((buy_price - pipu.get_price_by_pip(current_row['pip_close'] - 1)) / buy_price))
    sl_pipe_no = current_row['pip_close'] - no_of_sl_pip_change
    sl_price = pipu.get_price_by_pip(sl_pipe_no)
    order_temp = {
        'symbol': symbol,
        'buy_price': buy_price,
        'buy_time': end_sniff_time,
        'buy_pip_no': current_row['pip_close'],
        'sl_pip_no': sl_pipe_no,
        'sl_price': sl_price,
        'date': date_text
    }
    return order_temp