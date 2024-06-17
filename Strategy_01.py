import utils.ticker_table_manager as ttm
import utils.stockeod_utils as stockeod
import pandas as pd

symbol = 'PK'
date_text = '2024-06-14'

prev_date = stockeod.get_prev_date(date_text)
prev_date_text = prev_date.strftime('%Y-%m-%d')

prev_date_ticker_table_name = ttm.get_table_name_from_string(prev_date_text)
mean_of_prev_buy_volume = ttm.get_mean_buy_volume_from_ticker_table_of_date_text(symbol, prev_date_text)

df1m = ttm.get_df1m_from_ticker_table_of_date_text(symbol, date_text)

# add and calculate column that was required by this strategy
df1m['vol_signal'] = df1m['buyvolume'] > mean_of_prev_buy_volume*10
df1m['vol_signal'].rolling(window=2).sum()

# orderStatus used for keep record of order status, 0: no order, 1: buy order, 2: sell order
df1m['orderStatus'] = 0

# last_index: (time) used for keep record of last index of df1m
last_index = df1m.index[-1]
global buy_price
buy_price = 0


# orders dataframe: keep history of order to analyze later
orders = pd.DataFrame(columns=['buy_price', 'buy_time'])




# print all
print(f"symbol: {symbol}")
print(f"date_text: {date_text}")
print(f"prev_date: {prev_date}")
print(f"prev_date_ticker_table_name: {prev_date_ticker_table_name}")
print(f"mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")