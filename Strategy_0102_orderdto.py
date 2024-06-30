import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
import utils.stockeod_utils as stockeod
import pandas as pd
import dto.OrderDTO as OrderDTO

symbol = 'GFPT'
date_text = '2024-06-13'

prev_date = stockeod.get_prev_date(date_text)
prev_date_text = prev_date.strftime('%Y-%m-%d')

prev_date_ticker_table_name = ttm.get_table_name_from_string(prev_date_text)
mean_of_prev_buy_volume = ttm.get_mean_buy_volume_from_ticker_table_of_date_text(symbol, prev_date_text)

df1m = ttm.get_df1m_from_ticker_table_of_date_text(symbol, date_text)

# add pip_high column, that calculate pip from high column, use pipu.get_buy_pip function
df1m['pip_high'] = df1m['high'].apply(pipu.get_buy_pip, args=('B',))

# add EMA3, EMA10 of 'close' column
df1m['close_EMA5'] = df1m['close'].ewm(span=5, adjust=False).mean()
df1m['close_EMA10'] = df1m['close'].ewm(span=10, adjust=False).mean()

# add and calculate column that was required by this strategy
df1m['vol_signal'] = df1m['buyvolume'] > mean_of_prev_buy_volume * 10
df1m['vol_signal'].rolling(window=2).sum()
# find first index that vol_signal is True
vol_signal_start_time = df1m[df1m['vol_signal']].index[0]

# orderStatus used for keep record of order status, 0: no order, 1: buy order, 2: sell order
df1m['order_status'] = 0

# last_index: (time) used for keep record of last index of df1m
last_index = df1m.index[-1]
buy_price = 0

# orders dataframe: keep history of order to analyze later
## orders = pd.DataFrame(columns=['buy_price', 'buy_time'])
order: OrderDTO = {'buy_price': 0}

# iterate start from row 2 to last row
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
                'buy_price': row.high,
                'max_price': row.high,
                'buy_time': row.name,
                'buy_pip_no': row.pip_no,
                'stop_loss_pip_no': row.pip_high - 3
            }
            print(f"after BUY, order: {order}")

    else:  # row['order_status'] == 1
        # increase stop loss
        if row['high'] > order['max_price']:
            # set value of orders last row ['stop_loss_pip_no] to 80% of pip_no
            # and update maxPrice of Order
            order['stop_loss_pip_no'] = row['pip_no'] - 3
            order['max_price'] = row['high']

            #print(f"{row.name}, row.high: {row.high}, orders.iloc[-1]['max_price']: {orders.iloc[-1]['max_price']}, increase stop loss")

        if row['pip_no'] < order['stop_loss_pip_no']:
            # print(f"SELL: @{row.name}: Stop loss at {index}")
            df1m.loc[this_index:last_index, 'order_status'] = 0
            sell_price = row['low']
            buy_price = 0
            # add order's sell data to orders dataframe
            order['sell_time'] = row.name
            order['sell_price'] = sell_price

# print all
print(f"symbol: {symbol}")
print(f"date_text: {date_text}")
print(f"prev_date: {prev_date}")
print(f"prev_date_ticker_table_name: {prev_date_ticker_table_name}")
print(f"mean_of_prev_buy_volume: {mean_of_prev_buy_volume}")
