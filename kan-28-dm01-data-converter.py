# convert from ticker_20240906 ticker data to OHLCV data 1 minute interval

import utils.db_utils as dbu
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm
import utils.pip_utils as pipu
from dto.OrderDTO import OrderDTO

# from stockeod table, get the stock list in date_text
date_text = '2024-09-06'
symbols = stockeod.get_stock_list_in_date_text(date_text)

print(f"symbols len {len(symbols)}")

# dbu.get_pool()
# with dbu.get_pool().get_connection() as con:
#     cursor = con.cursor()
#     for symbol in symbols:
symbol = "EA"
df1m = ttm.get_df1m_from_ticker_table_of_date_text(symbol, date_text)
# read field in df1m, genertate create table statement to store df1m
print('finish')

