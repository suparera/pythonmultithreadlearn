# from ticker table create pickle file of PPPM 2024-11-08

import pandas as pd
from utils import ticker_table_manager as ttm

# create pickle file of PPPM 2024-11-08
# ttm.get_ticker_df_from_ticker_table_of_date_text('PPPM', '2024-11-08').to_pickle('pppm-20241108.pkl')

# load df1m.pkl to dataframe
tickers = pd.read_pickle('pppm-20241108.pkl')

# iterate every 5 seconds start from time 09:55:00
# iterate over tickers
for index, row in tickers.iterrows():
    print(row)
    break


print("END")