import pandas as pd
from conds.OpenPipJumpCondition import OpenPipJumpCondition
from conds.PrevDayHighPipSupportCondition import PrevDayHighPipSupportCondition


class StockManager:
    def __init__(self, symbol, prev_day_close_pip=None, start_time=None):
        if not symbol:
            raise ValueError("Symbol is required")
        self.symbol = symbol
        self.prev_day_close_pip = prev_day_close_pip
        self.start_time = start_time
        # create blank dataframes
        self.df_1m = None

        # condition result
        self.cond_results = {
            'OpenPipJumpCondition': None
        }

        # add OpenConditionMap
        self.open_cond_map = {
            'OpenPipJumpCondition': OpenPipJumpCondition(prev_day_close_pip), # this will be checked 1 min after market open
            'PrevDayClosePipCondition': PrevDayHighPipSupportCondition(prev_day_close_pip, threshold=2)
        }

    def append_df1m_ticker_data(self, end_sniff_time, ticker_df):
        data = self.create_data(end_sniff_time, ticker_df)

        if self.df_1m is None:
            self.df_1m = pd.DataFrame([data])
        else:
            self.df_1m = pd.concat([self.df_1m, pd.DataFrame([data])], ignore_index=True)

        # check open condition if end_sniff_time plus 2 min is not over self.start_time, then check open condition
        if end_sniff_time - pd.Timedelta(minutes=1) <= self.start_time:
            self.cond_results['OpenPipJumpCondition'] = self.open_cond_map['OpenPipJumpCondition'].check(data['open_pip_no'])
            # remove this condition from open_cond_map
            self.open_cond_map.pop('OpenPipJumpCondition')


    def create_data(self, end_sniff_time, ticker_df):
        time = end_sniff_time.time()
        # available fields in ticker_df: sniffTime, symbol, price, pipNo, side, volume, qty, tf01
        open_price = ticker_df.iloc[0]['price']
        open_pip_no = ticker_df.iloc[0]['pipNo']
        # high price, high pip_no
        high_price = ticker_df['price'].max()
        high_pip_no = ticker_df['pipNo'].max()
        # low price, low pip_no
        low_price = ticker_df['price'].min()
        low_pip_no = ticker_df['pipNo'].min()
        # close price, close pip_no
        close_price = ticker_df.iloc[-1]['price']
        close_pip_no = ticker_df.iloc[-1]['pipNo']
        ticker_count = len(ticker_df)
        buy_ticker_count = len(ticker_df[ticker_df['side'] == 'B'])
        sell_ticker_count = len(ticker_df[ticker_df['side'] == 'S'])
        vol = ticker_df['volume'].sum()
        buy_vol = ticker_df[ticker_df['side'] == 'B']['volume'].sum()
        sell_vol = ticker_df[ticker_df['side'] == 'S']['volume'].sum()
        # concat all data to 1 row of df_1m
        data = {
            'time': time, 'open': open_price, 'high': high_price, 'low': low_price, 'close': close_price
            , 'open_pip_no': open_pip_no, 'high_pip_no': high_pip_no, 'low_pip_no': low_pip_no,
            'close_pip_no': close_pip_no
            , 'buy_ticker_count': buy_ticker_count, 'sell_ticker_count': sell_ticker_count, 'ticker_count': ticker_count
            , 'buy_vol': buy_vol, 'sell_vol': sell_vol, 'vol': vol
        }
        return data

