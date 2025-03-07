from datetime import datetime, timedelta

import pandas as pd
import utils.order_utils as order_utils
from conds.OpenPipJumpCondition import OpenPipJumpCondition
from conds.PrevDayHighPipSupportCondition import PrevDayHighPipSupportCondition
from conds.SellEMA5CrossEMA10DownCondition import SellEMA5CrossEMA10DownCondition
from conds.TickerCountCondition import TickerCountCondition

PREV_DAY_HIGH_PIP_SUPPORT_CONDITION = 'PrevDayClosePipCondition'
OPEN_PIP_JUMP_CONDITION = 'OpenPipJumpCondition'
TICKER_COUNT_CONDITION = 'TickerCountCondition'
SELL_EMA_CROSS_DOWN_CONDITION = 'SellEMA5CrossEMA10DownCondition'


class StockManager:
    pd.options.mode.copy_on_write = True
    ema5_factor = 2 / (5 + 1)
    ema10_factor = 2 / (10 + 1)

    def __init__(self, date_text, symbol, prev_day_close_pip=None, start_time=None):
        if not symbol:
            raise ValueError("Symbol is required")
        self.date_text = date_text
        self.symbol = symbol
        self.prev_day_close_pip = prev_day_close_pip
        self.order = None

        # Convert start_time to timedelta
        self.start_timedelta = timedelta(hours=start_time.hour, minutes=start_time.minute)
        # create blank dataframes
        self.df_1m = None

        # Condition (both buy and sell) register in open_cond_map
        # add OpenConditionMap
        self.open_cond_map = {
            OPEN_PIP_JUMP_CONDITION: OpenPipJumpCondition(OPEN_PIP_JUMP_CONDITION, prev_day_close_pip)
            # this will be checked 1 min after market open
            , PREV_DAY_HIGH_PIP_SUPPORT_CONDITION: PrevDayHighPipSupportCondition(PREV_DAY_HIGH_PIP_SUPPORT_CONDITION,
                                                                                  prev_day_close_pip, threshold=2,
                                                                                  ema_fast_diff_column='pip_ema5_diff',
                                                                                  ema_slow_diff_column='pip_ema10_diff')
            , TICKER_COUNT_CONDITION: TickerCountCondition(TICKER_COUNT_CONDITION, 30)
            , SELL_EMA_CROSS_DOWN_CONDITION: SellEMA5CrossEMA10DownCondition(SELL_EMA_CROSS_DOWN_CONDITION, 'pip_ema5',
                                                                             'pip_ema10')
        }

        # condition result, has all keys of open_cond_map, with None
        self.cond_results = {key: None for key in self.open_cond_map.keys()}

    def append_df1m_ticker_data(self, end_sniff_time, data):
        global order
        order = None
        try:
            # if data is empty, then return
            if data.empty:
                return
            self.calculate_data_ema_and_update_order_status(data)

            if self.df_1m is None:
                self.df_1m = data
            else:
                self.df_1m = pd.concat([self.df_1m, data], ignore_index=True)

            # check open condition if end_sniff_time plus 2 min is not over self.start_time, then check open condition
            if end_sniff_time - pd.Timedelta(minutes=1) < self.start_timedelta:
                self.cond_results[OPEN_PIP_JUMP_CONDITION] = self.open_cond_map[OPEN_PIP_JUMP_CONDITION].check(
                    data, self.df_1m, self.cond_results)
                # remove this condition from open_cond_map
                self.open_cond_map.pop(OPEN_PIP_JUMP_CONDITION)

            else:
                # check other conditions
                for cond_name, cond in self.open_cond_map.items():
                    self.cond_results[cond_name] = cond.check(data, self.df_1m, self.cond_results)

            # 2 condition were met, then return then print buy signal, check key available first
            if data.iloc[0]['order_status'] == 0:
                if (TICKER_COUNT_CONDITION in self.cond_results
                        and OPEN_PIP_JUMP_CONDITION in self.cond_results
                        and PREV_DAY_HIGH_PIP_SUPPORT_CONDITION in self.cond_results
                        and self.cond_results[OPEN_PIP_JUMP_CONDITION]
                        and self.cond_results[PREV_DAY_HIGH_PIP_SUPPORT_CONDITION]
                        and self.cond_results[TICKER_COUNT_CONDITION]):
                    print(f"StockManager: {self.date_text} {self.symbol} at {end_sniff_time}: Buy signal.")
                    self.order = order_utils.open_order(self.date_text, self.symbol, end_sniff_time, data)

            # Sell signal
            elif data.iloc[0]['order_status'] == 1:
                if (data['pip_close'].iloc[0] < self.order['sl_pip_no']  # Case 1: Stop Loss
                        or (data['pip_close'].iloc[0] > self.order['buy_pip_no'].iloc[0] + 3  # Case 2: Take Profit
                            and SELL_EMA_CROSS_DOWN_CONDITION in self.cond_results
                            and self.cond_results[SELL_EMA_CROSS_DOWN_CONDITION]
                )
                ):
                    print(f"StockManager: {self.date_text} {self.symbol} at {end_sniff_time}: Sell signal.")
                    # set orderStatus to 2, start from current row to last_index
                    self.df_1m.loc[self.df_1m.index[-1], 'order_status'] = 2
                    self.order['sell_price'] = data['close']
                    self.order['sell_time'] = end_sniff_time
                    self.order['sell_pip_no'] = data['pip_close']
                    self.order['sell_condition'] = 'stop_loss' 


        except Exception as e:
            print(
                f"StockManager.append_df1m_ticker_data Error date={self.date_text} symbol={self.symbol} : {e}")
            # print stacktrace
            import traceback
            traceback.print_exc()

    def calculate_data_ema_and_update_order_status(self, data):
        try:
            # if len(data) is 0, then return
            if len(data) == 0:
                return
            if self.df_1m is None or self.df_1m.empty:
                # Initialize EMA columns if df_1m is empty
                data.loc[0, 'pip_ema5'] = data['pip_close'][0]
                data.loc[0, 'pip_ema10'] = data['pip_close'][0]
                data.loc[0, 'totalId_ema5'] = data['totalId'][0]
                data.loc[0, 'totalId_ema10'] = data['totalId'][0]
                data.loc[0, 'order_status'] = 0
            else:
                # Calculate EMA5 and EMA10
                pip_ema5 = data.iloc[0]['pip_close'] * self.ema5_factor + self.df_1m.iloc[-1]['pip_ema5'] * (
                        1 - self.ema5_factor)
                pip_ema10 = data.iloc[0]['pip_close'] * self.ema10_factor + self.df_1m.iloc[-1]['pip_ema10'] * (
                        1 - self.ema10_factor)
                totalId_ema5 = data.iloc[0]['totalId'] * self.ema5_factor + self.df_1m.iloc[-1]['totalId_ema5'] * (
                        1 - self.ema5_factor)
                totalId_ema10 = data.iloc[0]['totalId'] * self.ema10_factor + self.df_1m.iloc[-1]['totalId_ema10'] * (
                        1 - self.ema10_factor)
                pip_ema5_diff = pip_ema5 - self.df_1m.iloc[-1]['pip_ema5']
                pip_ema10_diff = pip_ema10 - self.df_1m.iloc[-1]['pip_ema10']
                totalId_ema5_diff = totalId_ema5 - self.df_1m.iloc[-1]['totalId_ema5']
                totalId_ema10_diff = totalId_ema10 - self.df_1m.iloc[-1]['totalId_ema10']
                data['pip_ema5'] = pip_ema5
                data['pip_ema10'] = pip_ema10
                data['totalId_ema5'] = totalId_ema5
                data['totalId_ema10'] = totalId_ema10
                data['pip_ema5_diff'] = pip_ema5_diff
                data['pip_ema10_diff'] = pip_ema10_diff
                data['totalId_ema5_diff'] = totalId_ema5_diff
                data['totalId_ema10_diff'] = totalId_ema10_diff
                data['order_status'] = 0
                data['order_status'] = self.df_1m.iloc[-1]['order_status']

        except Exception as e:
            print(
                f"StockManager.calculate_data_ema Error date={self.date_text} symbol={self.symbol} in calculate_data_ema: {e}")
            # print stacktrace
            import traceback
            traceback.print_exc()

    def create_data(self, end_sniff_time, ticker_df):
        time = end_sniff_time.time()
        # available fields in ticker_df: sniffTime, symbol, price, pipNo, side, volume, qty, tf01
        open_price = ticker_df.iloc[0]['price']
        open_pip_no = ticker_df.iloc[0]['pipNo']
        high_price = ticker_df['price'].max()
        high_pip_no = ticker_df['pipNo'].max()
        low_price = ticker_df['price'].min()
        low_pip_no = ticker_df['pipNo'].min()
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

    def get_order(self):
        return self.order
