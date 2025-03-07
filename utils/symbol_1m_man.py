from datetime import datetime

import utils.db_utils as dbu
import pandas as pd


def get_by_date_symbol(date_text, symbol):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = f"""select * from symbol_1m where date = '{date_text}' and symbol='{symbol}';"""
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result, columns=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'buyVolume', 'sellVolume', 'qty', 'pip_open', 'pip_high', 'pip_low', 'pip_close', 'buyTickQty', 'sellTickQty', 'buyId', 'sellId', 'totalId'])
        return df


def find_opportunities_risks(date_text, symbol, buy_time, buy_pip_no):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = f"""WITH PipCloseStats AS (
    SELECT
        MAX(pip_close) AS maxPipClose,
        MIN(pip_close) AS minPipClose
    FROM symbol_1m
    WHERE date = '{date_text}'
      AND symbol = '{symbol}'
      AND time > '{buy_time}'
)
SELECT
    MIN(time) AS minTimeForMaxPipClose,
    (SELECT maxPipClose FROM PipCloseStats) AS maxPipClose,
    (SELECT MIN(time) FROM symbol_1m WHERE date = '{date_text}' AND symbol = '{symbol}' AND time > '{buy_time}' AND pip_close = (SELECT minPipClose FROM PipCloseStats)) AS minTimeForMinPipClose,
    (SELECT minPipClose FROM PipCloseStats) AS minPipClose
FROM symbol_1m
WHERE date = '{date_text}'
  AND symbol = '{symbol}'
  AND time > '{buy_time}'
  AND pip_close = (SELECT maxPipClose FROM PipCloseStats);"""
        # print("------------")
        # print(sql)
        # print("------------")
        cursor.execute(sql)
        result = cursor.fetchall()
        min_time_for_max_pip_close = result[0][0]
        max_pip_close = result[0][1]
        min_time_for_min_pip_close = result[0][2]
        min_pip_close = result[0][3]

        # return as dict
        return {
            'min_time_for_max_pip_close': min_time_for_max_pip_close,
            'max_pip_close': max_pip_close,
            'min_time_for_min_pip_close': min_time_for_min_pip_close,
            'min_pip_close': min_pip_close
        }

if __name__ == "__main__":
    find_opportunities_risks('2024-10-17', 'MCOT', '09:57', 429)
