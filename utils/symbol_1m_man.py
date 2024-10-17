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
