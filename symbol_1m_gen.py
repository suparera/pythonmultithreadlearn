# refactor from 1st draft df1m_data_gen.ipynb
import multiprocessing

import utils.db_utils as dbu
import utils.stockeod_utils as stockeod
import utils.ticker_table_manager as ttm

# from stockeod table, get the stock list in date_text
date_text = '2024-10-16'
symbols = stockeod.get_stock_list_in_date_text(date_text)

dbu.get_pool()

def fix_nan_value(df1m, symbol):
    try:
        df1m['open'] = df1m['open'].replace("", None).ffill()
        df1m['high'] = df1m['high'].mask(df1m['qty'] == 0, df1m['open'])
        df1m['low'] = df1m['low'].mask(df1m['qty'] == 0, df1m['open'])
        df1m['close'] = df1m['close'].mask(df1m['qty'] == 0, df1m['open'])

        df1m['pip_open'] = df1m['pip_open'].replace("", None).bfill()
        df1m['pip_high'] = df1m['pip_high'].mask(df1m['qty'] == 0, df1m['pip_open'])
        df1m['pip_low'] = df1m['pip_low'].mask(df1m['qty'] == 0, df1m['pip_open'])
        df1m['pip_close'] = df1m['pip_close'].mask(df1m['qty'] == 0, df1m['pip_open'])
    except Exception as e:
        print(f"Error: {symbol}: {e}")
        df1m = None


def run_symbol_1m_gen(symbol, date_text):
    df1m = ttm.get_df1m_from_ticker_table_of_date_text(symbol, date_text)
    # fix nan value
    fix_nan_value(df1m, symbol)
    if df1m is None:
        return None

    with dbu.get_pool().get_connection() as con:
        for index, row in df1m.iterrows():
            symbol = symbol
            date = date_text
            time = index.time()
            # open, in case of NaN convert to null
            open = row['open']
            # insert_sql = f"""insert into symbol_1m (symbol, date, time, open) values ('{symbol}', '{date}', '{time}', {open});"""
            insert_sql = f""" INSERT INTO symbol_1m (symbol, date, time, open, high, low, close, buyVolume, sellVolume, qty, pip_open, pip_high, pip_low, pip_close, buyTickQty, sellTickQty, buyId, sellId, totalId) values ('{symbol}', '{date}', '{time}', {open}, {row['high']}, {row['low']}, {row['close']}, {row['buyVolume']}, {row['sellVolume']}, {row['qty']}, {row['pip_open']}, {row['pip_high']}, {row['pip_low']}, {row['pip_close']}, {row['buyTickQty']}, {row['sellTickQty']}, {row['buyId']}, {row['sellId']}, {row['totalId']});"""

            cursor = con.cursor()
            cursor.execute(insert_sql)
        con.commit()


if __name__ == "__main__":
    pool = multiprocessing.Pool(processes=15)

    pool.starmap(run_symbol_1m_gen,
                          [(symbol, date_text) for symbol in symbols])
