from datetime import datetime

import utils.db_utils as dbu
import pandas as pd

createSqlPrefix = "CREATE TABLE "
TICKER_TABLE_NAME_PREFIX = "ticker_"
createSqlSuffix = """ ( no bigint NOT NULL AUTO_INCREMENT,
  symbol varchar(30) NOT NULL,
  price decimal(7,2) DEFAULT NULL,
  side char(1) DEFAULT NULL,
  volume bigint DEFAULT NULL,
  pipNo int DEFAULT NULL,
  qty int DEFAULT NULL,
  sniffTime datetime(6) NOT NULL,
  tf01 int DEFAULT NULL,
  PRIMARY KEY (no),
  KEY idx_ticker (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;"""


def create_table_for_day(table_name):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = createSqlPrefix + table_name + createSqlSuffix
        cursor.execute(sql)

def get_table_name_from_date(date):
    return TICKER_TABLE_NAME_PREFIX + date.strftime("%Y%m%d")

def get_table_name_from_string(date_text):
    date = datetime.strptime(date_text, '%Y-%m-%d')
    return TICKER_TABLE_NAME_PREFIX + date.strftime("%Y%m%d")

def get_mean_buy_volume_from_ticker_table_of_date_text(symbol, prev_date_text):
    prev_date_ticker_table_name = get_table_name_from_string(prev_date_text)
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()

        sql = f"""SELECT sniffTime,price,side,qty,
        CASE WHEN side = 'B' THEN volume ELSE 0 END AS buyvolume,
        CASE WHEN side = 'S' THEN volume ELSE 0 END AS sellvolume
        FROM {prev_date_ticker_table_name} WHERE symbol = '{symbol}';"""

        cursor.execute(sql)
        myresult = cursor.fetchall()
        df = pd.DataFrame(myresult, columns=['sniffTime','price','side','qty','buyvolume','sellvolume'])
        df['price'] = df['price'].astype(float)
        df.set_index('sniffTime', inplace=True)

        # resample to 1 minute interval
        df1m = df.resample('1min').agg({'price':'ohlc','buyvolume':'sum', 'sellvolume':'sum', 'qty':'sum'})
        df1m.columns = df1m.columns.droplevel(0)

        # remove df1m where sniffTime between '2024-01-19 12:30:00' and '2024-01-19 14:30:00' and qty = 0
        time1230_text = f'{prev_date_text} 12:30:00'
        time1430_text = f'{prev_date_text} 14:30:00'
        df1m = df1m[~((df1m.index >= time1230_text) & (df1m.index <= time1430_text) & (df1m['qty'] == 0))]

        # return mean of buyvolume/1min
        return df1m['buyvolume'].mean()

def get_df1m_from_ticker_table_of_date_text(symbol, date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        ticker_table_name = get_table_name_from_string(date_text)
        sql = f"""SELECT sniffTime,price,side,qty,
        CASE WHEN side = 'B' THEN volume ELSE 0 END AS buyvolume,
        CASE WHEN side = 'S' THEN volume ELSE 0 END AS sellvolume,
        pipNo as pip_no,
        CASE WHEN side = 'B' THEN qty ELSE 0 END AS buyTickQty,
        CASE WHEN side = 'S' THEN qty ELSE 0 END AS sellTickQty
        FROM {ticker_table_name} WHERE symbol = '{symbol}';"""
        cursor.execute(sql)
        myresult = cursor.fetchall()
        df = pd.DataFrame(myresult, columns=['sniffTime','price','side','qty','buyvolume','sellvolume','pip_no', 'buyTickQty', 'sellTickQty'])
        df['price'] = df['price'].astype(float)
        df.set_index('sniffTime', inplace=True)
        # catch exception TypeError: Only valid with DatetimeIndex, TimedeltaIndex or PeriodIndex, but got an instance of 'Index'
        df.index = pd.to_datetime(df.index)
        # resample to 1 minute interval
        df1m = df.resample('1min').agg({'price':'ohlc','buyvolume':'sum', 'sellvolume':'sum', 'qty':'sum', 'pip_no':'ohlc', 'buyTickQty':'sum', 'sellTickQty':'sum'})
        df1m.columns = df1m.columns.droplevel(0)
        # rename column of df1m
        df1m.columns = ['open', 'high', 'low', 'close', 'buyVolume', 'sellVolume', 'qty', 'pip_open', 'pip_high', 'pip_low', 'pip_close', 'buyTickQty', 'sellTickQty']

        return df1m


# get all ticker data from ticker table of date_text, with specific symbol
def get_ticker_df_from_ticker_table_of_date_text(symbol, date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        ticker_table_name = get_table_name_from_string(date_text)
        sql = f"""select sniffTime, symbol, price, pipNo, side, volume, qty, tf01  
        FROM {ticker_table_name} WHERE symbol = '{symbol}';"""
        cursor.execute(sql)
        myresult = cursor.fetchall()
        df = pd.DataFrame(myresult, columns=['sniffTime','symbol','price','pipNo','side','volume','qty','tf01'])
        df['price'] = df['price'].astype(float)
        df.set_index('sniffTime', inplace=True)
        cursor.close()
        return df

def get_pip_change_count_df(symbol, date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        ticker_table_name = get_table_name_from_string(date_text)
        sql = f"""select sniffTime, (CASE side WHEN 'S' THEN pipNo+1 ELSE pipNo END) as buy_pip_no, volume, price*volume AS value FROM {ticker_table_name} WHERE symbol = '{symbol}';"""
        cursor.execute(sql)
        myresult = cursor.fetchall()
        df = pd.DataFrame(myresult, columns=['sniffTime','buy_pip_no','volume','value'])
        df['value'] = df['value'].astype(float)
        df.set_index('sniffTime', inplace=True)
        return df

def get_start_and_end_time_of_date_and_symbol_in(date_text, symbols):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        ticker_table_name = get_table_name_from_string(date_text)

        # convert symbols[list] to string that inside single quote, comma separated
        symbols_string = "','".join(symbols)

        sql = f"""select min(sniffTime) as start_time, max(sniffTime) as end_time FROM {ticker_table_name} WHERE symbol in ('{symbols_string}');"""
        cursor.execute(sql)
        myresult = cursor.fetchall()
        # round down start_time to minute level
        start_time = myresult[0][0].replace(second=0, microsecond=0)


        end_time = myresult[0][1].replace(second=59, microsecond=999999)
        return start_time, end_time

if __name__ == "__main__":
    # start_time, end_time = get_start_and_end_time_of_date('2024-06-13')
    # print(start_time, end_time)
    df1m = get_df1m_from_ticker_table_of_date_text('SABUY', '2024-06-13')
    print(df1m)
