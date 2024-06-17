import pandas as pd

def get_mean_buy_volume(symbol, date, dbcursor):
    ticker_table = get_ticker_table_name(date)
    sql = f"""SELECT sniffTime,price,side,qty,
    CASE WHEN side = 'B' THEN volume ELSE 0 END AS buyvolume,
    CASE WHEN side = 'S' THEN volume ELSE 0 END AS sellvolume
    FROM {ticker_table} WHERE symbol = '{symbol}';"""
    dbcursor.execute(sql)
    myresult = dbcursor.fetchall()
    df = pd.DataFrame(myresult, columns=['sniffTime','price','side','qty','buyvolume','sellvolume'])
    # fix above line, change price datatype to float
    df['price'] = df['price'].astype(float)
    df.set_index('sniffTime', inplace=True)
    df1m = df.resample('1min').agg({'price':'ohlc','buyvolume':'sum', 'sellvolume':'sum', 'qty':'sum'})
    df1m.columns = df1m.columns.droplevel(0)

    # remove df1m where sniffTime between '2024-01-19 12:30:00' and '2024-01-19 14:30:00' and qty = 0
    date_text = date.strftime('%Y-%m-%d')
    time1230_text = f'{date_text} 12:30:00'
    time1430_text = f'{date_text} 14:30:00'
    # remove df1m where sniffTime between time1230_text and time1430_text and qty = 0
    df1m = df1m[~((df1m.index >= time1230_text) & (df1m.index <= time1430_text) & (df1m['qty'] == 0))]
    return df1m['buyvolume'].mean()

def get_ticker_table_name(test_date):
    return f"ticker_{test_date.strftime('%Y%m%d')}"

# create main to test
if __name__ == "__main__":
    import mysql.connector
    from db_util import get_db_connection
    maindb = get_db_connection()
    date_text = '2023-10-05'
    test_date = pd.to_datetime(date_text)
    print(get_mean_buy_volume('A', test_date, maindb.cursor()))
    maindb.close()