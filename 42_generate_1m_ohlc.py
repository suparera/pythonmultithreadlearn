import os
import multiprocessing
from datetime import datetime

import mysql.connector
from db_util import get_db_connection
import stockeod_util as stockeod

date_text = '2024-06-12'
test_date = datetime.strptime(date_text, '%Y-%m-%d')
ticker_table = f"ticker_{test_date.strftime('%Y%m%d')}"
def run_backtest(symbol, ticker_table, prev_date):
    ticker_table = f"ticker_{test_date.strftime('%Y%m%d')}"

    query = f"select symbol, count(1) as qty from {ticker_table} where symbol = '{symbol}' group by symbol;"
    cursor = db.cursor(buffered=True)
    cursor.execute(query)
    db.commit()

    # return qty field result from query
    resultset = cursor.fetchall()

    if len(resultset) == 0:
        print(f"{symbol},0,{os.getpid()}")
        return 0

    qty = resultset[0][1]
    print (f"{symbol}, ticker_qty: {qty}, pid: {os.getpid()}")
    return qty

# This function is called when each worker process is initialized
# will create each database connection for each worker process
def worker_init_fn():
    global db
    db = mysql.connector.connect(host="localhost", user="suparera", password='34erdfcv', database="settradedb")
    # print process id, db connection object
    print("DB Connection: {0}, pid:{1}".format(db, os.getpid()))

if __name__ == "__main__":
    print("Start with test_date: {0}, ticker_table: {1}".format(test_date, ticker_table))
    # input stock from stockeod of previous day
    maindb = get_db_connection()

    prev_date = stockeod.get_prev_date(date_text, maindb.cursor())

    # get stock list from stockeod of previous day, send
    stock_list = stockeod.get_stock_list(prev_date.strftime('%Y-%m-%d'), maindb.cursor())

    # for EASILY TESTING, stock_list is limited to 5
    # stock_list = stock_list[:2]
    print("Stock list: {0}".format(stock_list))

    # creating a pool object, initializing worker function, limit to 5 workers
    pool = multiprocessing.Pool(initializer=worker_init_fn, initargs=(), processes=5)

    # result = pool.map(run_backtest, stock_list)
    result = pool.starmap(run_backtest, [(symbol, ticker_table, prev_date) for symbol in stock_list])
    print(f"FINISH, stock_qty result: {result}")
