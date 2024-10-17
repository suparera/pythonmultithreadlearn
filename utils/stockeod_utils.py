from datetime import datetime
from utils.pip_utils import get_buy_pip

import utils.db_utils as dbu
import pandas as pd

def get_prev_date(current_date):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = "SELECT date FROM stockeod WHERE Date < '" + current_date + "' ORDER BY Date DESC LIMIT 1"
        cursor.execute(sql)
        result = cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return None


def get_stock_list_in_date_text(date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = "SELECT symbol FROM stockeod WHERE Date = '" + date_text + "'"
        cursor.execute(sql)
        result = cursor.fetchall()
        stock_list = []
        for x in result:
            stock_list.append(x[0])
        return stock_list

if __name__ == "__main__":
    current_date = '2024-06-13'
    prev_date = get_prev_date(current_date)
    print(f"prev_date: {prev_date}")


def get_prev_date_close_pip(symbol, prev_date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = "SELECT closep FROM stockeod WHERE Date = '" + prev_date_text + "' and symbol = '" + symbol + "'"
        cursor.execute(sql)
        result = cursor.fetchall()

        if len(result) > 0:
            return get_buy_pip(result[0][0], 'B')
        else:
            return None