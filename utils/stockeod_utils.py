from datetime import datetime

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

if __name__ == "__main__":
    current_date = '2024-06-13'
    prev_date = get_prev_date(current_date)
    print(f"prev_date: {prev_date}")