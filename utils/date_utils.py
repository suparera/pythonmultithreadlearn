import db_utils as dbu


def get_prev_date_from_stockeod(date_text):
    with dbu.get_pool().get_connection() as con:
        cursor = con.cursor()
        sql = "SELECT date FROM stockeod WHERE Date < '" + date_text + "' ORDER BY Date DESC LIMIT 1"
        cursor.execute(sql)
        result = cursor.fetchall()
        if len(result) > 0:
            return result[0][0]
        else:
            return None