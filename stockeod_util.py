
def get_prev_date(date_text, mycursor):
    sql = "SELECT date FROM stockeod WHERE Date < '" + date_text + "' ORDER BY Date DESC LIMIT 1"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    if len(myresult) > 0:
        return myresult[0][0]
    else:
        return None

def get_stock_list(date_text, mycursor):
    '''Get stock list from stockeod of previous day
    :param date_text: date in format 'YYYY-MM-DD'
    :param mycursor: cursor object
    :return: list of stock symbols
    '''
    sql = "SELECT symbol FROM stockeod WHERE Date = '" + date_text + "'"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    stock_list = []
    for x in myresult:
        stock_list.append(x[0])
    return stock_list