# Database utility

# get DB connection
def get_db_connection():
    import mysql.connector
    db = mysql.connector.connect(host="localhost", user="suparera", password='34erdfcv', database="settradedb")
    return db