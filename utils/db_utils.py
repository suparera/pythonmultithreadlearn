import mysql.connector
from mysql.connector import pooling

# Configuration
pool_name = "pool"
config = {
    "host": "localhost",
    "user": "suparera",
    "password": "34erdfcv",
    "database": "settradedb",
    "pool_name": pool_name,
    "pool_size": 5
}

# Create a connection pool

cnxpool = mysql.connector.pooling.MySQLConnectionPool(**config)

def get_pool():
    return cnxpool

# Get connection object from a pool
def get_db_connection():
    return cnxpool.get_connection()

def release_db_connection(con):
    con.close()

