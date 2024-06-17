# create 10 mysql db connection pool
import mysql.connector
from mysql.connector import Error
import multiprocessing
import os
import time

def create_pool():
    try:
        pool = mysql.connector.pooling.MySQLConnectionPool(
            pool_name="my_pool",
            pool_size=10,
            pool_reset_session=True,
            host='localhost',
            database='settradedb',
            user='suparera',
            password='34erdfcv',
            connection_timeout=180
        )
        return pool
    except Error as e:
        print("Error: ", e)

def execute_query(query, pool):
    try:
        # Get connection object from a pool
        connection = pool.get_connection()
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor(buffered=True)
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully")
    except Error as e:
        print("Error: ", e)
    finally:
        # close the connection
        connection.close()
        print("Connection closed")

def main():
    # calculate DB pool creation time, show in seconds
    dbpool_start = time.time()
    pool = create_pool()
    dbpool_end = time.time()
    print("DB Pool creation time: {0} seconds".format(dbpool_end - dbpool_start))

    query = "select * from ticker_20240122 where symbol = 'PTTGC';"

    # calculate loop start to finish , show in seconds, with 2 decimal places
    loop_start = time.time()

    processes = multiprocessing.Pool(2)
    for _ in range(10):
        result = processes.apply_async(execute_query, args=(query, pool))
        print (result.get())
    processes.close()
    processes.join()


    loop_end = time.time()
    print("Loop execution time: {0:.2f} seconds".format(loop_end - loop_start))

if __name__ == "__main__":
    main()