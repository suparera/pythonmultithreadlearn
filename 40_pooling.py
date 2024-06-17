import os
import multiprocessing
import mysql.connector
from mysql.connector import Error

def square(n):
    # print process id and number
    for _ in range(1000000):
        pass
    print("SQUARE {0}: {1}".format(n, os.getpid()))
    # query data from DB
    query = "select * from ticker_20240122 where symbol = 'PTTGC';"
    cursor = db.cursor(buffered=True)
    cursor.execute(query)
    db.commit()
    print("Query executed successfully")
    return n * n

def worker_init_fn():
    # sleep for 1 sec
    print("Worker initializing pid: {0}".format(os.getpid()))
    global db
    db = mysql.connector.connect(host="localhost", user="suparera", password='34erdfcv', database="settradedb")

if __name__ == "__main__":
    # input list
    my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28,
               29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54,
               55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80,
               81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100]

    # creating a pool object, initializing worker function, limit to 5 workers
    pool = multiprocessing.Pool(initializer=worker_init_fn, initargs=(), processes=3)

    result = pool.map(square, my_list)

    print(result)
