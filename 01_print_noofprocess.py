# print number of processes and core
# importing the multiprocessing module
import multiprocessing
import os

def worker1():
    # printing process id
    print("ID of process running worker1: {}".format(os.getpid()))

def worker2():
    # printing process id
    print("ID of process running worker2: {}".format(os.getpid()))

if __name__ == "__main__":
    # printing main program process id
    print("ID of main process: {}".format(os.getpid()))

    # creating processes
    p1 = multiprocessing.Process(target=worker1)
    p2 = multiprocessing.Process(target=worker2)

    # starting processes
    p1.start()
    p2.start()

    # process IDs
    print("ID of process p1: {}".format(p1.pid))
    print("ID of process p2: {}".format(p2.pid))

    # wait until processes are finished
    p1.join()
    p2.join()

    # both processes finished
    print("Both processes finished execution!")

    # check if processes are alive
    print("Process p1 is alive: {}".format(p1.is_alive()))
    print("Process p2 is alive: {}".format(p2.is_alive()))

    # print number of CPU cores
    print("Number of CPU cores: {}".format(multiprocessing.cpu_count()))
    # print number of processes
    print("Number of processes: {}".format(multiprocessing.active_children()))