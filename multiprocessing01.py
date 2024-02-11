# importing the multiprocessing module
import multiprocessing
from time import sleep


def print_cube(num):
	"""
	function to print cube of given num
	"""
	# range start from 1 to num
	for i in range(1, num + 1):
		print("pid: {}, Cube: {}".format(multiprocessing.current_process().pid, i * i * i))

def print_square(num):
	"""
	function to print square of given num
	"""
	# range start from 1 to num
	for i in range(1, num + 1):
		print("pid: {}, Square: {}".format(multiprocessing.current_process().pid, i * i))

if __name__ == "__main__":
	# creating processes
	p1 = multiprocessing.Process(target=print_square, args=(10000, ))
	p2 = multiprocessing.Process(target=print_cube, args=(10000, ))


	# starting process 1
	p1.start()
	# starting process 2
	p2.start()

	# wait until process 1 is finished
	p1.join()
	# wait until process 2 is finished
	p2.join()

	# both processes finished
	print("Done!")
	# check if processes are alive
	print("Process p1 is alive: {}".format(p1.is_alive()))
