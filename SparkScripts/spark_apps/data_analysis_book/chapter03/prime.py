from pyspark import SparkContext
from math import sqrt, ceil
import time

# Initialize SparkContext
sc = SparkContext(appName="Prime Numbers")

# Function to check if a number is prime
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2, ceil(sqrt(n)) + 1):
        if n % i == 0:
            return False
    return True

# Measure the start time
start_time = time.time()

# Input and output paths
numbers_file = "/opt/spark/data/numbers.txt"
prime_numbers_rdd_output_path = "/opt/spark/data/results/chapter03/prime.txt"

# Read the numbers from the file
numbers_rdd = sc.textFile(numbers_file)
numbers_rdd = numbers_rdd.map(lambda x: int(x))

# Filter prime numbers
prime_numbers_rdd = numbers_rdd.filter(is_prime)

# Repartition the RDD for parallel processing
prime_numbers_rdd = prime_numbers_rdd.repartition(36)

# Print the first 10 prime numbers
print(prime_numbers_rdd.take(10))

# Measure the end time
end_time = time.time()

# Calculate and print the execution duration
duration = end_time - start_time
print(f"Execution time: {duration} seconds")

# Check the number of workers
sc_java = sc._jsc.sc()
n_workers = len([executor.host() for executor in sc_java.statusTracker().getExecutorInfos()]) - 1

# Print the number of workers
print(f"Number of workers: {n_workers}")

# Get memory status of executors
executor_memory_status = sc_java.getExecutorMemoryStatus().keys()
executor_count = len([executor.host() for executor in sc_java.statusTracker().getExecutorInfos()]) - 1

# Print memory status and executor count
print(f"Executor memory status: {executor_memory_status}")
print(f"Executor count (excluding driver): {executor_count}")

# Stop SparkContext
sc.stop()
