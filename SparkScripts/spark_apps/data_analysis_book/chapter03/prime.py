from pyspark import SparkContext

sc = SparkContext(appName="Prime Numbers")

from math import sqrt,ceil
def is_prime(n):
    if n <= 1:
        return False
    for i in range(2,ceil(sqrt(n))+1):
        if n%i == 0:
            return False
    return True
import time

start_time = time.time()

numbers_file = "/opt/spark/data/numbers.txt"


prime_numbers_rdd_output_path = "/opt/spark/data/results/chapter03/prime.txt"
numbers_rdd = sc.textFile(numbers_file)
numbers_rdd = numbers_rdd.map(lambda x: int(x))
prime_numbers_rdd = numbers_rdd.filter(is_prime)
prime_numbers_rdd = prime_numbers_rdd.repartition(36)
print(prime_numbers_rdd.take(10))

end_time = time.time()

duration = end_time - start_time
print(f"Execution time: {duration} seconds")

sc.stop()