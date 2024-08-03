from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Join and Filter")

# Define the RDDs
RDD_A = sc.parallelize([(1, -1), (2, 20), (3, 3), (4, 0), (5, -12)])
RDD_B = sc.parallelize([(1, 31), (2, 3), (3, 0), (4, -2), (5, 17)])

#  Filter RDD_A
filtered_RDD_A = RDD_A.filter(lambda x: x[1] <= 0)

# Filter RDD_B
filtered_RDD_B = RDD_B.filter(lambda x: x[1] > 5)

# Join the filtered RDDs
result_RDD = filtered_RDD_A.join(filtered_RDD_B)

# Collect and print the result
print(result_RDD.collect())
