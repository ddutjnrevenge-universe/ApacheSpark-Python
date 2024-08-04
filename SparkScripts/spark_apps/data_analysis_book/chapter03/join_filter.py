from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "Join and Filter")

# Define the RDDs
RDD_A = sc.parallelize([(1, -1), (2, 20), (3, 3), (4, 0), (5, -12)])
RDD_B = sc.parallelize([(1, 31), (2, 3), (3, 0), (4, -2), (5, 17)])

# Convert RDDs to key-value pairs
RDD_A = RDD_A.map(lambda x: (x[0], x[1]))
RDD_B = RDD_B.map(lambda x: (x[0], x[1]))

# Join the RDDs
joined_RDD = RDD_A.join(RDD_B)

# Filter the joined RDD
result_RDD = joined_RDD.filter(lambda x: x[1][0] <= 0 and x[1][1] > 5)

# Collect and print the result
print(result_RDD.collect())
