from pyspark import SparkContext,SparkConf
import time
# Initialize SparkContext
# sc = SparkContext(appName="Join and Filter Compare")
conf = SparkConf().setAppName("Join and Filter Compare").set("spark.driver.host","10.88.51.141").set("spark.driver.pỏt","50353")
sc = SparkContext("local", "Join and Filter Local")

# Define the RDDs
RDD_A = sc.parallelize([(1, -1), (2, 20), (3, 3), (4, 0), (5, -12)])
RDD_B = sc.parallelize([(1, 31), (2, 3), (3, 0), (4, -2), (5, 17)])

# Convert RDDs to key-value pairs
RDD_A = RDD_A.map(lambda x: (x[0], x[1]))
RDD_B = RDD_B.map(lambda x: (x[0], x[1]))

# Approach 1: Join then Filter
start_time = time.time()

# Join the RDDs
joined_RDD = RDD_A.join(RDD_B)

# Filter the joined RDD
result_RDD_1 = joined_RDD.filter(lambda x: x[1][0] <= 0 and x[1][1] > 5)

# Collect and print the result
result_1 = result_RDD_1.collect()
end_time = time.time()
approach_1_time = end_time - start_time
print("Approach 1 result:", result_1)
print("Approach 1 time:", approach_1_time)

# Approach 2: Filter then Join
start_time = time.time()

# Filter RDD_A
filtered_RDD_A = RDD_A.filter(lambda x: x[1] <= 0)

# Filter RDD_B
filtered_RDD_B = RDD_B.filter(lambda x: x[1] > 5)

# Join the filtered RDDs
result_RDD_2 = filtered_RDD_A.join(filtered_RDD_B)

# Collect and print the result
result_2 = result_RDD_2.collect()
end_time = time.time()
approach_2_time = end_time - start_time
print("Approach 2 result:", result_2)
print("Approach 2 time:", approach_2_time)

# Print comparison
print("\nPerformance Comparison:")
print(f"Approach 1 time: {approach_1_time:.4f} seconds")
print(f"Approach 2 time: {approach_2_time:.4f} seconds")
if approach_1_time < approach_2_time:
    print("Approach 1 is faster.")
else:
    print("Approach 2 is faster.")
