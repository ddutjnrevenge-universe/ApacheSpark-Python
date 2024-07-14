from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql import Row

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

friends = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("data/fakefriends-header.csv")


# select("col1","col2") statement to get the columns we want
friendsByAge = friends.select("age","friends")
# handy DF functions: avg groupby show
print("Average friends by Age: ", friendsByAge.groupBy("age").avg("friends").sort("age").show())
#format more readable
print("Average friends by Age (nicer ver.): ", friendsByAge.groupBy("age").agg(func.round(func.avg("friends"),2)).alias("friends_avg_age").sort("age").show())

spark.stop()