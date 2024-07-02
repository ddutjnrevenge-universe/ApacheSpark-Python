#  List the names of all superheroes with only ONE connections
#  Compute the actual smallest number of connections in the dataset instead of assuming it is 1
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ObsecureHeroes").getOrCreate()

schema = StructType([\
        StructField("id", IntegerType(), True),\
        StructField("name", StringType(), True),\
])

names = spark.read.schema(schema).option("sep", " ").csv("Marvel-Names.txt")

lines = spark.read.text("Marvel-Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value"), " " )[0])\
    .withColumn("connections", func.size(func.split(func.col("value")," "))-1)\
    .groupBy("id").agg(func.sum("connections").alias("connections"))

minConnectionCount = connections.agg(func.min("connections")).first()[0]

minConnections = connections.filter(func.col("connections") == minConnectionCount)

minConnectionWithNames = minConnections.join(names, "id")

print("These characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionWithNames.select("name").show()
