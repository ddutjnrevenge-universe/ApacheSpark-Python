from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
spark = SparkSession.builder.appName("AmountByCustomer").getOrCreate()

schema = StructType([StructField("customerID", IntegerType(), True),\
                     StructField("itemID", IntegerType(), True),\
                        StructField("amount", FloatType(), True)])

df = spark.read.schema(schema).csv("data/customer-orders.csv")
df.printSchema()

# Group by customerID and sum up the amount round to 2 decimal places
totalByCustomer = df.groupBy("customerID").agg(func.round(func.sum("amount"),2).alias("totalAmount"))

# Sort by total spent
totalByCustomerSorted = totalByCustomer.sort("totalAmount")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()