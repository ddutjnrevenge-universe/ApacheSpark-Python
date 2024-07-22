from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() 
    
query.awaitTermination()