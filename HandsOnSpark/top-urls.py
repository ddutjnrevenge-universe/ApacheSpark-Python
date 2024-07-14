from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as func

from pyspark.sql.functions import regexp_extract

spark = SparkSession.builder.appName("StructuredStreaming").getOrCreate()

accessLines = spark.readStream.text("logs")

# Regular expression for parsing the Apache Access Log
contentSizeExp = r'\s(\d+)$'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
timeExp = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})]'
hostExp = r'(^\S+\.[\S+\.]+\S+)\s'\

logsDF =accessLines.select(regexp_extract('value', hostExp, 1).alias('host'),
                           regexp_extract('value', timeExp, 1).alias('timestamp'),
                           regexp_extract('value', generalExp, 1).alias('method'),
                           regexp_extract('value', generalExp, 2).alias('endpoint'),
                           regexp_extract('value', generalExp, 3).alias('protocol'),
                           regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                           regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))

# Keep a running count for every access by status code
# statusCountsDF = logsDF.groupBy(logsDF.status).count()
logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())
endpointCounts = logsDF2.groupBy(func.window(func.col("eventTime"),\
                        "30 seconds", "10 seconds"), func.col("endpoint")).count()

# Kick off streaming query, dumping results to the console
# query = ( statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start() )
sortedEndpointCounts = endpointCounts.orderBy(func.col("count").desc())

query = sortedEndpointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start()
# Run until terminated
query.awaitTermination()

# Cleanly shut down the session
spark.stop()