from pyspark.sql import SparkSession

logFile = r"D:\FPT\asm\040724\README.md"
spark = SparkSession.builder.appName("DemoApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numTables = logData.filter(logData.value.contains('table')).count()
numSpaces = logData.filter(logData.value.contains(' ')).count()

print("Lines with table: %i, lines with spaces: %i" % (numTables, numSpaces))

spark.stop()