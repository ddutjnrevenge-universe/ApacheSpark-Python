from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")),\
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("D:/Data_Engineering/Apache_Spark/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache() # create a DataFrame and cache it
schemaPeople.createOrReplaceTempView("people") # create a temporary view named people

teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

for teen in teenagers.collect():
    print(teen)

# use functions instead of SQL queries
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()

