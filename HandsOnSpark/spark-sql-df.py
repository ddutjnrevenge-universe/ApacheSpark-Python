from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true").option("inferSchema","true")\
    .csv("data/fakefriends-header.csv")

print("Inferred schema:", people.printSchema())

print("Name column:", people.select("name").show())

print("People older than 21:", people.filter(people.age <21).show())

print("Group by age:", people.groupBy("age").count().show())

print("Increase age of everyone by 15:", people.select(people.name, people.age + 15).show())

spark.stop()