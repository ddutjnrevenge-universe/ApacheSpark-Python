from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("D:/Data_Engineering/Apache_Spark/book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
wordsWithoutEmptyString = words.filter(words.word != "")
#Normalize everything to lowercase
lowerWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))
# Count up occurences of each word
wordCounts = lowerWords.groupBy("word").count()
#sort by wordCounts descendingly
wordCountsSorted = wordCounts.sort("count")
#show the results
wordCountsSorted.show(wordCountsSorted.count())


