from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType

def calcCosineSimilarity(spark, data):
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg( \
            func.sum(func.col("xy")).alias("numerator"), \
            (func.sqrt(func.sum(func.col("xx")))*\
             func.sqrt(func.sum(func.col("yy"))))\
                .alias("denominator"), \
            func.count(func.col("xy")).alias("numPairs")
    )

    result = calculateSimilarity \
        .withColumn("score",\
                    func.when(func.col("denominator") != 0, \
                              func.col("numerator")/func.col("denominator"))\
                                .otherwise(0)\
                ).select("movie1", "movie2", "score", "numPairs")
    
    return result

def getMovieNames(movieNames, movieID):
    result = movieNames.filter(func.col("movieID") == movieID)\
        .select("movieTitle").collect()[0]
    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

movieNamesSchema = StructType([\
    StructField("movieID", IntegerType(), True),\
    StructField("movieTitle", StringType(), True)\
    ])

moviesSchema = StructType([\
                StructField("userID", IntegerType(), True),\
                StructField("movieID", IntegerType(), True),\
                StructField("rating", IntegerType(), True),\
                StructField("timestamp", LongType(), True)\
                ])

movieNames = spark.read\
    .option("sep", "|")\
    .option("charset", "UTF-8")\
    .schema(movieNamesSchema)\
    .csv("data/ml-100k/u.item")

movies = spark.read\
    .option("sep", "\t")\
    .schema(moviesSchema)\
    .csv("data/ml-100k/u.data")

ratings = movies.select("userID", "movieID", "rating")

moviePairs = ratings.alias("ratings1")\
            .join(ratings.alias("ratings2"), (func.col("ratings1.userID")==func.col("ratings2.userID"))\
                  & (func.col("ratings1.movieID") < func.col("ratings2.movieID")))\
            .select(func.col("ratings1.movieID").alias("movie1"),\
                    func.col("ratings2.movieID").alias("movie2"),\
                    func.col("ratings1.rating").alias("rating1"),\
                    func.col("ratings2.rating").alias("rating2"))

moviePairSimilarities = calcCosineSimilarity(spark, moviePairs).cache()

scoreThreshold = 0.97
coOccurenceThreshold = 50.0

# Replace `movieID` with a specific movie ID to test
movieID = 1

filteredResults = moviePairSimilarities.filter(\
    ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &\
    (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurenceThreshold))

results = filteredResults.sort(func.col("score").desc()).take(10)

print("Top 10 similar movies for " + getMovieNames(movieNames, movieID))

for result in results:
    similarMovieID = result.movie1
    if (similarMovieID == movieID):
        similarMovieID = result.movie2

    print(getMovieNames(movieNames, similarMovieID) \
          + "\tscore: " + str(result.score) +\
             "\tstrength: " + str(result.numPairs)
        )
