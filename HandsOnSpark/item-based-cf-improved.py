from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
import sys

def calcCosineSimilarity(data):
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

def calcPearsonSimilarity(data):
    pairScores = data \
        .withColumn("xx", func.col("rating1") * func.col("rating1")) \
        .withColumn("yy", func.col("rating2") * func.col("rating2")) \
        .withColumn("xy", func.col("rating1") * func.col("rating2")) \
        .withColumn("x", func.col("rating1")) \
        .withColumn("y", func.col("rating2"))
    
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg( \
            func.sum(func.col("xy")).alias("numerator"), \
            func.sum(func.col("x")).alias("sum_x"), \
            func.sum(func.col("y")).alias("sum_y"), \
            func.sum(func.col("xx")).alias("sum_xx"), \
            func.sum(func.col("yy")).alias("sum_yy"), \
            func.count(func.col("xy")).alias("numPairs")
    )

    result = calculateSimilarity \
        .withColumn("score",\
                    (func.col("numerator") - (func.col("sum_x") * func.col("sum_y") / func.col("numPairs"))) / \
                    (func.sqrt(func.col("sum_xx") - (func.col("sum_x") * func.col("sum_x") / func.col("numPairs"))) * \
                     func.sqrt(func.col("sum_yy") - (func.col("sum_y") * func.col("sum_y") / func.col("numPairs"))))\
                ).select("movie1", "movie2", "score", "numPairs")
    
    return result

def calcJaccardSimilarity(data):
    pairScores = data \
        .withColumn("intersection", func.col("rating1") * func.col("rating2")) \
        .withColumn("union", func.col("rating1") + func.col("rating2") - func.col("rating1") * func.col("rating2"))
    
    calculateSimilarity = pairScores \
        .groupBy("movie1", "movie2") \
        .agg( \
            func.sum(func.col("intersection")).alias("numerator"), \
            func.sum(func.col("union")).alias("denominator"), \
            func.count(func.col("intersection")).alias("numPairs")
    )

    result = calculateSimilarity \
        .withColumn("score",\
                    func.when(func.col("denominator") != 0, \
                              func.col("numerator")/func.col("denominator"))\
                                .otherwise(0)\
                ).select("movie1", "movie2", "score", "numPairs")
    
    return result

def calcCustomSimilarity(data):
    cosineScores = calcCosineSimilarity(data)
    adjustedScores = cosineScores \
        .withColumn("customScore", func.col("score") * (func.col("numPairs") / 10)) # Example adjustment
    
    return adjustedScores.select("movie1", "movie2", "customScore", "numPairs")

def getMovieNames(movieNames, movieID):
    result = movieNames.filter(func.col("movieID") == movieID)\
        .select("movieTitle").collect()[0]
    return result[0]

spark = SparkSession.builder.appName("MovieSimilarities").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

movieNamesSchema = StructType([\
    StructField("movieID", IntegerType(), True),\
    StructField("movieTitle", StringType(), True),\
    StructField("genres", StringType(), True) # Added genres
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

ratings = movies.select("userID", "movieID", "rating").filter("rating >= 4") # Filtering bad ratings

moviePairs = ratings.alias("ratings1")\
            .join(ratings.alias("ratings2"), (func.col("ratings1.userID")==func.col("ratings2.userID"))\
                  & (func.col("ratings1.movieID") < func.col("ratings2.movieID")))\
            .select(func.col("ratings1.movieID").alias("movie1"),\
                    func.col("ratings2.movieID").alias("movie2"),\
                    func.col("ratings1.rating").alias("rating1"),\
                    func.col("ratings2.rating").alias("rating2"))

# Choose similarity metric
# moviePairSimilarities = calcCustomSimilarity(moviePairs).cache()
# pearson
# moviePairSimilarities = calcPearsonSimilarity(moviePairs).cache()
# cosine
moviePairSimilarities = calcCosineSimilarity(moviePairs).cache()

# # Boost scores for movies in the same genre
# moviePairSimilarities = moviePairSimilarities.alias("similarities")\
#     .join(movieNames.alias("names1"), func.col("similarities.movie1") == func.col("names1.movieID"))\
#     .join(movieNames.alias("names2"), func.col("similarities.movie2") == func.col("names2.movieID"))\
#     .withColumn("boostedScore", func.col("score") * \
#                 func.when(func.col("names1.genres") == func.col("names2.genres"), 1.1).otherwise(1.0))\
#     .select("movie1", "movie2", "boostedScore", "numPairs")

# moviePairSimilarities = moviePairSimilarities.alias("similarities")\
#     .join(movieNames.alias("names1"), func.col("similarities.movie1") == func.col("names1.movieID"))\
#     .join(movieNames.alias("names2"), func.col("similarities.movie2") == func.col("names2.movieID"))\
#     .withColumn("boostedScore", func.col("customScore") * \
#                 func.when(func.col("names1.genres") == func.col("names2.genres"), 1.1).otherwise(1.0))\
#     .select("movie1", "movie2", "boostedScore", "numPairs")

scoreThreshold = 0.85
coOccurenceThreshold = 50.0

# Replace `movieID` with a specific movie ID to test
movieID = 1

filteredResults = moviePairSimilarities.filter(\
    ((func.col("movie1") == movieID) | (func.col("movie2") == movieID)) &\
    (func.col("score") > scoreThreshold) & (func.col("numPairs") > coOccurenceThreshold))

# results = filteredResults.sort(func.col("boostedScore").desc()).take(10)
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
