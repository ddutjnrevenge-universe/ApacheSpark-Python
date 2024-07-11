from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("data/ml-100k/u.item", "r", encoding='UTF-8', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("MovieRecs").getOrCreate()

movieSchema = StructType([\
                StructField("userID", IntegerType(), True),\
                StructField("movieID", IntegerType(), True),\
                StructField("rating", IntegerType(), True),\
                StructField("timestamp", LongType(), True)\
            ])
 
names = loadMovieNames()

ratings = spark.read.option("sep","\t").schema(movieSchema)\
    .csv("data/ml-100k/u.data")

print("\nTraining recommendation model...")

als = ALS().setMaxIter(5).setRegParam(0.01)\
    .setUserCol("userID").setItemCol("movieID") \
    .setRatingCol("rating")

model = als.fit(ratings)

userID = int(sys.argv[1])
userSchema = StructType([StructField("userID", IntegerType(), True)])
users = spark.createDataFrame([[userID,]], userSchema)

recommendations = model.recommendForUserSubset(users, 10).collect()

print("\nTop 10 recommendations for user ID " + str(userID))

for userRecs in recommendations:
    myRecs = userRecs[1] #userRecs is (userID, [Row(movieID, rating), Row(movieID, rating)...]
    for rec in myRecs: # my Recs is just the column of recs for the user
        movie = rec[0] # For each rec in myRecs, movie is the movieID, rec[1] is the rating
        rating = rec[1]
        movieName = names[movie]
        print(movieName + str(rating))