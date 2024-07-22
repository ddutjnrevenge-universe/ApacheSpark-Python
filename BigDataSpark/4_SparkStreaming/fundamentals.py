import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("JSON Streaming Example") \
    .getOrCreate()

# Define the schema for the JSON data
jsonSchema = StructType([
    StructField("quiz", StructType([
        StructField("sport", StructType([
            StructField("q1", StructType([
                StructField("question", StringType(), True),
                StructField("options", StringType(), True),
                StructField("answer", StringType(), True)
            ]))
        ])),
        StructField("maths", StructType([
            StructField("q1", StructType([
                StructField("question", StringType(), True),
                StructField("options", StringType(), True),
                StructField("answer", StringType(), True)
            ])),
            StructField("q2", StructType([
                StructField("question", StringType(), True),
                StructField("options", StringType(), True),
                StructField("answer", StringType(), True)
            ]))
        ]))
    ]))
])

# Specify the input path
inputPath = "F:\Data_Engineering\Apache_Spark\data\example_2.json"

streamingInputDF = (
    spark
    .readStream
    .schema(jsonSchema) # set schema of the JSON data
    .option("maxFilesPerTrigger", 1)
    .json(inputPath)
)

# Extract fields from the nested JSON structure
extractedDF = streamingInputDF.selectExpr(
    "quiz.sport.q1.question as sport_question",
    "quiz.sport.q1.answer as sport_answer",
    "quiz.maths.q1.question as maths_question1",
    "quiz.maths.q1.answer as maths_answer1",
    "quiz.maths.q2.question as maths_question2",
    "quiz.maths.q2.answer as maths_answer2"
)

streamingCountsDF = (
    extractedDF
    .groupBy(
        "sport_question",
        window(current_timestamp(), "1 hour")
    )
    .count()
)

query = (
    streamingCountsDF
    .writeStream
    .outputMode("complete")
    .format("console")
    .start()
)

query.awaitTermination()