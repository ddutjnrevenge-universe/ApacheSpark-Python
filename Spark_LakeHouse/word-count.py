import os
import shutil
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, trim, lower

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Word Count Test Suite") \
    .getOrCreate()

# Define the base data directory
base_data_dir = "./data"  # assuming you have your data in ./data

# Define the batchWC class
class batchWC():
    def __init__(self):
        self.base_data_dir = base_data_dir

    def getRawData(self):
        lines = (spark.read
                 .format("text")
                 .option("lineSep", ".")
                 .load(f"{self.base_data_dir}/data/text")
                 )
        return lines.select(explode(split(lines.value, " ")).alias("word"))

    def getQualityData(self, rawDF):
        return (rawDF.select(lower(trim(rawDF.word)).alias("word"))
                .where("word is not null")
                .where("word rlike '[a-z]'"))

    def getWordCount(self, qualityDF):
        return qualityDF.groupBy("word").count()

    def overwriteWordCount(self, wordCountDF):
        (wordCountDF.write
         .format("parquet")  # Changed to parquet for local filesystem
         .mode("overwrite")
         .saveAsTable("word_count_table"))

    def wordCount(self):
        print(f"\tExecuting Word Count...", end="")
        rawDF = self.getRawData()
        qualityDF = self.getQualityData(rawDF)
        resultDF = self.getWordCount(qualityDF)
        self.overwriteWordCount(resultDF)
        print("Done")

# Define the batch word count test suite class
class batchWCTestSuite():
    def __init__(self):
        self.base_data_dir = base_data_dir

    def cleanTests(self):
        # Drop table if exists
        spark.sql("drop table if exists word_count_table")

        # Remove files and directories
        if os.path.exists(f"{self.base_data_dir}/checkpoint"):
            shutil.rmtree(f"{self.base_data_dir}/checkpoint")
        if os.path.exists(f"{self.base_data_dir}/data/text"):
            shutil.rmtree(f"{self.base_data_dir}/data/text")

        os.makedirs(f"{self.base_data_dir}/data/text")
        print("Done\n")

    def ingestData(self, itr):
        print(f"\tStarting Ingestion...", end='')
        shutil.copy(f"{self.base_data_dir}/text_data_{itr}.txt", f"{self.base_data_dir}/data/text/text_data_{itr}.txt")
        print("Done")

    def assertResult(self, expected_count):
        actual_count = spark.sql("select sum(count) from word_count_table where substr(word, 1, 1) = 's'").collect()[0][0]
        assert expected_count == actual_count, f"Test failed! actual count is {actual_count}"

    def runTests(self):
        self.cleanTests()
        wc = batchWC()  # Use the batchWC class defined above

        print("Testing first iteration of batch word count...")
        self.ingestData(1)
        wc.wordCount()
        self.assertResult(25)
        print("First iteration of batch word count completed.\n")

        print("Testing second iteration of batch word count...")
        self.ingestData(2)
        wc.wordCount()
        self.assertResult(32)
        print("Second iteration of batch word count completed.\n")

        print("Testing third iteration of batch word count...")
        self.ingestData(3)
        wc.wordCount()
        self.assertResult(37)
        print("Third iteration of batch word count completed.\n")

# Create an instance of the test suite and run the tests
bwcTS = batchWCTestSuite()
bwcTS.runTests()
