import sys
from pyspark.sql import *
from lib.transformation import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("PySpark_Test").getOrCreate()
    if len(sys.argv) != 2:
        print("Usage: main.py <data_file>")
        sys.exit(-1)

    data_file = sys.argv[1]
    print(f"Loading data from: {data_file}")

    df = load_survey_df(spark, data_file)
    count_df = count_by_country(df)
    count_df.show()
    print(f"Total count: {count_df.count()}")
    print("Finished file processing" + data_file)
    