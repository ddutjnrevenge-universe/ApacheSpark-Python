from numpy.ma.setup import configuration
from pandas import DataFrame
from pyspark.sql import SparkSession
# from config.config import configuration
import json

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from udf_utils import *
from pyspark.sql.functions import udf, regexp_replace

with open('config/config.json') as f:
    configuration = json.load(f)

ACCESS_KEY = configuration.get('AWS_ACCESS_KEY')
SECRET_KEY = configuration.get('AWS_SECRET_KEY')

def define_udfs():
    return {
        'extract_file_name_udf': udf(extract_file_names, StringType()),
        'extract_position_udf': udf(extract_position, StringType()),
        'extract_salary_udf': udf(extract_salary, StructType([
                StructField('salary_start', DoubleType(), True),
                StructField('salary_end', DoubleType(), True)
                ])),
        'extract_start_date_udf': udf(extract_start_date, DateType()),
        'extract_end_date_udf': udf(extract_end_date, DateType()),
        'extract_classcode_udf': udf(extract_classcode, StringType()),
        'extract_req_udf': udf(extract_req, StringType()),
        'extract_notes_udf': udf(extract_notes, StringType()),
        'extract_duties_udf': udf(extract_duties, StringType()),
        'extract_selection_udf': udf(extract_selection, StringType()),
        'extract_experience_length_udf': udf(extract_experience_length, StringType()),
        'extract_education_length_udf': udf(extract_education_length, StringType()),
        'extract_application_location_udf': udf(extract_application_location, StringType())

    }

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("AWS_Spark_Unstructured")
             .config('spark.jars.package',
                     'org.apache.hadoop:hadoop-aws:3.3.1,'
                     'com.amazonaws:aws-java-sdk:1.11.469')
             .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
            #  .config('spark.hadoop.fs.access.key', configuration.get('AWS_ACCESS_KEY'))
            #  .config('spark.hadoop.fs.secret.key', configuration.get('AWS_SECRET_KEY'))
                .config('spark.hadoop.fs.s3a.access.key', ACCESS_KEY)
                .config('spark.hadoop.fs.s3a.secret.key', SECRET_KEY)
             .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                     'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
             .getOrCreate())

    text_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_text'
    json_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_json'
    csv_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_csv'
    pdf_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_pdf'
    video_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_video'
    img_input_dir = 'file:///F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_img'

    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructField('classcode', StringType(), True),
        StructField('salary_start', DoubleType(), True),
        StructField('salary_end', DoubleType(), True),
        StructField('start_date', DateType(), True),
        StructField('end_date', DateType(), True),
        StructField('req', StringType(), True),
        StructField('notes', StringType(), True),
        StructField('duties', StringType(), True),
        StructField('selection', StringType(), True),
        StructField('experience_length', StringType(), True),
        StructField('job_type', StringType(), True),
        StructField('education_length', StringType(), True),
        StructField('school_type', StringType(), True),
        StructField('application_location', StringType(), True)
        ])

udfs = define_udfs()

job_bulletins_df = (spark.readStream
                    .format('text')
                    .option('wholetext', 'true')
                    .load(text_input_dir)
                )


# -------- Text Files --------
# job_bulletins_df.show()
job_bulletins_df = job_bulletins_df.withColumn('file_name', 
                                               regexp_replace(udfs['extract_file_name_udf']('value'), r'\r', ' '))
job_bulletins_df = job_bulletins_df.withColumn('value', 
                                               regexp_replace(regexp_replace('value', r'\n', ' '), r'\r', ' '))
job_bulletins_df = job_bulletins_df.withColumn('position', 
                                               regexp_replace(udfs['extract_position_udf']('value'), r'\r', ' '))
job_bulletins_df = job_bulletins_df.withColumn('salary_start', 
                                               udfs['extract_salary_udf']('value').getField('salary_start'))
job_bulletins_df = job_bulletins_df.withColumn('salary_end', 
                                               udfs['extract_salary_udf']('value').getField('salary_end'))
job_bulletins_df = job_bulletins_df.withColumn('start_date', 
                                               udfs['extract_start_date_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('end_date', 
                                               udfs['extract_end_date_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('classcode',
                                               udfs['extract_classcode_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('req',
                                               udfs['extract_req_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('notes',
                                               udfs['extract_notes_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('duties',
                                               udfs['extract_duties_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('selection',
                                               udfs['extract_selection_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('experience_length',
                                               udfs['extract_experience_length_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('education_length',
                                               udfs['extract_education_length_udf']('value'))
job_bulletins_df = job_bulletins_df.withColumn('application_location',
                                               udfs['extract_application_location_udf']('value'))

job_bulletins_df = job_bulletins_df.select('file_name', 'start_date', 'end_date', 'salary_start', 'salary_end', \
                               'classcode', 'req', 'notes', 'duties', 'selection', 'experience_length', \
                                'education_length', 'application_location')
# ---------------------------

# -------- JSON Files --------
json_df = spark.readStream.json(json_input_dir, schema=data_schema, multiLine=True)

json_df = json_df.select('file_name', 'start_date', 'end_date', 'salary_start', 'salary_end', \
                               'classcode', 'req', 'notes', 'duties', 'selection', 'experience_length', \
                                'education_length', 'application_location')


union_df = job_bulletins_df.union(json_df)

def streamWriter(input: DataFrame, checkpointFolder, output):
    return (input.writeStream
            .format('parquet')
            .outputMode('append')
            # .option('truncate', 'false')
            .option('path', output)
            .option('checkpointLocation', checkpointFolder)
            .trigger(processingTime='5 seconds')
            .start())

query = streamWriter(union_df, 's3a://batch10-nguyentnh4-spark-unstructured/checkpoints/', 
                     's3a://batch10-nguyentnh4-spark-unstructured/data/spark_unstructured')

query.awaitTermination()
