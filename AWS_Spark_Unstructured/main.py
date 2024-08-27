from numpy.ma.setup import configuration
from pyspark.sql import SparkSession
from config.config import configuration
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    spark = (SparkSession.builder.appName("AWS_Spark_Unstructured")
             .config('spark.jars.package',
                     'org.apache.hadoop:hadoop-aws:3.3.1,'
                     'com.amazonaws:aws-java-sdk:1.11.469')
             .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
             .config('spark.hadoop.fs.access.key', configuration.get('AWS_ACCESS_KEY'))
             .config('spark.hadoop.fs.secret.key', configuration.get('AWS_SECRET_KEY'))
             .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                     'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
             .getOrCreate())

    text_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_text'
    json_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_json'
    csv_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_csv'
    pdf_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_pdf'
    video_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_video'
    img_input_dir = 'file://F:/Data_Engineering/Apache_Spark/AWS_Spark_Unstructured/input/input_img'

    data_schema = StructType([
        StructField('file_name', StringType(), True),
        StructField('position', StringType(), True),
        StructuField('classcode', StringType(), True),
        StructField('salary_start', StringType(), True,
        StructField('salary_end', StringType(), True),
        StructField('start_date', StringType(), True),
        StructField('end_date', StringType(), True),
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



