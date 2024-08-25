# If on Windows -> using wsl
# Put your private key file "[ec2-key-pair-name].pem" in the home directory
# Open terminal and run the following commands
wsl
ls # Check if the private key file is in the home directory
mv [ec2-key-pair-name].pem ~/ # Move the private key file to the home directory as wsl might not handle changing permission same as linux
chmod 400 ~/[ec2-key-pair-name].pem # Change permission of the private key file as required by AWS, otherwise you will get "WARNING: UNPROTECTED PRIVATE KEY FILE! and " "Permissions are too open" error
ls -l ~/[ec2-key-pair-name].pem # Check if the permission is changed
# Remember to add addtional inbound rule to the security group of the ec2 instance to allow ssh connection from your IP address/anywhere (not recommended)
# Start ssh to the ec2 instance
ssh -i ~/[ec2-key-pair-name].pem hadoop@[ec2-public-dns]
ssh -i 
# Then it will ask you to confirm the connection, type "yes" and press enter
# If it pop up the screen with title "Amazon Linux 2023" with the bird and EMR logo, you are in the right place
# Prepare the script for the Spark job
nano spark-etl.py
# Copy the content of the script to the terminal
```python
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":

    print(len(sys.argv))
    if (len(sys.argv) != 3):
        print("Usage: spark-etl [input-folder] [output-folder]")
        sys.exit(0)

    spark = SparkSession\
        .builder\
        .appName("SparkETL")\
        .getOrCreate()

    nyTaxi = spark.read.option("inferSchema", "true").option("header", "true").csv(sys.argv[1])

    updatedNYTaxi = nyTaxi.withColumn("current_date", lit(datetime.now()))

    updatedNYTaxi.printSchema()

    print(updatedNYTaxi.show())

    print("Total number of records: " + str(updatedNYTaxi.count()))

    updatedNYTaxi.write.mode("overwrite").parquet(sys.argv[2]) # Overwrite as by default, Spark will not overwrite an existing directory unless explicitly instructed to do so.
```
# Run the Spark job
spark-submit spark-etl.py [s3 input URI path] [s3 output URI path]
# If the job runs successfully, you will see the output in the terminal with the number of records and the schema of the output
# Output will be saved in the output folder in parquet format
# Can check the job details in the Spark UI through Spark History Server and YARN Timeline Server

# If want to run directly on the EMR cluster 
# 1. Adding step to the EMR cluster
# Go to the EMR console
# Click on the cluster you want to run the job
# Click on "Add step" button
# Create custom jar and add jar arguments
# Name: `command-runner.jar`
# For the script, you can upload the script to S3 and provide the S3 URI path
arguments: `spark-submit [s3 script URI path] [s3 input URI path] [s3 output URI path]`


