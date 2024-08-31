# Databricks notebook source
# MAGIC %md
# MAGIC # Infer Schema

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula-scope', key = 'formuladatalake1-account-key')
spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formuladatalake1.dfs.core.windows.net"))

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields =[StructField("forename", StringType(), True), StructField("surname", StringType(), True)])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

drivers_df = spark.read \
    .schema(drivers_schema)\
    .json("abfss://raw@formuladatalake1.dfs.core.windows.net/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

drivers_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

# Drop url column
from pyspark.sql.functions import col
drivers_df = drivers_df.drop('url')
display(drivers_df)

# COMMAND ----------

drivers_df.columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

# rename columns and add column
drivers_df_final = drivers_df.withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('driverRef', 'driver_ref')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname")))

# COMMAND ----------

display(drivers_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


drivers_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/drivers')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/drivers"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/drivers")

# COMMAND ----------

display(df)

# COMMAND ----------

