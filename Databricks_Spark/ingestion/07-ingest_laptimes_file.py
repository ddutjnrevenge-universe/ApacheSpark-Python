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

laptimes_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("lap", IntegerType(), False),
                                     StructField("position", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

laptimes_df = spark.read \
    .schema(laptimes_schema)\
    .option("multiline", True)\
    .csv("abfss://raw@formuladatalake1.dfs.core.windows.net/lap_times")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

laptimes_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns and add column
laptimes_df_final = laptimes_df.withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(laptimes_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


laptimes_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/laptimes')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/laptimes"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/laptimes")

# COMMAND ----------

display(df)

# COMMAND ----------

