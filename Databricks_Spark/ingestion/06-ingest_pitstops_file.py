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

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("stop", IntegerType(), False),
                                     StructField("lap", IntegerType(), False),
                                     StructField("time", StringType(), False),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", IntegerType(), True)])


# COMMAND ----------

pitstops_df = spark.read \
    .schema(pitstops_schema)\
    .option("multiline", True)\
    .json("abfss://raw@formuladatalake1.dfs.core.windows.net/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

pitstops_df.printSchema()

# COMMAND ----------

pitstops_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns and add column
pitstops_df_final = pitstops_df.withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(pitstops_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


pitstops_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/pitstops')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/pitstops"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/pitstops")

# COMMAND ----------

display(df)

# COMMAND ----------

