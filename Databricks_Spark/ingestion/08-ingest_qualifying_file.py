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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), False),
                                     StructField("constructorId", IntegerType(), False),
                                     StructField("number", IntegerType(), False),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)])


# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema)\
    .option("multiline", True)\
    .json("abfss://raw@formuladatalake1.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

qualifying_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns and add column
qualifying_df_final = qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('constructorId', 'constructor_id')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(qualifying_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


qualifying_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/qualifying')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/qualifying"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/qualifying")

# COMMAND ----------

display(df)

# COMMAND ----------

