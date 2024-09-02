# Databricks notebook source
# MAGIC %md
# MAGIC # Infer Schema

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_data_source

# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

raw_folder_path

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula-scope', key = 'formuladatalake1-account-key')
spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls(raw_folder_path))

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
    .json(f"{raw_folder_path}/qualifying")

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

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# rename columns and add column
qualifying_df_final = add_ingestion_date(qualifying_df.withColumnRenamed('qualifyId', 'qualify_id')\
    .withColumnRenamed('driverId', 'driver_id')\
    .withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('constructorId', 'constructor_id'))\
    .withColumn('testing', lit(v_data_source))
    # .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(qualifying_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


qualifying_df_final.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(df)

# COMMAND ----------

