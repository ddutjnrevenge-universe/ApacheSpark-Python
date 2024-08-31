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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
    ])


# COMMAND ----------

races_df = spark.read \
    .option("header",True)\
    .schema(races_schema)\
    .csv("abfss://raw@formuladatalake1.dfs.core.windows.net/races.csv")

# COMMAND ----------


display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

# Drop url column
races_df = races_df.drop('url')
display(races_df)

# COMMAND ----------

races_df.columns

# COMMAND ----------

# rename columns
races_df = races_df.withColumnRenamed('raceId', 'race_id')\
    .withColumnRenamed('year', 'race_year')\
    .withColumnRenamed('circuitId', 'circuit_id')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat, col

# COMMAND ----------

races_df_final = races_df.withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(races_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


races_df_final.write.mode("overwrite").partitionBy('race_year').parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/races')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/races"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/races")

# COMMAND ----------

display(df)

# COMMAND ----------

