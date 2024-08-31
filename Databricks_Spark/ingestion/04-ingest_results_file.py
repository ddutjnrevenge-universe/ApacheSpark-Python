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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), False),
                                    StructField("driverId", IntegerType(), False),
                                    StructField("constructorId", IntegerType(), False),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), False),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), False),
                                    StructField("positionOrder", IntegerType(), False),
                                    StructField("points", DoubleType(), False),
                                    StructField("laps", IntegerType(), False),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", DoubleType(), True),
                                    StructField("statusId", IntegerType(), False)])



# COMMAND ----------

results_df = spark.read \
    .schema(results_schema)\
    .json("abfss://raw@formuladatalake1.dfs.core.windows.net/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

results_df.printSchema()

# COMMAND ----------

results_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

results_df.columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

# rename columns and add column
results_df_final = results_df.withColumnRenamed('resultId', 'result_id')\
                            .withColumnRenamed('raceId', 'race_id')\
                            .withColumnRenamed('driverId', 'driver_id')\
                            .withColumnRenamed('constructorId', 'constructor_id')\
                            .withColumnRenamed('positionText','position_text')\
                            .withColumnRenamed('positionOrder','position_order')\
                            .withColumnRenamed('fastestLap','fastest_lap')\
                            .withColumnRenamed('fastestLapTime','fastest_lap_time')\
                            .withColumnRenamed('fastestLapSpeed','fastest_lap_speed')\
                            .withColumn('ingestion_date', current_timestamp())    

# COMMAND ----------

display(results_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


results_df_final.write.mode("overwrite").partitionBy('race_id').parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/results')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/results"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/results")

# COMMAND ----------

display(df)

# COMMAND ----------

