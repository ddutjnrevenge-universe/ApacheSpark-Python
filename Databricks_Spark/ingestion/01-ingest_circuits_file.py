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

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

circuits_schema = StructType([
    StructField("circuitId", IntegerType(), False),
    StructField("circuitRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
    StructField("alt", IntegerType(), True),
    StructField("url", StringType(), True)
    ])


# COMMAND ----------

circuits_df = spark.read \
    .option("header",True)\
    .schema(circuits_schema)\
    .csv("abfss://raw@formuladatalake1.dfs.core.windows.net/circuits.csv")

# COMMAND ----------


display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

# Drop url column
circuits_df = circuits_df.drop('url')
display(circuits_df)

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

# rename columns
circuits_df = circuits_df.withColumnRenamed('circuitId', 'circuit_id')\
    .withColumnRenamed('circuitRef', 'circuit_ref')\
    .withColumnRenamed('name', 'circuit_name')\
    .withColumnRenamed('lat', 'latitude')\
    .withColumnRenamed('lng', 'longtitude')\
    .withColumnRenamed('alt', 'altitude')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_df_final = circuits_df.withColumn("ingestion_date", current_timestamp())\
                                .withColumn("env", lit("Production"))
    

# COMMAND ----------

display(circuits_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


circuits_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/circuits')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/circuits"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/circuits")

# COMMAND ----------

display(df)