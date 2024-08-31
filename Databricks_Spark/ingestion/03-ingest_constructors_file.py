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

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema)\
    .json("abfss://raw@formuladatalake1.dfs.core.windows.net/constructors.json")

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.printSchema()

# COMMAND ----------

constructors_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Processing

# COMMAND ----------

# Drop url column
from pyspark.sql.functions import col
constructors_df = constructors_df.drop('url')
display(constructors_df)

# COMMAND ----------

constructors_df.columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# rename columns and add column
constructors_df_final = constructors_df.withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

display(constructors_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


constructors_df_final.write.mode("overwrite").parquet('abfss://processed@formuladatalake1.dfs.core.windows.net/constructors')

# COMMAND ----------

display(dbutils.fs.ls("abfss://processed@formuladatalake1.dfs.core.windows.net/constructors"))

# COMMAND ----------

df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/constructors")

# COMMAND ----------

display(df)

# COMMAND ----------

