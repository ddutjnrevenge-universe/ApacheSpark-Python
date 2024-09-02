# Databricks notebook source
# MAGIC %md
# MAGIC # Infer Schema

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula-scope', key = 'formuladatalake1-account-key')
spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@formuladatalake1.dfs.core.windows.net"))

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
    .schema(constructors_schema)\
    .json(f"abfss://raw@formuladatalake1.dfs.core.windows.net/{v_file_date}/constructors.json")

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

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# rename columns and add column
constructors_df_final = constructors_df.withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
    .withColumn('ingestion_date', current_timestamp())\
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructors_df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Write Data to datalake as Parquet

# COMMAND ----------


constructors_df_final.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/constructors"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_processed.constructors;