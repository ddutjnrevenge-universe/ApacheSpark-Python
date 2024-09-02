-- Databricks notebook source
-- MAGIC %run ../includes/configuration

-- COMMAND ----------

-- MAGIC %run ../includes/credentials

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;


-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of directories and corresponding table names
-- MAGIC directories = [
-- MAGIC     "circuits", 
-- MAGIC     "constructors", 
-- MAGIC     "drivers", 
-- MAGIC     "laptimes", 
-- MAGIC     "pitstops", 
-- MAGIC     "qualifying"
-- MAGIC ]
-- MAGIC
-- MAGIC # Loop through each directory to create a table for the corresponding Parquet files
-- MAGIC for dir_name in directories:
-- MAGIC     table_name = dir_name.lower()  # Table name derived from directory name
-- MAGIC     parquet_path = f"abfss://processed@formuladatalake1.dfs.core.windows.net/{dir_name}/"
-- MAGIC
-- MAGIC     # Create SQL command to create an external table
-- MAGIC     create_table_sql = f"""
-- MAGIC     CREATE TABLE IF NOT EXISTS f1_processed.{table_name}
-- MAGIC     USING PARQUET
-- MAGIC     LOCATION '{parquet_path}'
-- MAGIC     """
-- MAGIC
-- MAGIC     # Execute SQL command
-- MAGIC     spark.sql(create_table_sql)
-- MAGIC     print(f"Table {table_name} created successfully at {parquet_path}")
-- MAGIC

-- COMMAND ----------

use f1_processed;
select * from f1_processed.circuits

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/results/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC results_df.write.mode("overwrite").saveAsTable("f1_processed.results")
-- MAGIC

-- COMMAND ----------

use f1_processed;
select * from f1_processed.results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC races_df = spark.read.parquet("abfss://processed@formuladatalake1.dfs.core.windows.net/races/")
-- MAGIC races_df.write.mode("overwrite").saveAsTable("f1_processed.races")
-- MAGIC

-- COMMAND ----------

use f1_processed;
select * from f1_processed.races

-- COMMAND ----------

DESC DATABASE f1_processed;