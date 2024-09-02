-- Databricks notebook source
-- MAGIC %run ../includes/configuration

-- COMMAND ----------

-- MAGIC %run ../includes/credentials

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List of directories and corresponding table names
-- MAGIC directories = [
-- MAGIC     "race_results",
-- MAGIC     "driver_standings",
-- MAGIC     "constructor_standings"
-- MAGIC ]
-- MAGIC
-- MAGIC # Loop through each directory to create a table for the corresponding Parquet files
-- MAGIC for dir_name in directories:
-- MAGIC     table_name = dir_name.lower()  # Table name derived from directory name
-- MAGIC     parquet_path = f"abfss://presentation@formuladatalake1.dfs.core.windows.net/{dir_name}/"
-- MAGIC
-- MAGIC     # Create SQL command to create an external table
-- MAGIC     create_table_sql = f"""
-- MAGIC     CREATE TABLE IF NOT EXISTS f1_presentation.{table_name}
-- MAGIC     USING PARQUET
-- MAGIC     LOCATION '{parquet_path}'
-- MAGIC     """
-- MAGIC
-- MAGIC     # Execute SQL command
-- MAGIC     spark.sql(create_table_sql)
-- MAGIC     print(f"Table {table_name} created successfully at {parquet_path}")
-- MAGIC

-- COMMAND ----------

DESC DATABASE f1_presentation;