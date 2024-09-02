# Databricks notebook source
# MAGIC %sql
# MAGIC create database demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases;

# COMMAND ----------

# MAGIC %sql
# MAGIC describe database demo

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

# COMMAND ----------

# MAGIC %sql 
# MAGIC use demo;
# MAGIC show tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc race_results_python

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demo.race_results_python;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table race_results_sql
# MAGIC as 
# MAGIC select * from demo.race_results_python
# MAGIC where race_year = 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_sql

# COMMAND ----------

race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended demo.race_results_ext_py

# COMMAND ----------

# MAGIC %sql
# MAGIC create table demo.race_results_ext_sql
# MAGIC (race_year INT,
# MAGIC race_name STRING,
# MAGIC race_date STRING,
# MAGIC circuit_location STRING,
# MAGIC driver_name STRING,
# MAGIC driver_number INT,
# MAGIC driver_nationality STRING,
# MAGIC team STRING,
# MAGIC grid INT,
# MAGIC fastest_lap INT,
# MAGIC race_time STRING,
# MAGIC points FLOAT,
# MAGIC position INT, 
# MAGIC created_date TIMESTAMP
# MAGIC )
# MAGIC USINg parquet
# MAGIC LOCATION "abfss://presentation@formuladatalake1.dfs.core.windows.net/race_results_ext_sql"

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into race_results_ext_sql
# MAGIC select * from demo.race_results_ext_py where race_year = 2020;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table demo.race_results_ext_sql;
# MAGIC drop table demo.race_results_sql

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in demo;