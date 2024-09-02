# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

# MAGIC %md
# MAGIC ## Local Temp View

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

race_results_2019 = spark.sql("SELECt * FROM v_race_results where race_year = 2019")

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Global Temp View

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results;