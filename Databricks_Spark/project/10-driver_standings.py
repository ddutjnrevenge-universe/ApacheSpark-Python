# Databricks notebook source
# MAGIC %md
# MAGIC ##### Produce driver standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

# MAGIC %md
# MAGIC Find race years for which the data is to be reprocessed

# COMMAND ----------

# display(dbutils.fs.ls(f"{presentation_folder_path}/race_results/"))
# test_df = spark.read.parquet(f"{presentation_folder_path}/race_results/")
# display(test_df)

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results").filter(f"file_date = '{v_file_date}'") 

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import col

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality", "team") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("append").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

overwrite_partition(final_df, 'f1_presentation', 'driver_standings', 'race_year')

# COMMAND ----------

# display(dbutils.fs.ls(f"{presentation_folder_path}/driver_standings"))
test = spark.read.parquet(f"{presentation_folder_path}/driver_standings")
display(test)
# dbutils.fs.rm(f"{presentation_folder_path}/driver_standings", recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table if exists f1_presentation.driver_standings;