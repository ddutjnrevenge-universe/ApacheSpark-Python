# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import  when, sum, count, col

driver_standings_df = race_results_df\
    .groupBy("race_year", "driver_name", "driver_nationality", "team")\
    .agg(sum("points").alias("total_points"),
          count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

display(driver_standings_df.filter("race_year == 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))

final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

display(final_df.filter("race_year==2020"))