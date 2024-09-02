# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

# MAGIC %md
# MAGIC # Aggregation Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Built-in functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter("race_year == 2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(countDistinct('race_name')).show()

# COMMAND ----------

# Total points scored by Lewis Hamilton and number of races
demo_df.filter("driver_name='Lewis Hamilton'").select(sum('points'), countDistinct('race_name'))\
    .withColumnRenamed('sum(points)', 'total_points')\
    .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races')\
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group By

# COMMAND ----------

display(demo_df.groupBy('driver_name')\
  .agg(sum('points'), countDistinct('race_name'))\
  .withColumnRenamed('sum(points)', 'total_points')\
  .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Window Functions

# COMMAND ----------

df_1920 = race_results_df.filter("race_year in (2019, 2020)")
display(df_1920)

# COMMAND ----------

grouped_df = df_1920.groupBy("race_year", "driver_name")\
  .agg(sum('points'), countDistinct('race_name'))\
  .withColumnRenamed('sum(points)', 'total_points')\
  .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races')


# COMMAND ----------

display(grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(desc('total_points'))

# COMMAND ----------

display(grouped_df.withColumn("rank", rank().over(driverRankSpec)))

# COMMAND ----------

