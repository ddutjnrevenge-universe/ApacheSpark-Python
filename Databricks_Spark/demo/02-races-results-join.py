# Databricks notebook source
# MAGIC %md
# MAGIC # Join Race Results

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("nationality", "driver_nationality")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")\
    .withColumnRenamed("name","team")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
    .withColumnRenamed("location", "circuit_location")
races_df = spark.read.parquet(f"{processed_folder_path}/races")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("date", "race_date") 
results_df = spark.read.parquet(f"{processed_folder_path}/results")\
    .withColumnRenamed("time", "race_time")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id)\
  .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)
  

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join Results to other dfs

# COMMAND ----------

races_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id)\
  .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
  .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

display(races_results_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = races_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid","fastest_lap", "race_time","points")\
    .withColumn("created_at", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year==2020 and race_name=='Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

presentation_folder_path

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")