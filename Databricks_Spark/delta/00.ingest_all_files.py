# Databricks notebook source
# MAGIC %sql
# MAGIC -- create database f1_processed_delta
# MAGIC -- location 'hive_metastore'
# MAGIC -- create database f1_presentation_delta
# MAGIC -- location 'hive_metastore'

# COMMAND ----------

v_result = dbutils.notebook.run("01.ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("02.ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("03.ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("04.ingest_drivers_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("05.ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("06.ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("07.ingest_lap_times_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("08.ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-28"})

# COMMAND ----------

v_result

# COMMAND ----------

