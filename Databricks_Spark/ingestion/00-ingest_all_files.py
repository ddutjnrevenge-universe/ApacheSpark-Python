# Databricks notebook source
v_result = dbutils.notebook.run("01-ingest_circuits_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("02-ingest_races_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("03-ingest_constructors_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("04-ingest_drivers_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("05-ingest_results_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("06-ingest_pitstops_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("07-ingest_laptimes_file",0,{"p_data_source": "Ergast API"})
v_result = dbutils.notebook.run("08-ingest_qualifying_file",0,{"p_data_source": "Ergast API"})

v_result

# COMMAND ----------

