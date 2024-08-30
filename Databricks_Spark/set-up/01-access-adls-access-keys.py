# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    "<access-key>")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladatalake1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladatalake1.dfs.core.windows.net/"))