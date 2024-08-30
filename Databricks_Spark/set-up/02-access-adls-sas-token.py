# Databricks notebook source
spark.conf.set( "fs.azure.account.auth.type.formuladatalake1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formuladatalake1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formuladatalake1.dfs.core.windows.net", "<sas-token>")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladatalake1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladatalake1.dfs.core.windows.net/"))