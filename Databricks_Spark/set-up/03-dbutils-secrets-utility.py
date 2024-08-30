# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula-scope')

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula-scope', key = 'formuladatalake1-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladatalake1.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladatalake1.dfs.core.windows.net/"))