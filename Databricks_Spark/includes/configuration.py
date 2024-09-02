# Databricks notebook source
raw_folder_path = 'abfss://raw@formuladatalake1.dfs.core.windows.net'
processed_folder_path = 'abfss://processed@formuladatalake1.dfs.core.windows.net'
presentation_folder_path = 'abfss://presentation@formuladatalake1.dfs.core.windows.net'
demo_folder_path = 'abfss://demo@formuladatalake1.dfs.core.windows.net'

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula-scope', key = 'formuladatalake1-account-key')
spark.conf.set(
    "fs.azure.account.key.formuladatalake1.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

