# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC # Load data

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists f1_demo
# MAGIC location 'hive_metastore'
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls(f"{raw_folder_path}/2021-03-28"))

# COMMAND ----------

results_df = spark.read\
    .option("inferSchema", True)\
    .json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("append").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# results_df.write.format("delta").mode("append").
# save to {demo_folder_path}/results_managed
results_df.write.format("delta").mode("append").save(f"{demo_folder_path}/results_managed")

# COMMAND ----------

display(dbutils.fs.ls(f"{demo_folder_path}/results_partitioned"))


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_external")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/results_external")

# COMMAND ----------

results_external_def = spark.read.format("delta").load(f"{demo_folder_path}/results_external")

# COMMAND ----------

display(results_external_def)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")


# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").save(f"{demo_folder_path}/results_partitioned")


# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC # Update and Delete

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC update f1_demo.results_managed
# MAGIC   set points = 11 - position
# MAGIC where position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/results_managed")

deltaTable.update("position <= 10", { "points": "21 - position" } ) 

# COMMAND ----------

deltaTable.delete("points=0")

# COMMAND ----------

results_managed_def = spark.read.format("delta").load(f"{demo_folder_path}/results_managed")

# COMMAND ----------

display(results_managed_def)

# COMMAND ----------

# MAGIC %md
# MAGIC # Upsert using MERGE

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")


# COMMAND ----------


from pyspark.sql.functions import upper

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))


# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")


# COMMAND ----------

display(drivers_day2_df)


# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 1

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Day 3

# COMMAND ----------

display(dbutils.fs.ls(f"{demo_folder_path}"))

# COMMAND ----------

# Check if directory exists
if not any(f.name == "drivers_merge" for f in dbutils.fs.ls(demo_folder_path)):
    dbutils.fs.mkdirs(f"{demo_folder_path}/drivers_merge")


# COMMAND ----------

# Create a Delta table if it doesn't exist
drivers_day3_df.write.format("delta").save(f"{demo_folder_path}/drivers_merge")


# COMMAND ----------

# Load the Delta table and print the schema
delta_df = spark.read.format("delta").load(f"{demo_folder_path}/drivers_merge")
delta_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp

# Add missing columns with default values to the DataFrame
drivers_day3_df = drivers_day3_df.withColumn("updatedDate", lit(None).cast("timestamp"))
drivers_day3_df = drivers_day3_df.withColumn("createdDate", current_timestamp())

# Write the updated DataFrame back to Delta table
drivers_day3_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{demo_folder_path}/drivers_merge")


# COMMAND ----------


from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f"{demo_folder_path}/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO f1_demo.drivers_merge AS tgt
# MAGIC USING (SELECT driverId, dob, forename, surname FROM drivers_day3) AS upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED THEN 
# MAGIC   INSERT (driverId, dob, forename, surname, createdDate)
# MAGIC   VALUES (upd.driverId, upd.dob, upd.forename, upd.surname, current_timestamp());
# MAGIC

# COMMAND ----------

# MAGIC %sql SELECT * FROM f1_demo.drivers_merge;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##
# MAGIC ## 1. History & Versioning
# MAGIC ## 2. Tiime Travel
# MAGIC ## 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 4;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-01T07:17:36.000+00:00';
# MAGIC

# COMMAND ----------

df = spark.table("f1_demo.drivers_merge")
from pyspark.sql.functions import col, to_timestamp

# Cast columns to match the Delta table schema
df = df.withColumn("driverId", col("driverId").cast("long"))
df = df.withColumn("dob", col("dob").cast("string"))
df = df.withColumn("createdDate", to_timestamp("createdDate"))
df = df.withColumn("updatedDate", to_timestamp("updatedDate"))
df.printSchema()

# Save the DataFrame to Delta format at the desired location
df.write.format("delta").mode("overwrite").save(f"{demo_folder_path}/drivers_merge")



# COMMAND ----------

df = spark.read.format("delta").load(f"{demo_folder_path}/drivers_merge")
display(df)

# COMMAND ----------

# df = spark.read.format("delta").option("timestampAsOf", '2024-09-01T07:17:36.000+00:00').load(f"{demo_folder_path}/drivers_merge")
# Use a timestamp after the earliest available version
df = spark.read.format("delta").option("timestampAsOf", '2024-09-01T08:16:00.000+00:00').load(f"{demo_folder_path}/drivers_merge")
display(df)


# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY delta.`{demo_folder_path}/drivers_merge`").show(truncate=False))

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-01T07:17:36.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge TIMESTAMP AS OF '2024-09-01T07:17:36.000+00:00';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_demo.drivers_merge VERSION AS OF 4;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 3 src
# MAGIC    ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC    INSERT *

# COMMAND ----------

# MAGIC %sql DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId = 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM  f1_demo.drivers_txn
# MAGIC WHERE driverId = 1;

# COMMAND ----------

for driver_id in range(3, 20):
  spark.sql(f"""INSERT INTO f1_demo.drivers_txn
                SELECT * FROM f1_demo.drivers_merge
                WHERE driverId = {driver_id}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")


df.write.format("parquet").save(f"{demo_folder_path}/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@formuladatalake1.dfs.core.windows.net/drivers_convert_to_delta_new`
# MAGIC
# MAGIC

# COMMAND ----------

df = spark.table("f1_demo.drivers_txn")


df.write.format("parquet").save(f"{demo_folder_path}/drivers_txn_new")

# COMMAND ----------

display(dbutils.fs.ls(f"{demo_folder_path}/drivers_txn_new"))