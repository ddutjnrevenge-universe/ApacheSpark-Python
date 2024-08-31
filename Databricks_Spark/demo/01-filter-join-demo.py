# Databricks notebook source
# MAGIC %md
# MAGIC # Filter & Join Transformation

# COMMAND ----------

# MAGIC %run ../includes/credentials

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter

# COMMAND ----------

races_filtered = races_df.filter("race_year == 2019 and round <= 5")

# COMMAND ----------

display(races_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(circuits_df)

# COMMAND ----------

race_circuit_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)
display(race_circuit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Outer

# COMMAND ----------

# right outer Join

right_outer_join_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "right")\
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)
display(right_outer_join_df)

# COMMAND ----------

# Full Outer Join
full_outer_join_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")\
.select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.name, races_df.round)
display(full_outer_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Join

# COMMAND ----------

semi_join_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)
display(semi_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti Join

# COMMAND ----------

anti_join_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "anti")\
    .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)
display(anti_join_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Join
# MAGIC

# COMMAND ----------

cross_join_df = races_df.crossJoin(circuits_df)
display(cross_join_df)

# COMMAND ----------

cross_join_df.count()

# COMMAND ----------

int(races_df.count())*int(circuits_df.count())