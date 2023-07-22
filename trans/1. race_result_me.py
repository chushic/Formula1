# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

constructors_df = spark.read.format("parquet").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

circuits_df = spark.read.format("parquet").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

races_df = spark.read.format("parquet").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

results_df = spark.read.format("parquet").load(f"{processed_folder_path}/results") \
.withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC join the dataframes

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

race_circuits_df = race_circuits_df.withColumnRenamed("race_id", "race_id_race")
race_results_df = results_df.join(race_circuits_df, results_df.race_id==race_circuits_df.race_id_race, "inner") \
    .join(drivers_df, results_df.driver_id==drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id==constructors_df.constructor_id, "inner")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                            .withColumn("create_date", current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

 final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

