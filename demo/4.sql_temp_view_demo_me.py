# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from v_race_results
# MAGIC where race_year = 2020

# COMMAND ----------

temp = spark.sql("select * from v_race_results where race_year=2020")

# COMMAND ----------

display(temp)

# COMMAND ----------

# MAGIC %md
# MAGIC ## global temp view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC   FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()

# COMMAND ----------

