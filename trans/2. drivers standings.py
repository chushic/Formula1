# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results") 

# COMMAND ----------

from pyspark.sql.functions import sum, when, col, count 

# COMMAND ----------

 driver_standing_df = race_results_df \
    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
    .agg(sum(col("points")).alias("total_points"), 
        count(when(col("position")==1, True)).alias("wins")
        )

# COMMAND ----------

display(driver_standing_df.filter("race_year==2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_desc_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standing_df.withColumn("rank", rank().over(driver_desc_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

