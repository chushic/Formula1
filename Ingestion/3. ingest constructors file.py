# Databricks notebook source
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dlrobin/raw/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlrobin/processed/constructors")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls mnt/formula1dlrobin/processed/constructors

# COMMAND ----------

