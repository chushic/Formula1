# Databricks notebook source
# MAGIC %md
# MAGIC ## Step 1 - Read the input file using Spark df reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

# MAGIC  %fs
# MAGIC ls /mnt/formula1dlrobin/raw

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = spark.read.option("header", True).schema(circuits_schema).csv("/mnt/formula1dlrobin/raw/circuits.csv")

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC select specific columns

# COMMAND ----------

circuits_df_selected = circuits_df.select("circuitId", 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')
circuits_df_selected.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("Ingestion_date", current_timestamp()) \
#    .withColumn("env", lit("production"))

# COMMAND ----------

circuits_final_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC write data to the datalake

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dlrobin/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlrobin/processed/circuits

# COMMAND ----------

df = spark.read.parquet("/mnt/formula1dlrobin/processed/circuits")
display(df)

# COMMAND ----------

