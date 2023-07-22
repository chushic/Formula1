# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
])

# COMMAND ----------

races_df = spark.read.option("header", True).schema(races_schema).csv("/mnt/formula1dlrobin/raw/races.csv")
races_df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

# COMMAND ----------

race_timestamp = to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
    .withColumn("race_timestamp", race_timestamp) 
display(races_with_timestamp_df)

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------



# COMMAND ----------

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dlrobin/processed/races')

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dlrobin/processed/races

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlrobin/processed/races"))

# COMMAND ----------

