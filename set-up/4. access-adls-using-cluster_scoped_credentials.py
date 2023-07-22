# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azire.account.jey
# MAGIC 2. List ..
# MAGIC 3. ...

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlrobin.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlrobin.dfs.core.windows.net/circuits.csv"))


# COMMAND ----------

