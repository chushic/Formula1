# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config fs.azire.account.jey
# MAGIC 2. List ..
# MAGIC 3. ...

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dlrobin.dfs.core.windows.net",
    "VuI50zE0P9vf/BKpWUI8CqKhMtyPkNYQoU4r3xVcrMqQOVLBWsRsrljhypagElH0hN+tDtWUcs9/+AStCF2hpw==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlrobin.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlrobin.dfs.core.windows.net/circuits.csv"))


# COMMAND ----------

