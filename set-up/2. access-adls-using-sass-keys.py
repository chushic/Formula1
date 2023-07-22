# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS tokens
# MAGIC 1. Set the spark config fs.azire.account.jey
# MAGIC 2. List ..
# MAGIC 3. ...

# COMMAND ----------



spark.conf.set("fs.azure.account.auth.type.formula1dlrobin.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dlrobin.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dlrobin.dfs.core.windows.net", "sp=rl&st=2023-06-08T21:39:33Z&se=2023-06-09T05:39:33Z&spr=https&sv=2022-11-02&sr=c&sig=PfcMnbnt%2FHb3Kr8nwqt0MtgpGqfl%2F423z50oad4RTtg%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlrobin.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlrobin.dfs.core.windows.net/circuits.csv"))


# COMMAND ----------

