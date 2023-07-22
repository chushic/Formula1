# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/ password for the Application
# MAGIC 3. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 4. Assign Role 'Storage Blob Data Contributor' to the Data Lake. 

# COMMAND ----------

client_id = "e8bcd822-e232-47a9-9382-c48451d25bf9"
tenant_id = "a8eec281-aaa3-4dae-ac9b-9a398b9215e7"
client_secret = "r.X8Q~.uu60pPmlzjY4NV6iWGOUecF2IWvLYBay1"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dlrobin.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dlrobin.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dlrobin.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dlrobin.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dlrobin.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlrobin.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

