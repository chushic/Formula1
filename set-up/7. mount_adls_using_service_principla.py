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

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1dlrobin.dfs.core.windows.net/",
  mount_point = "/mnt/formula1dlrobin/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dlrobin.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dlrobin.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

