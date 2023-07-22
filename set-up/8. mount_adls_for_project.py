# Databricks notebook source
def mount_adls(storage_account_name, container_name):
    client_id = "e8bcd822-e232-47a9-9382-c48451d25bf9"
    tenant_id = "a8eec281-aaa3-4dae-ac9b-9a398b9215e7"
    client_secret = "r.X8Q~.uu60pPmlzjY4NV6iWGOUecF2IWvLYBay1"

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    display(dbutils.fs.mounts())


# COMMAND ----------

mount_adls("formula1dlrobin", "raw")

# COMMAND ----------

mount_adls("formula1dlrobin", "processed")

# COMMAND ----------

mount_adls("formula1dlrobin", "presentation")

# COMMAND ----------

