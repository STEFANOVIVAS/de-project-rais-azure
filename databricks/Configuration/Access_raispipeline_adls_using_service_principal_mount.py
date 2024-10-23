# Databricks notebook source
def mount_adls(storage_account,container):
    # get secrets from key vault
    
    client_id=dbutils.secrets.get(scope='rais-scope',key='rais-app-client-id')
    tenant_id=dbutils.secrets.get(scope='rais-scope',key='rais-app-tenant-id')
    client_secret=dbutils.secrets.get(scope='rais-scope',key='rais-app-secret-id')
    # Set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

    if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account}/{container}") 
    # Mount the Storage
    dbutils.fs.mount(
    source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account}/{container}",
    extra_configs = configs)
    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('raispipeline','rais')

# COMMAND ----------


