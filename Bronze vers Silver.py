# Databricks notebook source
# Récupération des secrets depuis ton Key Vault
client_id = dbutils.secrets.get(scope="keyvault-exo-test", key="client-ID")
tenant_id = dbutils.secrets.get(scope="keyvault-exo-test", key="tenant-ID")
client_secret = dbutils.secrets.get(scope="keyvault-exo-test", key="secret-client")

# Nom de ton storage account ADLS Gen2
storage_account = "dtlakeexotest"

# Configuration OAuth pour accéder au stockage
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

dbutils.fs.ls("abfss://bronze@dtlakeexotest.dfs.core.windows.net")

# COMMAND ----------

dbutils.fs.ls("abfss://bronze@dtlakeexotest.dfs.core.windows.net/SalesLT/")


# COMMAND ----------

df_Address = spark.read.parquet("abfss://bronze@dtlakeexotest.dfs.core.windows.net/SalesLT/Address")
display(df_Address)

# COMMAND ----------

df_Address.dtypes


# COMMAND ----------

# Permet de trouver le nom des listes de table qui nous aidera dans la modification de l'ensemble des tables
table_name = []
path = f"abfss://bronze@dtlakeexotest.dfs.core.windows.net/SalesLT/"
for i in dbutils.fs.ls(path):
	table_name.append(i.name.split('/')[0])

table_name 


# COMMAND ----------

# Modifier la date sur l'ensemble des tables

from pyspark.sql.functions import from_utc_timestamp, date_format, col
from pyspark.sql.types import TimestampType

for i in table_name:
    # Chemin direct vers ADLS Gen2
    input_path = f"abfss://bronze@dtlakeexotest.dfs.core.windows.net/SalesLT/{i}/{i}.parquet"
    
    # Lire le fichier Parquet
    df = spark.read.parquet(input_path)
    
    # Convertir toutes les colonnes qui contiennent 'Date' ou 'date' en format yyyy-MM-dd
    for c in df.columns:
        if "Date" in c or "date" in c:
            df = df.withColumn(c, date_format(from_utc_timestamp(col(c).cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
    
    # Chemin de sortie Delta
    output_path = f"abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/{i}/"
    
    # Sauvegarder en Delta
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)

# COMMAND ----------

display(df)

# COMMAND ----------

df_Product = spark.read.format('delta').load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Product/")
display(df_Product)

# COMMAND ----------

table_name_silver = []
path = f"abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/"
for i in dbutils.fs.ls(path):
	table_name_silver.append(i.name.split('/')[0])

table_name_silver 


# COMMAND ----------

df_Custumer= spark.read.format('delta').load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Customer/")
display(df_Custumer)