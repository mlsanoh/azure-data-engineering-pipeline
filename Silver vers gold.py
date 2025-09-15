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

dbutils.fs.ls("abfss://silver@dtlakeexotest.dfs.core.windows.net")

# COMMAND ----------

dbutils.fs.ls("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/")

# COMMAND ----------

df_Address = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Address")
display(df_Address)

# COMMAND ----------

num_rows = df_Address.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_Address.columns)
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_Address = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Address")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_Address.groupBy(df_Address.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_Address.groupBy(df_Address.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_Address.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_Address = df_clean.drop("rowguid")
df_Address.show 
# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/Address/"

df_Address.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")

# COMMAND ----------

display(df_Address)
num_rows = df_Address.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_Address.columns)
print(f"Nombre de colonnes : {num_cols}")


# COMMAND ----------

df_Customer = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Customer")
display(df_Customer)
num_rows = df_Customer.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_Customer.columns)
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_Customer = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Customer")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_Customer.groupBy(df_Customer.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_Customer.groupBy(df_Customer.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_Customer.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_Customer = df_clean.drop("rowguid","passwordHash","passwordSalt")
df_Customer.show 
# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/Customer/"

df_Customer.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")

# COMMAND ----------

display(df_Customer)

# COMMAND ----------

df_Product = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Product")
display(df_Product)
num_rows = df_Product.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_Product.columns)
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_Product = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/Product")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_Product.groupBy(df_Product.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_Product.groupBy(df_Product.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_Product.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_Product = df_clean.drop("thumbNailPhoto", "thumbnailPhotoFileName", "rowguid")
df_Product.show()
# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/Product/"

df_Product.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_Product)

# COMMAND ----------

df_CustomerAddress = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/CustomerAddress")
display(df_CustomerAddress)
num_rows = df_CustomerAddress.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_CustomerAddress.columns)
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_CustomerAddress = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/CustomerAddress")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_CustomerAddress.groupBy(df_CustomerAddress.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_CustomerAddress.groupBy(df_CustomerAddress.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_CustomerAddress.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_CustomerAddress = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/CustomerAddress/"

df_CustomerAddress.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_CustomerAddress)

# COMMAND ----------

df_ProductCategory = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductCategory")
display(df_ProductCategory)
num_rows = df_ProductCategory.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_ProductCategory.columns)
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_ProductCategory = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductCategory")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_ProductCategory.groupBy(df_ProductCategory.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_ProductCategory.groupBy(df_ProductCategory.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_ProductCategory.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_ProductCategory = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/ProductCategory/"

df_ProductCategory.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_ProductCategory)

# COMMAND ----------

df_ProductDescription = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductDescription")
display(df_ProductDescription)
num_rows = df_ProductDescription.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_ProductDescription.columns)    
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_ProductDescription  = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductDescription")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_ProductDescription .groupBy(df_ProductDescription .columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_ProductDescription .groupBy(df_ProductDescription .columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_ProductDescription .dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_ProductDescription  = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/ProductDescription/"

df_ProductDescription.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_ProductDescription)

# COMMAND ----------

df_ProductModel = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductModel")
display(df_ProductModel)
num_rows = df_ProductModel.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_ProductModel.columns)    
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_ProductModel = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductModel")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_ProductModel.groupBy(df_ProductModel.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_ProductModel.groupBy(df_ProductModel.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_ProductModel.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_ProductModel = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/ProductModel/"

df_ProductModel.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_ProductModel)

# COMMAND ----------

df_ProductModelProductDescription = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductModelProductDescription")
display(df_ProductModelProductDescription)
num_rows = df_ProductModelProductDescription.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_ProductModelProductDescription.columns)    
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_ProductModelProductDescription = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/ProductModelProductDescription")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_ProductModelProductDescription.groupBy(df_ProductModelProductDescription.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_ProductModelProductDescription.groupBy(df_ProductModelProductDescription.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_ProductModelProductDescription.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_ProductModelProductDescription = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/ProductModelProductDescription/"

df_ProductModelProductDescription.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_ProductModelProductDescription)

# COMMAND ----------

df_SalesOrderDetail = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/SalesOrderDetail")
display(df_SalesOrderDetail)
num_rows = df_SalesOrderDetail.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_SalesOrderDetail.columns)    
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_SalesOrderDetail = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/SalesOrderDetail")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_SalesOrderDetail.groupBy(df_SalesOrderDetail.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_SalesOrderDetail.groupBy(df_SalesOrderDetail.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_SalesOrderDetail.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_SalesOrderDetail = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/SalesOrderDetail/"

df_SalesOrderDetail.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_SalesOrderDetail)

# COMMAND ----------

df_SalesOrderHeader = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/SalesOrderHeader")
display(df_SalesOrderHeader)
num_rows = df_SalesOrderHeader.count()
print(f"Nombre de lignes : {num_rows}")
num_cols = len(df_SalesOrderHeader.columns)    
print(f"Nombre de colonnes : {num_cols}")

# COMMAND ----------

from pyspark.sql import SparkSession

# Lire la table Delta existante
df_SalesOrderHeader = spark.read.format("delta").load("abfss://silver@dtlakeexotest.dfs.core.windows.net/SalesLT/SalesOrderHeader")

# Compter et afficher le nombre de doublons
duplicate_rows_count = df_SalesOrderHeader.groupBy(df_SalesOrderHeader.columns).count().filter('count > 1').count()
print(f"Nombre de doublons avant suppression : {duplicate_rows_count}")

# Afficher les lignes en doublon
if duplicate_rows_count > 0:
    print("Lignes en doublon :")
    df_SalesOrderHeader.groupBy(df_SalesOrderHeader.columns).count().filter('count > 1').show(truncate=False)

# Supprimer les doublons (garde une seule occurrence)
df_clean = df_SalesOrderHeader.dropDuplicates()

# Vérifier qu’il ne reste plus de doublons
duplicate_rows_count_after = df_clean.groupBy(df_clean.columns).count().filter('count > 1').count()
print(f"Nombre de doublons après suppression : {duplicate_rows_count_after}")

# Supprimer une ou plusieurs colonnes
df_SalesOrderHeader = df_clean.drop("rowguid")

# Enregistrer la table nettoyée dans ADLS Gen2
storage_account = "dtlakeexotest"
container_silver = "gold"
output_path = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net/SalesLT/SalesOrderHeader/"

df_SalesOrderHeader.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(output_path)
print(f"Table nettoyée sauvegardée dans : {output_path}")


# COMMAND ----------

display(df_SalesOrderHeader)