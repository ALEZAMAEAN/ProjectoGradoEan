# Databricks notebook source
#instalar librerias
%pip install geopandas
%pip install pandas 

# COMMAND ----------

# MAGIC %md
# MAGIC Configuracion de blob storage
# MAGIC

# COMMAND ----------


# Configuración de almacenamiento
storage_account_name = "2024ean"
flat_container_name = "flatfile"
raw_container_name = "raw"
storage_account_access_key = "DZF+y8C7VvcMQYbOmo2PEsCoyRiGHseTynESaEHx1+KWKXU5UZlxDxDI/HGQRkj9nOJN4JL5l3gB+AStVNkd/g=="

# Montar el contenedor
dbutils.fs.mount(
    source = f"wasbs://{flat_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{flat_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)





# COMMAND ----------

dbutils.fs.mount(
    source = f"wasbs://{raw_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{raw_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

# MAGIC %md
# MAGIC Carga de documentos GeoJson

# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="DAILoc"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)


# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="DAISCAT"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)


# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="DAIUPZ"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="IRLoc"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="IRSCAT"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

#Ruta al archivo GeoJSON en tu Blob Storage montado
nombre_archivo="IRUPZ"
ruta_geojson = f"/mnt/flatfile/{nombre_archivo}.geojson"

# Lee el archivo GeoJSON utilizando spark-geojson
json_df = spark.read.option("multiline","true").json(ruta_geojson)

#Exporta libreria functions
from pyspark.sql import functions as F

#Desagrega el feature lo saca de la lista y lo vuelve en tabular
features_exploded_df = json_df.select(F.explode(F.col("features")).alias("feature"))

#Desagrega todas las columnas de propierties y convierte las coordenadas en un solo string
json_explode_dfp = features_exploded_df.select(
    "feature.properties.*",
    F.to_json(F.col("feature.geometry.coordinates")).alias("coordinates")
)

# Consolidar particiones
json_explode_dfp = json_explode_dfp.coalesce(1)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/raw/{nombre_archivo}.csv"
json_explode_dfp.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Carga documentos CSV

# COMMAND ----------

# Leer archivos CSV
df_csv = spark.read.csv("/mnt/flatfile/hurto_a_motocicletas_6.csv", header=True, inferSchema=True, sep=";")
# Guardar el DataFrame resultante en un archivo CSV
output_path = "/mnt/raw/hurto_a_motocicletas_6.csv"
df_csv.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
df_csv = spark.read.csv("/mnt/flatfile/DIVIPOLA_CentrosPoblados.csv", header=True, inferSchema=True, sep=";")
# Guardar el DataFrame resultante en un archivo CSV
output_path = "/mnt/raw/DIVIPOLA_CentrosPoblados.csv"
df_csv.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
df_csv = spark.read.csv("/mnt/flatfile/hurto_a_personas_22.csv", header=True, inferSchema=True, sep=";")
# Guardar el DataFrame resultante en un archivo CSV
output_path = "/mnt/raw/hurto_a_personas_22.csv"
df_csv.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
df_csv = spark.read.csv("/mnt/flatfile/hurto_automotores_14.csv", header=True, inferSchema=True, sep=";")
# Guardar el DataFrame resultante en un archivo CSV
output_path = "/mnt/raw/hurto_automotores_14.csv"
df_csv.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC Cerrar montaje de contenedor

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/flatfile")
dbutils.fs.unmount(f"/mnt/raw")