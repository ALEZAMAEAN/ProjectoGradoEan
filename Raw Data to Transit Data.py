# Databricks notebook source
#instalar librerias
%pip install geopandas
%pip install pandas 

# COMMAND ----------

# Configuración de almacenamiento
storage_account_name = "2024ean"
flat_container_name = "transit"
raw_container_name = "raw"
storage_account_access_key = "DZF+y8C7VvcMQYbOmo2PEsCoyRiGHseTynESaEHx1+KWKXU5UZlxDxDI/HGQRkj9nOJN4JL5l3gB+AStVNkd/g=="

# Montar el contenedor
dbutils.fs.mount(
    source = f"wasbs://{flat_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{flat_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)
dbutils.fs.mount(
    source = f"wasbs://{raw_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{raw_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

# MAGIC %md
# MAGIC Limpieza de caracteres extraños

# COMMAND ----------

# Definir un diccionario de reemplazos
replacements = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
    "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
    "ñ": "n", "Ñ": "N"
}
replacements2 = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
    "Á": "A", "É": "E", "Í": "I", "Ó": "O", "Ú": "U",
    "ñ": "n", "Ñ": "N",".":"_"
}

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace
# Leer archivos CSV
nombre_archivo="DAILoc_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))


# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="DAISCAT_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="DAIUPZ_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="DIVIPOLA_CentrosPoblados"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep="|")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
from pyspark.sql.functions import lit
nombre_archivo="hurto_a_motocicletas_6"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep="|")

# Agregar columnas con coordenadas
df_csv1 = df_csv1.withColumn("longitude", lit("-74.08175"))
df_csv1 = df_csv1.withColumn("latitude", lit("4.60971"))

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="hurto_a_personas_22"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep="|")
# Agregar columnas con coordenadas
df_csv1 = df_csv1.withColumn("longitude", lit("-74.08175"))
df_csv1 = df_csv1.withColumn("latitude", lit("4.60971"))

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="hurto_automotores_14"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep="|")

# Agregar columnas con coordenadas
df_csv1 = df_csv1.withColumn("longitude", lit("-74.08175"))
df_csv1 = df_csv1.withColumn("latitude", lit("4.60971"))

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="IRLoc_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="IRCAT_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

# Leer archivos CSV
nombre_archivo="IRUPZ_processed"
df_csv1 = spark.read.csv(f"/mnt/raw/{nombre_archivo}.csv", header=True, inferSchema=True, sep=",")

# Renombrar las columnas aplicando los reemplazos
for col_name in df_csv1.columns:
    new_col_name = col_name
    for key, value in replacements2.items():
        new_col_name = new_col_name.replace(key, value)
    df_csv1 = df_csv1.withColumnRenamed(col_name, new_col_name)
    
# Aplicar los reemplazos utilizando regex y regexp_replace para todas las columnas
for col_name in df_csv1.columns:
    for key, value in replacements.items():
        df_csv1 = df_csv1.withColumn(col_name, regexp_replace(col(col_name), key, value))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/transit/{nombre_archivo}.csv"
df_csv1.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

dbutils.fs.unmount(f"/mnt/transit")
dbutils.fs.unmount(f"/mnt/raw")