# Databricks notebook source


dbutils.fs.unmount(f"/mnt/curated")


# COMMAND ----------

#Configuración de almacenamiento
storage_account_name = "2024ean"
curated_container_name = "curated"
raw_container_name = "transit"
storage_account_access_key = "DZF+y8C7VvcMQYbOmo2PEsCoyRiGHseTynESaEHx1+KWKXU5UZlxDxDI/HGQRkj9nOJN4JL5l3gB+AStVNkd/g=="

# Montar el contenedor
dbutils.fs.mount(
    source = f"wasbs://{curated_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{curated_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)
dbutils.fs.mount(
    source = f"wasbs://{raw_container_name}@{storage_account_name}.blob.core.windows.net/",
    mount_point = f"/mnt/{raw_container_name}",
    extra_configs = {f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net": storage_account_access_key}
)

# COMMAND ----------

from pyspark.sql.functions import lit
from pyspark.sql.functions import lit, monotonically_increasing_id, when, col
#Creacion tabla Dim_Armas

#traer datos de armas 
nombre_archivo1="hurto_a_motocicletas_6"
df_csv1 = spark.read.csv(f"/mnt/transit/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo2="hurto_a_personas_22"
df_csv2 = spark.read.csv(f"/mnt/transit/{nombre_archivo2}.csv", header=True, inferSchema=True, sep="|")
df_csv2 = df_csv2.withColumnRenamed("ARMA MEDIO", "ARMAS_MEDIOS")
nombre_archivo3="hurto_automotores_14"
df_csv3 = spark.read.csv(f"/mnt/transit/{nombre_archivo3}.csv", header=True, inferSchema=True, sep="|")

# Agregar registros a la columna existente
#dim_armas = df_csv1.withColumn("ARMAS_MEDIOS", df_csv1.ARMAS_MEDIOS + df_csv2.ARMAS_MEDIOS+df_csv3.ARMAS_MEDIOS+ lit("sin registro"))
df_csv1_a =df_csv1.select("ARMAS_MEDIOS")
df_csv2_a =df_csv2.select("ARMAS_MEDIOS")
df_csv3_a =df_csv3.select("ARMAS_MEDIOS")
# Define the column names y el dato dummy
columns = ["ARMAS_MEDIOS"]
newRow = spark.createDataFrame([("SIN REGISTRO", "SIN REGISTRO")], columns)
newRow_a =newRow.select("ARMAS_MEDIOS")

#une los registros
dim_armas = df_csv1_a.union(df_csv2_a).union(df_csv3_a).union(newRow_a)

#borra duplicados
dim_armas =  dim_armas.dropDuplicates()
dim_armas = dim_armas.fillna("NO REPORTADO")

#Cambiar nombre
dim_armas = dim_armas.withColumnRenamed("ARMAS_MEDIOS", "Nom_arma")

# Agregar una columna de índice con valores incrementales
dim_armas = dim_armas.withColumn("Id_arma", monotonically_increasing_id())

# Asignar -1 a un registro específico, por ejemplo, el que tiene "ARMAS_MEDIOS" = "sin registro"
dim_armas = dim_armas.withColumn(
    "Id_arma",
    when(col("Nom_arma") == "SIN REGISTRO", -1).otherwise(col("Id_arma"))
)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/curated/Dim_arma.csv"
dim_armas.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

#Creacion tabla tipo de delito
from pyspark.sql.functions import lit, upper

# Crear el DataFrame
data = [
    (1, 'Delitos de alto impacto', 'Delitos que causan un gran impacto en la sociedad, incluyendo violencia y crímenes graves'),
    (2, 'Incidentes reportados', 'Incidentes que han sido reportados a las autoridades, independientemente de su gravedad'),
    (3, 'Hurto a motocicletas', 'Robo de motocicletas'),
    (4, 'Hurto a personas', 'Robo de pertenencias personales'),
    (5, 'Hurto a automotores', 'Robo de automóviles y otros vehículos motorizados'),
    (-1, "Sin registro", "Sin registro")
]

columns = ["Id_tipo_Delito", "Nom_delito", "Desc_delito"]

Dim_tipo_delito = spark.createDataFrame(data, columns)

#pasar a mayusculas
Dim_tipo_delito = Dim_tipo_delito.withColumn("Nom_delito", upper(Dim_tipo_delito["Nom_delito"]))
Dim_tipo_delito = Dim_tipo_delito.withColumn("Desc_delito", upper(Dim_tipo_delito["Desc_delito"]))


# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/curated/Dim_tipo_delito.csv"
dim_armas.write.csv(output_path, sep="|", header=True)

# COMMAND ----------

#Creacion tabla fechas
import datetime
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, monotonically_increasing_id, year, month, dayofmonth

# Crear un rango de fechas desde 2016-01-01 hasta 2030-12-31
start_date = datetime.date(2016, 1, 1)
end_date = datetime.date(2030, 12, 31)

# Generar una lista de fechas en el rango especificado
date_list = [start_date + datetime.timedelta(days=x) for x in range((end_date - start_date).days + 1)]

# Crear el DataFrame con la columna de fechas
df_dates = spark.createDataFrame(date_list, DateType()).toDF("Fecha")

# Extraer Año, Mes y Día de la columna de fechas
df_dates = df_dates.withColumn("Año", year(col("Fecha")))
df_dates = df_dates.withColumn("Mes", month(col("Fecha")))
df_dates = df_dates.withColumn("Día", dayofmonth(col("Fecha")))

df_dates = df_dates.withColumnRenamed("Fecha", "Id_fecha")

# Seleccionar y reorganizar las columnas según la especificación
df_dates = df_dates.select("Id_fecha", "Año", "Mes", "Día")

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/curated/Dim_Tiempo.csv"
df_dates.write.csv(output_path, sep="|", header=True)


# COMMAND ----------

# creación tabla ubicacion
from pyspark.sql.functions import lit, monotonically_increasing_id, when, col
#traer datos de ubicacion 
nombre_archivo1="DAILoc_processed"
df_csv1 = spark.read.csv(f"/mnt/transit/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
df_csv1 = df_csv1.withColumnRenamed("CMNOMLOCAL", "DESC_UBICACION")
nombre_archivo2="DAISCAT_processed"
df_csv2 = spark.read.csv(f"/mnt/transit/{nombre_archivo2}.csv", header=True, inferSchema=True, sep="|")
df_csv2 = df_csv2.withColumnRenamed("CMNOMSCAT", "DESC_UBICACION")
nombre_archivo3="DAIUPZ_processed"
df_csv3 = spark.read.csv(f"/mnt/transit/{nombre_archivo3}.csv", header=True, inferSchema=True, sep="|")
df_csv3 = df_csv3.withColumnRenamed("CMNOMUPLA", "DESC_UBICACION")
nombre_archivo4="IRCAT_processed"
df_csv4 = spark.read.csv(f"/mnt/transit/{nombre_archivo4}.csv", header=True, inferSchema=True, sep="|")
df_csv4 = df_csv4.withColumnRenamed("CMNOMSCAT", "DESC_UBICACION")
nombre_archivo5="IRLoc_processed"
df_csv5 = spark.read.csv(f"/mnt/transit/{nombre_archivo5}.csv", header=True, inferSchema=True, sep="|")
df_csv5 = df_csv5.withColumnRenamed("CMNOMLOCAL", "DESC_UBICACION")
nombre_archivo6="IRUPZ_processed"
df_csv6 = spark.read.csv(f"/mnt/transit/{nombre_archivo6}.csv", header=True, inferSchema=True, sep="|")
df_csv6 = df_csv6.withColumnRenamed("CMNOMUPLA", "DESC_UBICACION")

#AGREGAR COLUMNA TIPO_UBICACION
df_csv1 =df_csv1.withColumn("Tipo_ubicacion",lit("LOCALIDAD"))
df_csv2 =df_csv2.withColumn("Tipo_ubicacion",lit("CATASTRAL"))
df_csv3 =df_csv3.withColumn("Tipo_ubicacion",lit("UPZ"))
df_csv4 =df_csv4.withColumn("Tipo_ubicacion",lit("CATASTRAL"))
df_csv5 =df_csv5.withColumn("Tipo_ubicacion",lit("LOCALIDAD"))
df_csv6 =df_csv6.withColumn("Tipo_ubicacion",lit("UPZ"))

# Agregar registros a la columna existente

df_csv1_a =df_csv1.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
df_csv2_a =df_csv2.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
df_csv3_a =df_csv3.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
df_csv4_a =df_csv4.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
df_csv5_a =df_csv5.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
df_csv6_a =df_csv6.select("Tipo_ubicacion","DESC_UBICACION","longitude","latitude")
# Define the column names y el dato dummy
columns = ["Tipo_ubicacion","DESC_UBICACION","longitude","latitude"]
newRow = spark.createDataFrame([("SIN REGISTRO","SIN REGISTRO", "-1","-1"),("CIUDAD","BOGOTA", "-74.08175","4.60971")], columns)

#une los registros
dim_ubicacion = df_csv1_a.union(df_csv2_a).union(df_csv3_a).union(newRow).union(df_csv4_a).union(df_csv5_a).union(df_csv6_a)

#borra duplicados
dim_ubicacion =  dim_ubicacion.dropDuplicates()

#Cambiar nombre
dim_ubicacion = dim_ubicacion.withColumnRenamed("DESC_UBICACION", "Desc_ubicacion")
dim_ubicacion = dim_ubicacion.withColumnRenamed("longitude", "Longitud")
dim_ubicacion = dim_ubicacion.withColumnRenamed("latitude", "Latitud")

# Agregar una columna de índice con valores incrementales
dim_ubicacion = dim_ubicacion.withColumn("Id_ubicacion", monotonically_increasing_id())

# Asignar -1 a un registro específico, por ejemplo, el que tiene "ARMAS_MEDIOS" = "sin registro"
dim_ubicacion = dim_ubicacion.withColumn(
   "Id_ubicacion",
    when(col("Desc_ubicacion") == "SIN REGISTRO", -1).otherwise(col("Id_ubicacion"))
)

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/curated/Dim_ubicacion.csv"
#dim_ubicacion.write.csv(output_path, sep="|", header=True)



# COMMAND ----------

#creacion tabla fact
from pyspark.sql.functions import col
#Lectura de dimensiones

nombre_archivo1="Dim_arma"
dim_arma = spark.read.csv(f"/mnt/curated/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo1="Dim_tipo_delito"
Dim_tipo_delito = spark.read.csv(f"/mnt/curated/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo1="Dim_ubicacion"
Dim_ubicacion = spark.read.csv(f"/mnt/curated/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")

#Lectura de transit
#hurtos
nombre_archivo1="hurto_a_motocicletas_6"
motos = spark.read.csv(f"/mnt/transit/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo2="hurto_a_personas_22"
personas = spark.read.csv(f"/mnt/transit/{nombre_archivo2}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo3="hurto_automotores_14"
carros = spark.read.csv(f"/mnt/transit/{nombre_archivo3}.csv", header=True, inferSchema=True, sep="|")

# Unir los dataframes motos
df_motos = motos.join(dim_arma, col("ARMAS_MEDIOS") == col("Nom_arma"), "left")
df_motos =df_motos.withColumn("Id_tipo_delito",lit(3))
df_motos = df_motos.filter(col("MUNICIPIO") == "Bogot� D.C. (CT)")
df_motos =df_motos.withColumn("Id_ubicacion",lit(131647))
df_motos = df_motos.withColumnRenamed("FECHA", "Id_fecha")
df_motos = df_motos.withColumnRenamed("CANTIDAD", "Num_cantidad")
df_motos = df_motos.select("Id_fecha","Id_ubicacion","Id_tipo_delito","Id_arma","Num_cantidad")

# Unir los dataframes personas
df_personas = personas.join(dim_arma, col("ARMA MEDIO") == col("Nom_arma"), "left")
df_personas =df_personas.withColumn("Id_tipo_delito",lit(4))
df_personas = df_personas.filter(col("MUNICIPIO") == "Bogot� D.C. (CT)")
df_personas =df_personas.withColumn("Id_ubicacion",lit(131647))
df_personas = df_personas.withColumnRenamed("FECHA HECHO", "Id_fecha")
df_personas = df_personas.withColumnRenamed("CANTIDAD", "Num_cantidad")
df_personas = df_personas.select("Id_fecha","Id_ubicacion","Id_tipo_delito","Id_arma","Num_cantidad")

# Unir los dataframes automotores
df_carros= carros.join(dim_arma, col("ARMAS_MEDIOS") == col("Nom_arma"), "left")
df_carros =df_carros.withColumn("Id_tipo_delito",lit(5))
df_carros = df_carros.filter(col("MUNICIPIO") == "Bogot� D.C. (CT)")
df_carros =df_carros.withColumn("Id_ubicacion",lit(131647))
df_carros = df_carros.withColumnRenamed("FECHA", "Id_fecha")
df_carros = df_carros.withColumnRenamed("CANTIDAD", "Num_cantidad")
df_carros = df_carros.select("Id_fecha","Id_ubicacion","Id_tipo_delito","Id_arma","Num_cantidad")




# COMMAND ----------


import pandas as pd
# Importar la función expr para evaluar expresiones SQL en columnas
from pyspark.sql.functions import expr
from pyspark.sql.functions import lit, monotonically_increasing_id, when, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import to_date

Dim_ubicacion = spark.read.csv(f"/mnt/curated/Dim_ubicacion.csv", header=True, inferSchema=True, sep="|")
#Delito de alto impacto
nombre_archivo1="DAILoc_processed"
data = spark.read.csv(f"/mnt/transit/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo2="DAISCAT_processed"
data1 = spark.read.csv(f"/mnt/transit/{nombre_archivo2}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo3="DAIUPZ_processed"
data2 = spark.read.csv(f"/mnt/transit/{nombre_archivo3}.csv", header=True, inferSchema=True, sep="|")

nombre_archivo3="IRLoc_processed"
data3 = spark.read.csv(f"/mnt/transit/{nombre_archivo1}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo4="IRCAT_processed"
data4 = spark.read.csv(f"/mnt/transit/{nombre_archivo2}.csv", header=True, inferSchema=True, sep="|")
nombre_archivo5="IRUPZ_processed"
data5 = spark.read.csv(f"/mnt/transit/{nombre_archivo3}.csv", header=True, inferSchema=True, sep="|")

def procesar_dataframe(data, nombre_cmnolocal, num):
    # Definir el esquema
    schema = StructType([
        StructField("fecha", StringType(), True),
        StructField("cantidad", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField(nombre_cmnolocal, StringType(), True)
    ])

    indicadores = ['20', '21', '22', '23']
    fechas = ["2020-01-01", "2021-01-01", "2022-01-01", "2023-01-01"]

    # Inicializar un DataFrame vacío para almacenar los resultados finales
    df_resultante = spark.createDataFrame([], schema)

    for indicador, fecha in zip(indicadores, fechas):
        # Filtrar las columnas que contienen el indicador
        columnas = [col for col in data.columns if indicador in col]
        
        if columnas:
            # Construir una expresión para sumar todas las columnas que contienen el indicador
            suma_total_expr = " + ".join(columnas)
            
            # Agregar una nueva columna 'cantidad' con la suma de las columnas seleccionadas
            df_con_cantidad = data.withColumn("cantidad", expr(suma_total_expr))
            
            # Agregar una columna 'fecha' con el valor correspondiente
            df_con_cantidad = df_con_cantidad.withColumn("fecha", lit(fecha))
            
            # Seleccionar las columnas deseadas
            df_con_cantidad = df_con_cantidad.select("fecha", "cantidad", "longitude", "latitude", nombre_cmnolocal)
            
            # Combinar con el DataFrame resultante
            df_resultante = df_resultante.union(df_con_cantidad)

    df_resultante = df_resultante.join(Dim_ubicacion, (col(nombre_cmnolocal) == col("Desc_ubicacion")) , "left")
    df_resultante =df_resultante.withColumn("Id_arma",lit(-1))            
    df_resultante =df_resultante.withColumn("Id_tipo_delito",lit(num))
    df_resultante = df_resultante.withColumnRenamed("fecha", "Id_fecha")
    df_resultante = df_resultante.withColumnRenamed("cantidad", "Num_cantidad")
    df_resultante = df_resultante.select("Id_fecha","Id_ubicacion","Id_tipo_delito","Id_arma","Num_cantidad")

    return df_resultante


# Llamar a la función con el DataFrame y el nombre del campo "CMNOMLOCAL"
nombre_cmnolocal = "CMNOMLOCAL"
df_resultante = procesar_dataframe(data, nombre_cmnolocal, 1)
nombre_cmnolocal1 = "CMNOMSCAT"
df_resultante1 = procesar_dataframe(data1, nombre_cmnolocal1, 1)
nombre_cmnolocal2 = "CMNOMUPLA"
df_resultante2 = procesar_dataframe(data2, nombre_cmnolocal2, 1)

nombre_cmnolocal = "CMNOMLOCAL"
df_resultante3 = procesar_dataframe(data3, nombre_cmnolocal, 2)
nombre_cmnolocal1 = "CMNOMSCAT"
df_resultante4 = procesar_dataframe(data4, nombre_cmnolocal1, 2)
nombre_cmnolocal2 = "CMNOMLOCAL"
df_resultante5 = procesar_dataframe(data5, nombre_cmnolocal2, 2)


fact_delitos = df_resultante.union(df_resultante1).union(df_resultante2).union(df_resultante3).union(df_resultante4).union(df_resultante5).union(df_carros).union(df_personas).union(df_motos)
fact_delitos = fact_delitos.withColumn("Id_fecha", to_date("Id_fecha", "yyyy-MM-dd"))

# Guardar el DataFrame resultante en un archivo CSV
output_path = f"/mnt/curated/Fact_delitos.csv"
fact_delitos.write.csv(output_path, sep="|", header=True)

