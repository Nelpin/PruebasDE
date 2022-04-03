from ctypes import cast
from unicodedata import decimal
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Test_spark").master("local[*]").getOrCreate()
spark

spark.sparkContext.setLogLevel("ERROR")

# Carga del archivo de tiendas
store = spark.read.csv("Assets/store_info.csv", sep=";", header=True,inferSchema=True)
store.printSchema()
# Carga del archivo de características
features = spark.read.csv("Assets/features.csv", sep="|", header=True,inferSchema=True)
features.printSchema()
# Carga del archivo de características
sales = spark.read.csv("Assets/historic_sales.csv", sep="|", header=True,inferSchema=True)
sales.printSchema()


sales=sales.dropna() # Eliminar los na
sales = sales.withColumn('Weekly_Sales', regexp_replace('Weekly_Sales', ',', '.').cast('decimal(12,2)')) # Convertir a decimal la columna
sales=sales.withColumn('year', col('Date').substr(1, 4).cast('int')).withColumn('month',col('Date').substr(6, 2)) # Extraer año y mes de la fecha
sales=sales.withColumn("Period",concat_ws("",col("year"),col("month")).cast('int')) # Crear la columna Periodo

# Cual es la tienda con el mayor valor en ventas totales?
maxValue=sales.groupBy("Store") \
    .agg(sum("Weekly_Sales").alias("ventas_totales")) \
    .sort(desc("ventas_totales")) # Cálculo de las ventas acumuladas ordenadas de mayor a menor por el valor de la venta

tiendaMaxVentas = maxValue.first()[0]
print("----------------------------------------------------------------------------------------")
print("1. La tienda con más ventas totales es la tienda: ",tiendaMaxVentas)

# Entre las 3 tiendas más grandes cuál es la que más ventas totales registra?
store = store.sort(desc("Size")) #Ordeno las tiendas descendentemente por tamaño
df_join = store.limit(3).join(maxValue,"Store").sort(desc("ventas_totales")) # Extraer las 3 tiendas más grandes y se unen con las ventas acumuladas

tiendaMaxSize = df_join.first()[0]
print("2. Entre las 3 tiendas más grandes la que vendió mas es la tienda: ",tiendaMaxSize)

# Cual es la tienda con menor ventas ?
df_join_min_ventas = store.join(maxValue,"Store").sort("ventas_totales").take(1) # Se obtienen las ventas por tienda ordenadas de menor a maór por el total de ventas
print("3. La tienda con menores ventas es la tienda: ",df_join_min_ventas[0][0])

# Cual es la tienda que mas vendió en el 2 semestre del año 2012?
semestre=sales.createOrReplaceTempView('semestres') # Se crea la vista semestres
sem=spark.sql("SELECT * FROM semestres where Period>=201206 and Period<=201331") # Se obtienen los datos del segudo periodo 2012
maxValueSem=sem.groupBy("Store") \
    .agg(sum("Weekly_Sales").alias("ventas_totales")) \
    .sort(desc("ventas_totales")).limit(1) # cálculo de las ventas del segundo semestre 2012 

tiendaMaxSem2 = maxValueSem.first()[0]
print("4. La tienda con mayores ventas en el segundo semestre de 2012 es la tienda: ",tiendaMaxSem2)
print("----------------------------------------------------------------------------------------")




