from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import col

from hive_query_uio import *

# Crear una sesión de Spark con soporte para Hive
spark = SparkSession.builder \
    .appName("MINES_UNICOS_METRO_UIO") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.exec.dynamic.partition.mode","nonstrict") \
	.config("spark.sql.hive.metastorePartitionPruningFallbackOnException", "true") \
    .config("spark.sql.hive.metastorePartitionPruningFastFallback", "true") \
    .enableHiveSupport() \
    .getOrCreate()


df=spark.sql(q5min(20251209,'2025-12-10 22:05:00','5 minutes','20 minutes')).show()

df = df \
    .withColumn("fecha", col("fecha").cast(IntegerType())) \
    .withColumn("hora", col("hora").cast(TimestampType())) \
    .withColumn("hora_inicio", col("hora_inicio").cast(TimestampType())) \
    .withColumn("hora_fin", col("hora_fin").cast(TimestampType())) \
    .withColumn("frecuencia", col("frecuencia").cast(IntegerType()))

df.printSchema()
df.write.mode("overwrite").format("parquet").option("compression", "zstd").option("compressionLevel", "5").saveAsTable("db_trafico.tabla_numeros_unicos_uio")
