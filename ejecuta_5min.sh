#!/bin/bash
# Establece ruta completa del archivo excel
fechahora=$(date +%Y%m%d%H%M%S)
ruta_log=/home/nifi/shell/log/spark_5min-$fechahora.log
# Exporta la variable de python en la version python3.6
export PYSPARK_PYTHON=python3.6
# Se crea la variable de la ruta de spark
VAL_RUTA_SPARK=/home/nifi/python/pyspark/spark-3.4.0-bin-hadoop3

# Se carga el bashrc
. ~/.bashrc

# Recibe parametros
fecha_proceso=$1
fecha_timestamp=$2
calcular_minuto_muio=$3
calcular_minuto_muio_evento=$4

# Se ejecuta el archivo convertir_xlsx_df_to_hive.py con spark-submit
$VAL_RUTA_SPARK/bin/spark-submit   \
        --master yarn --name METRO_UIO_5MIN_HIVE /home/nifi/python/proceso_5min_uio_metro.py > $ruta_log 2>&1
