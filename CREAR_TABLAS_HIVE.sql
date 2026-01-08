DROP TABLE db_trafico.cdrs_datos PURGE;
 CREATE EXTERNAL TABLE db_trafico.cdrs_datos( 
   numero_telefono varchar(20) COMMENT 'numero de telefono de quien realiza tráfico cursado de voz o datos',  
   celda varchar(10) COMMENT 'numero de la celda en decimal donde trafica el usuario',  
   fecha_hora_evento string COMMENT 'fecha hora evento de finalizacion de llamada o cierre del cdr',  
   mb_datos_subida double COMMENT 'cantidad sumarizada de datos de subida en MB',  
   mb_datos_bajada double COMMENT 'cantidad sumarizada de datos de bajada en MB',  
   mb_datos_total double COMMENT 'cantidad total de subida + bajada de datos en MB',  
   fecha_hora_carga string COMMENT 'fecha hora de carga en tiempo real del registro a la tabla',  
   seccdr string COMMENT 'nombre del archivo del registro',  
   imsi int COMMENT 'Código de identificación asociado a una tarjeta SIM',  
   imei int COMMENT 'Código que identifica al terminal',
   tecnologia string COMMENT 'Tecnologia de navegacion: 2G, 3G, 4G, 5G') 
 COMMENT 'Tiene información detalla del tráfico datos de un cliente que trafica por alguna celda del catálogo parametrizado, pero dejando sólo un día de información con respecto a las horas de ejecución.' 
 PARTITIONED BY (                                   
   fecha_proceso bigint COMMENT 'numero de telefono de quien realiza tráfico cursado datos',  
   hora bigint COMMENT 'numero de la celda en decimal donde trafica el usuario',  
   minuto bigint COMMENT 'fecha hora evento de finalizacion de llamada o cierre del cdr') 
STORED AS AVRO
 TBLPROPERTIES (
 'external.table.purge'='true',
 'avro.output.codec'='snappy',
  'avro.schema.literal'='{
    "type": "record",
    "name": "rn_cdrs",
    "fields": [
  { "name" : "numero_telefono","type" : ["long","null"], "default":0},
  { "name" : "celda","type" : ["long","null"], "default":0},
  { "name" : "fecha_hora_evento","type" :  ["string","null"], "default":""},
  { "name" : "mb_datos_subida","type" :  ["long","null"], "default":0},
  { "name" : "mb_datos_bajada","type" :  ["long","null"], "default":0},
  { "name" : "mb_datos_totales","type" :  ["long","null"], "default":0},
  { "name" : "fecha_hora_carga","type" :  ["string","null"], "default":""},
  { "name" : "seccdr","type" :  ["string","null"], "default":""},
  { "name" : "imsi","type" :  ["long","null"], "default":0},
  { "name" : "imei","type" :  ["long","null"], "default":0},
  { "name" : "tecnologia","type" :  ["string","null"], "default":""}
    ]
  }');

  DROP TABLE db_trafico.tabla_numeros_unicos_uio PURGE;
  CREATE TABLE db_trafico.tabla_numeros_unicos_uio( 
   hora timestamp,                                
   hora_inicio timestamp,                         
   hora_fin timestamp,                            
   frecuencia int)                                
 PARTITIONED BY (                                   
   fecha int)                                     
 ROW FORMAT SERDE                                   
   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'  
 WITH SERDEPROPERTIES (                             
   'compression'='zstd',                            
   'compressionLevel'='5',                          
   'path'='hdfs://master:9000/user/hive/warehouse/db_trafico.db/tabla_numeros_unicos_uio')  
 STORED AS INPUTFORMAT                              
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'  
 OUTPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
 LOCATION                                           
   'hdfs://master:9000/user/hive/warehouse/db_trafico.db/tabla_numeros_unicos_uio' 
 TBLPROPERTIES (                                    
   'spark.sql.create.version'='3.4.0',              
   'spark.sql.partitionProvider'='catalog',         
   'spark.sql.sources.provider'='parquet',          
   'spark.sql.sources.schema'='{"type":"struct","fields":[{"name":"hora","type":"timestamp","nullable":true,"metadata":{}},{"name":"hora_inicio","type":"timestamp","nullable":true,"metadata":{}},{"name":"hora_fin","type":"timestamp","nullable":true,"metadata":{}},{"name":"frecuencia","type":"integer","nullable":true,"metadata":{}},{"name":"fecha","type":"integer","nullable":true,"metadata":{}}]}',  
   'spark.sql.sources.schema.numPartCols'='1',      
   'spark.sql.sources.schema.partCol.0'='fecha',    
   'transient_lastDdlTime'='1766014622')            