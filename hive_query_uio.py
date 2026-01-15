def q5min(fecha_proceso,fecha_timestamp,calcular_minuto_muio,calcular_minuto_muio_evento):
    query='''SELECT
            DATE_FORMAT(fecha_hora_evento,"yyyyMMdd") AS fecha
            ,muio.estacion
            ,muio.latitud
            ,muio.longitud
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT("{fecha_timestamp}","HH"),":00:00") AS hora
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT((CAST("{fecha_timestamp}" AS TIMESTAMP) - INTERVAL {calcular_minuto_muio}),"HH:mm"),":00") AS hora_inicio
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT("{fecha_timestamp}","HH:mm"),":00") AS hora_fin
            ,COUNT(DISTINCT cbn.numero_telefono) frecuencia
            FROM db_trafico.cdrs_datos cbn
            LEFT JOIN db_trafico.celdas_x_estacion_metrouio muio
            ON cbn.celda = muio.celda
            WHERE 
            fecha_proceso >= CAST(DATE_FORMAT((CAST("{fecha_timestamp}" AS TIMESTAMP) - INTERVAL {calcular_minuto_muio}),"yyyyMMdd") AS BIGINT)
            AND fecha_proceso <= CAST(DATE_FORMAT("{fecha_timestamp}",'yyyyMMdd') AS BIGINT)
            AND CAST(CONCAT(
                            CASE WHEN LENGTH(CAST(hora AS STRING))=1 THEN CONCAT(0,hora) ELSE hora END,
                            CASE WHEN LENGTH(CAST(minuto AS STRING))=1 THEN CONCAT(0,minuto) ELSE minuto END) AS BIGINT) >= CAST(DATE_FORMAT((CAST("{fecha_timestamp}" AS TIMESTAMP) - INTERVAL {calcular_minuto_muio_evento}),"HHmm") AS BIGINT)
            
            AND CAST(CONCAT(
                            CASE WHEN LENGTH(CAST(hora AS STRING))=1 THEN CONCAT(0,hora) ELSE hora END,
                            CASE WHEN LENGTH(CAST(minuto AS STRING))=1 THEN CONCAT(0,minuto) ELSE minuto END) AS BIGINT) <= CAST(DATE_FORMAT("{fecha_timestamp}","HHmm") AS BIGINT)

            AND LENGTH(cast(cbn.numero_telefono as string))=9
            group by DATE_FORMAT(fecha_hora_evento,"yyyyMMdd")
            ,muio.estacion
            ,muio.latitud
            ,muio.longitud
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT((CAST("{fecha_timestamp}" AS TIMESTAMP) - INTERVAL {calcular_minuto_muio}),"HH:mm"),":00")
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT("{fecha_timestamp}","HH:mm"),":00")
            ,CONCAT(DATE_FORMAT(fecha_hora_evento,"yyyy-MM-dd")," ", DATE_FORMAT("{fecha_timestamp}","HH"),":00:00")
            '''.format(fecha_proceso=fecha_proceso,fecha_timestamp=fecha_timestamp,calcular_minuto_muio=calcular_minuto_muio,calcular_minuto_muio_evento=calcular_minuto_muio_evento)

    return query

