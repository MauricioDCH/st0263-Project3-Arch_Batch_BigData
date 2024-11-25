#!/bin/bash

aws athena start-query-execution \
    --query-string "CREATE EXTERNAL TABLE IF NOT EXISTS covid_data_ ( \
        FechaNotificacion STRING, \
        Departamento STRING, \
        Ciudad STRING, \
        Edad INT, \
        Sexo STRING, \
        TipoContagio STRING, \
        UbicacionPaciente STRING, \
        EstadoPaciente STRING, \
        PaisProcedencia STRING, \
        PacienteRecuperado STRING, \
        FechaInicioSintomas STRING, \
        FechaMuerte STRING, \
        FechaRecuperacion STRING \
    ) \
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \
    WITH SERDEPROPERTIES ( \
        'field.delim' = ',' \
    ) \
    STORED AS TEXTFILE \
    LOCATION 's3://covid19bucket-mauricio/Trusted/DataFrame_Procesado/' \
    TBLPROPERTIES ( \
        'skip.header.line.count'='1', \
        'serialization.null.format'='' \
    )" \
    --query-execution-context Database="default" \
    --result-configuration OutputLocation="s3://datasets-mauricio/wcout10/"

