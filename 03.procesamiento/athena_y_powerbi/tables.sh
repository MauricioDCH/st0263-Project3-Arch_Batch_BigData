#!/bin/bash

OUTPUT_LOCATION="s3://datasets-mauricio/wcout10/"
DATABASE="default"

TABLES=(
  "FechaNotificacion STRING, count INT"
  "Departamento STRING, count INT"
  "Ciudad STRING, count INT"
  "Edad INT, count INT"
  "Sexo STRING, count INT"
  "TipoContagio STRING, count INT"
  "UbicacionPaciente STRING, count INT"
  "EstadoPaciente STRING, count INT"
  "PaisProcedencia STRING, count INT"
  "PacienteRecuperado STRING, count INT"
  "FechaInicioSintomas STRING, count INT"
  "FechaMuerte STRING, count INT"
  "FechaRecuperacion STRING, count INT"
)

LOCATIONS=(
  "s3://covid19bucket-mauricio/Refined/FechaNotificacion/"
  "s3://covid19bucket-mauricio/Refined/Departamento/"
  "s3://covid19bucket-mauricio/Refined/Ciudad/"
  "s3://covid19bucket-mauricio/Refined/Edad/"
  "s3://covid19bucket-mauricio/Refined/Sexo/"
  "s3://covid19bucket-mauricio/Refined/TipoContagio/"
  "s3://covid19bucket-mauricio/Refined/UbicacionPaciente/"
  "s3://covid19bucket-mauricio/Refined/EstadoPaciente/"
  "s3://covid19bucket-mauricio/Refined/PaisProcedencia/"
  "s3://covid19bucket-mauricio/Refined/PacienteRecuperado/"
  "s3://covid19bucket-mauricio/Refined/FechaInicioSintomas/"
  "s3://covid19bucket-mauricio/Refined/FechaMuerte/"
  "s3://covid19bucket-mauricio/Refined/FechaRecuperacion/"
)

TABLE_NAMES=(
  "FechaNotificacion"
  "Departamento"
  "Ciudad"
  "Edad"
  "Sexo"
  "TipoContagio"
  "UbicacionPaciente"
  "EstadoPaciente"
  "PaisProcedencia"
  "PacienteRecuperado"
  "FechaInicioSintomas"
  "FechaMuerte"
  "FechaRecuperacion"
)

for i in ${!TABLES[@]}; do
  QUERY="CREATE EXTERNAL TABLE IF NOT EXISTS ${TABLE_NAMES[$i]} (
      ${TABLES[$i]}
  )
  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
  WITH SERDEPROPERTIES (
      'field.delim' = ','
  )
  STORED AS TEXTFILE
  LOCATION '${LOCATIONS[$i]}'
  TBLPROPERTIES (
      'skip.header.line.count'='1',
      'serialization.null.format'=''
  );"

  echo "Creating table: ${TABLE_NAMES[$i]}"
  aws athena start-query-execution \
      --query-string "$QUERY" \
      --query-execution-context Database="$DATABASE" \
      --result-configuration OutputLocation="$OUTPUT_LOCATION"
done
