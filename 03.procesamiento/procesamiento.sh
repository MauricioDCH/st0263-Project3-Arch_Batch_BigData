#!/bin/bash
# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DE INGESTA DE DATOS
# RUTAS DE LOS ARCHIVOS
SCRIPT_PYSPARK_PROCESAMIENTO="/home/hadoop/spark-etl.py"
CRON_FILE2="/home/hadoop/cronjob2.txt"
SCRIPT_PYTHON="/home/hadoop/export_data.py"
SCRIPT_PYTHON_API="/home/hadoop/ingesta_api.py"
SCRIPT_CSV="/home/hadoop/basededatos.csv"
SCRIPT_CSV_API="/home/hadoop/covid_api.csv"
SCRIPT_DAILY="/home/hadoop/daily_data.sh"
# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# ARCHIVO DE PROCESAMIENTO DE DATOS
# ---------------------------------------------------------------------------------------------------------------------
## CREAR EL ARCHIVO PYSPARK PARA EL PROCESAMIENTO DE DATOS.
# ---------------------------------------------------------
echo '
# spark-submit spark-etl.py

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("SparkETL").getOrCreate()

    # Leer los tres archivos CSV y combinarlos
    csv_files = [
        "s3://covid19bucket-mauricio/Raw/basededatos.csv",
        "s3://covid19bucket-mauricio/Raw/covid_api.csv",
        "s3://covid19bucket-mauricio/Raw/datacovid.csv"
    ]
    df_covid_init = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(csv_files)
    # Registrar la tabla en Spark SQL
    df_covid_init.createOrReplaceTempView("covid19")
    # Consultar los datos desde SQL
    spark.sql("SELECT * FROM covid19").show(10)
    # Forma del dataset. (# Filas, # Colunas)
    print((df_covid_init.count(),len(df_covid_init.columns)))
    df_covid_init.printSchema()
    df_covid_init.columns
    # Eliminación de columnas que no se van a usar.
    df_eliminacion_columnas = df_covid_init.drop( "fecha reporte web",\
                                                "ID de caso",\
                                                "Código DIVIPOLA departamento",\
                                                "Código DIVIPOLA municipio",\
                                                "Unidad de medida de edad",\
                                                "Código ISO del país",\
                                                "Fecha de diagnóstico",\
                                                "Tipo de recuperación",\
                                                "Pertenencia étnica",\
                                                "Nombre del grupo étnico")
    
    # Forma del dataset. (# Filas, # Colunas)
    print((df_eliminacion_columnas.count(),len(df_eliminacion_columnas.columns)))

    # Cambio de nombre de las columnas.
    df_eliminacion_columnas.columns
    df_cambio_columnas = df_eliminacion_columnas.withColumnRenamed("Fecha de notificación","FechaNotificacion")\
                                                .withColumnRenamed("Nombre departamento","Departamento")\
                                                .withColumnRenamed("Nombre municipio","Ciudad")\
                                                .withColumnRenamed("Tipo de contagio","TipoContagio")\
                                                .withColumnRenamed("Ubicación del caso","UbicacionPaciente")\
                                                .withColumnRenamed("Estado","EstadoPaciente")\
                                                .withColumnRenamed("Nombre del país","PaisProcedencia")\
                                                .withColumnRenamed("Recuperado","PacienteRecuperado")\
                                                .withColumnRenamed("Fecha de inicio de síntomas","FechaInicioSintomas")\
                                                .withColumnRenamed("Fecha de muerte","FechaMuerte")\
                                                .withColumnRenamed("Fecha de recuperación","FechaRecuperacion")
    df_cambio_columnas.columns

    # CAMBIAMOS EL FORMATO DE LAS FECHAS.
    df_cambio_columnas = df_cambio_columnas.withColumn("FechaNotificacion", col("FechaNotificacion").cast("date"))\
                                            .withColumn("FechaInicioSintomas", col("FechaInicioSintomas").cast("date"))\
                                            .withColumn("FechaMuerte", col("FechaMuerte").cast("date"))\
                                            .withColumn("FechaRecuperacion", col("FechaRecuperacion").cast("date"))

    # FECHA POR DEFECTO PARA LAS FECHAS QUE NO TIENEN VALOR.
    fecha_por_defecto = to_date(lit("1900-01-01"), "yyyy-MM-dd")
    # Fecha por defecto significa que:
        # - No se tiene información de la fecha de muerte, por tanto se recuperó.
        # - No se tiene información de la fecha de recuperación, por lo tanto, no se recuperó.

    # MODIFICACIÓN DE ALGUNAS ENTRADAS DE LAS COLUMNAS PARA QUE LAS "N/A" Y NULL QUEDEN COMO "NO REGISTRA".
    df_cambio_columnas = df_cambio_columnas.withColumn("UbicacionPaciente", when(col("UbicacionPaciente")=="N/A", "NO REGISTRA").otherwise(col("UbicacionPaciente")))\
                                            .withColumn("EstadoPaciente", when(col("EstadoPaciente")=="N/A", "NO REGISTRA").otherwise(col("EstadoPaciente")))\
                                            .withColumn("PaisProcedencia", when(col("PaisProcedencia").isNull(), "COLOMBIA").otherwise(col("PaisProcedencia")))\
                                            .withColumn("PacienteRecuperado", when(col("PacienteRecuperado")=="N/A", "NO REGISTRA").otherwise(col("PacienteRecuperado")))\
                                            .withColumn("FechaMuerte", when(col("FechaMuerte").isNull(), fecha_por_defecto).otherwise(col("FechaMuerte")))\
                                            .withColumn("FechaRecuperacion",when(col("FechaRecuperacion").isNull(), fecha_por_defecto).otherwise(col("FechaRecuperacion")))

    # MODIFICACIÓN DE ALGUNAS COLUMNAS PARA HACER LA UNIFICACIÓN DE ENTRADAS A MAYÚSCULAS.
    df_cambio_columnas = df_cambio_columnas.withColumn("Departamento",upper(col("Departamento")))\
                                            .withColumn("Ciudad",upper(col("Ciudad")))\
                                            .withColumn("Sexo",upper(col("Sexo")))\
                                            .withColumn("TipoContagio",upper(col("TipoContagio")))\
                                            .withColumn("UbicacionPaciente",upper(col("UbicacionPaciente")))\
                                            .withColumn("EstadoPaciente",upper(col("EstadoPaciente")))\
                                            .withColumn("PaisProcedencia",upper(col("PaisProcedencia")))\
                                            .withColumn("PacienteRecuperado",upper(col("PacienteRecuperado")))\
                                            .withColumn("FechaMuerte",upper(col("FechaMuerte")))\
                                            .withColumn("FechaRecuperacion",upper(col("FechaRecuperacion")))

    # MOSTRAMOS LOS PRIMEROS 10 REGISTROS DEL DATAFRAME.
    df_cambio_columnas.show(10)

    # CREAMOS LOS DATAFRAMES PARA CADA COLUMNA.
    # "FechaNotificacion"
    dataframe_FechaNotificacion = df_cambio_columnas.groupBy("FechaNotificacion").count()
    # "Departamento"
    dataframe_Departamento = df_cambio_columnas.groupBy("Departamento").count()
    # "Ciudad"
    dataframe_Ciudad = df_cambio_columnas.groupBy("Ciudad").count()
    # "Edad"
    dataframe_Edad = df_cambio_columnas.groupBy("Edad").count()
    # "Sexo"
    dataframe_Sexo = df_cambio_columnas.groupBy("Sexo").count()
    # "TipoContagio"
    dataframe_TipoContagio = df_cambio_columnas.groupBy("TipoContagio").count()
    # "UbicacionPaciente"
    dataframe_UbicacionPaciente = df_cambio_columnas.groupBy("UbicacionPaciente").count()
    # "EstadoPaciente"
    dataframe_EstadoPaciente = df_cambio_columnas.groupBy("EstadoPaciente").count()
    # "PaisProcedencia"
    dataframe_PaisProcedencia = df_cambio_columnas.groupBy("PaisProcedencia").count()
    # "PacienteRecuperado"
    dataframe_PacienteRecuperado = df_cambio_columnas.groupBy("PacienteRecuperado").count()
    # "FechaInicioSintomas"
    dataframe_FechaInicioSintomas = df_cambio_columnas.groupBy("FechaInicioSintomas").count()
    # "FechaMuerte"
    dataframe_FechaMuerte = df_cambio_columnas.groupBy("FechaMuerte").count()
    # "FechaRecuperacion"
    dataframe_FechaRecuperacion = df_cambio_columnas.groupBy("FechaRecuperacion").count()

    # Directorios para guardar los dataframes.
    # Trusted.
    write_uri_DataFrame_Procesado = "s3://covid19bucket-mauricio/Trusted/DataFrame_Procesado"

    # Refined.
    write_uri_FechaNotificacion = "s3://covid19bucket-mauricio/Refined/FechaNotificacion"
    write_uri_Departamento = "s3://covid19bucket-mauricio/Refined/Departamento"
    write_uri_Ciudad = "s3://covid19bucket-mauricio/Refined/Ciudad"
    write_uri_Edad = "s3://covid19bucket-mauricio/Refined/Edad"
    write_uri_Sexo = "s3://covid19bucket-mauricio/Refined/Sexo"
    write_uri_TipoContagio = "s3://covid19bucket-mauricio/Refined/TipoContagio"
    write_uri_UbicacionPaciente = "s3://covid19bucket-mauricio/Refined/UbicacionPaciente"
    write_uri_EstadoPaciente = "s3://covid19bucket-mauricio/Refined/EstadoPaciente"
    write_uri_PaisProcedencia = "s3://covid19bucket-mauricio/Refined/PaisProcedencia"
    write_uri_PacienteRecuperado = "s3://covid19bucket-mauricio/Refined/PacienteRecuperado"
    write_uri_FechaInicioSintomas = "s3://covid19bucket-mauricio/Refined/FechaInicioSintomas"
    write_uri_FechaMuerte = "s3://covid19bucket-mauricio/Refined/FechaMuerte"
    write_uri_FechaRecuperacion = "s3://covid19bucket-mauricio/Refined/FechaRecuperacion"

    # GUARDAMOS LOS DATAFRAMES EN UN CSV
    # DataFrame Completo.
    df_cambio_columnas.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_DataFrame_Procesado)

    # "FechaNotificacion"
    dataframe_FechaNotificacion.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_FechaNotificacion)
    # "Departamento"
    dataframe_Departamento.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_Departamento)
    # "Ciudad"
    dataframe_Ciudad.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_Ciudad)
    # "Edad"
    dataframe_Edad.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_Edad)
    # "Sexo"
    dataframe_Sexo.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_Sexo)
    # "TipoContagio"
    dataframe_TipoContagio.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_TipoContagio)
    # "UbicacionPaciente"
    dataframe_UbicacionPaciente.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_UbicacionPaciente)
    # "EstadoPaciente"
    dataframe_EstadoPaciente.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_EstadoPaciente)
    # "PaisProcedencia"
    dataframe_PaisProcedencia.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_PaisProcedencia)
    # "PacienteRecuperado"
    dataframe_PacienteRecuperado.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_PacienteRecuperado)
    # "FechaInicioSintomas"
    dataframe_FechaInicioSintomas.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_FechaInicioSintomas)
    # "FechaMuerte"
    dataframe_FechaMuerte.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_FechaMuerte)
    # "FechaRecuperacion"
    dataframe_FechaRecuperacion.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save(write_uri_FechaRecuperacion)

    df_covid_init.select("Edad","Nombre municipio").show(10)
' > $SCRIPT_PYSPARK_PROCESAMIENTO

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# PERMISO DE EJECUCIÓN
## Otorgar permisos de ejecución al script
chmod +x $SCRIPT_PYSPARK_PROCESAMIENTO

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# EJECUCIÓN DE LOS SCRIPTS
## Ejecutar el script de PYSPARK por primera vez.
echo "Ejecutando el script de PYSPARK para procesamiento de datos..."
spark-submit $SCRIPT_PYSPARK_PROCESAMIENTO


# Configurar tareas en cron
echo "Configurando tareas cron..."

# Crear un archivo cron con las configuraciones
echo "*/10 * * * * python3 $SCRIPT_PYTHON && aws s3 cp $SCRIPT_CSV s3://covid19bucket-mauricio/Raw/" > $CRON_FILE2
echo "*/10 * * * * python3 $SCRIPT_PYTHON_API && aws s3 cp $SCRIPT_CSV_API s3://covid19bucket-mauricio/Raw/" >> $CRON_FILE2
echo "0 0 * * * /bin/bash $SCRIPT_DAILY" >> $CRON_FILE2
echo "*/15 * * * * spark-submit $SCRIPT_PYSPARK_PROCESAMIENTO" > $CRON_FILE2


# Aplicar el archivo cron al usuario "hadoop"
crontab $CRON_FILE2

# Limpiar el archivo temporal del cron
#rm $CRON_FILE2

echo "Tareas cron configuradas:"
echo " - Script de procesamiento de datos cada 15 minutos."
