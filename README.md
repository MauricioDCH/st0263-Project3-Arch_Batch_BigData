# info de la materia: ST0263 Tópicos en Telemática
#
# Estudiante(s): Mauricio David Correa Hernández, mdcorreah@eafit.edu.co. David Grisales Posada, dgrisalesp@eafit.edu.co. Juan Manuel Garzón, jmgarzonv@eafit.edu.co.
#
# Profesor: Edwin Nelson Montoya Múnera, emontoya@eafit.edu.co.
#


# Project 3 - Arquitectura Batch para Big Data
#
# 1. breve descripción de la actividad
#
En esta ocasión se realizó un trabajo de ingeniería de datos con la información del COVID-19 provista por el ministerio de Colombia. Para eso usamos un clúster EMR con una serie de steps en el cual ejecuta comandos de consola .sh. En los cuales los datos son limpiados, procesados y guardados en diferentes carpetas de un bucket S3.
## 1.1. Que aspectos cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)
- Se usó la obtención de datos por medio de API, base de datos con RDS MySQL, y por medio de archivo.
- Los datos se cargan en carpetas en el bucket, los datos puros en la zona Raw, los datos procesados en la zona Trusted y los datos limpios y procesados en la zona Refined.
- Se automatizaron los procesos ETL con Spark para limpiar, procesar los datos y guardarlos en el bucket S3.
- Se automatizó un proceso para cargar el csv generado en la zona Trusted a Power BI usando Athena y ODBC.
- Se automatizó la carga de archivos de la zona Refined hacia Athena.
- Se automatizó la carga de datos cada cierto tiempo usando cron.
## 1.2. Que aspectos NO cumplió o desarrolló de la actividad propuesta por el profesor (requerimientos funcionales y no funcionales)

# 2. información general de diseño de alto nivel, arquitectura, patrones, mejores prácticas utilizadas.
![image](https://github.com/user-attachments/assets/1793b076-ab5c-4e52-b082-568da1483cea)
Pasos:
* Creación clúster usando script creation_cluster
* Implementación step 1 con script init.sh para 
* Implementación step 2 con script init.sh para 
* Implementación step 3 con script init.sh para
* Implementación step 4 con script credentials.sh para subir las credenciales del laboratorio de AWS Academy para acceder a los servicios.
* Implementación step 5 con script creating_table.sh para crear la tabla sobre el covid con la información procesada almacenada en la zona Trusted.
* Implementación step 6 con script tables.sh para crear las tablas almacenadas en la zona Refined.
* Implementación ODBC en Power BI para leer los datos de las tablas en Athena y realizar análisis.
  
# 3. Descripción del ambiente de desarrollo y técnico: lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.

## como se compila y ejecuta.
## detalles del desarrollo.
## detalles técnicos
## descripción y como se configura los parámetros del proyecto (ej: ip, puertos, conexión a bases de datos, variables de ambiente, parámetros, etc)
## opcional - detalles de la organización del código por carpetas o descripción de algún archivo. (ESTRUCTURA DE DIRECTORIOS Y ARCHIVOS IMPORTANTE DEL PROYECTO, comando 'tree' de linux)
## 
## opcionalmente - si quiere mostrar resultados o pantallazos 

# 4. Descripción del ambiente de EJECUCIÓN (en producción) lenguaje de programación, librerias, paquetes, etc, con sus numeros de versiones.

# IP o nombres de dominio en nube o en la máquina servidor.

## descripción y como se configura los parámetros del proyecto (ej: ip, puertos, conexión a bases de datos, variables de ambiente, parámetros, etc)

## como se lanza el servidor.

## una mini guia de como un usuario utilizaría el software o la aplicación

## opcionalmente - si quiere mostrar resultados o pantallazos 

# 5. otra información que considere relevante para esta actividad.

# referencias:
<debemos siempre reconocer los créditos de partes del código que reutilizaremos, así como referencias a youtube, o referencias bibliográficas utilizadas para desarrollar el proyecto o la actividad>
## sitio1-url 
## sitio2-url
## url de donde tomo info para desarrollar este proyecto
