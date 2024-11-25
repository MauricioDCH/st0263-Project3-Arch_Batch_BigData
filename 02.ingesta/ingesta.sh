#!/bin/bash
# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# CONFIGURACIÓN DE INGESTA DE DATOS
# RUTAS DE LOS ARCHIVOS
SCRIPT_PYTHON="/home/hadoop/export_data.py"
SCRIPT_PYTHON_API="/home/hadoop/ingesta_api.py"
SCRIPT_CSV="/home/hadoop/basededatos.csv"
SCRIPT_CSV_API="/home/hadoop/covid_api.csv"
SCRIPT_DAILY="/home/hadoop/daily_data.sh"
CRON_FILE="/home/hadoop/cronjob.txt"

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# ARCHIVO DE INGESTA DE DATOS
# ---------------------------------------------------------------------------------------------------------------------
## CREAR EL SCRIPT DIARIO QUE DESCARGA LOS DATOS Y LOS SUBE A S3.
# ---------------------------------------------------------------
echo '
#!/bin/bash
# Descargar los datos en formato CSV
wget -O /home/hadoop/datacovid.csv "https://www.datos.gov.co/api/views/gt2j-8ykr/rows.csv?accessType=DOWNLOAD"
aws s3 cp /home/hadoop/datacovid.csv s3://covid19bucket-mauricio/Raw/
rm /home/hadoop/datacovid.csv

echo "Datos descargados y subidos a S3."
' > $SCRIPT_DAILY

# ---------------------------------------------------------------------------------------------------------------------
## CREAR EL ARCHIVO PYTHON PARA LA EXPORTACIÓN DE DATOS.
# ------------------------------------------------------
echo '
import mysql.connector
import pandas as pd

# Conexión a la base de datos.
conn = mysql.connector.connect(
    host="database-1.c1wixjboxffd.us-east-1.rds.amazonaws.com",
    user="mauricio",
    password="mauricio123",
    database="mauricio"
)

# Consulta para obtener los datos de la tabla Covid19
query = "SELECT * FROM Covid19"

# Usa pandas para ejecutar la consulta y guardar los datos en un archivo CSV
df = pd.read_sql(query, conn)
df.to_csv("'$SCRIPT_CSV'", index=False)

# Cierra la conexión
conn.close()

print("Datos exportados a '$SCRIPT_CSV'")
' > $SCRIPT_PYTHON

# ---------------------------------------------------------------------------------------------------------------------
## CREAR EL SCRIPT PYTHON PARA EXPORTAR DATOS DE LA API.
# ------------------------------------------------------
echo '
import csv
import requests

# URL de la API
url = "https://www.datos.gov.co/resource/gt2j-8ykr.json"

# Función para obtener los datos de la API
def obtener_datos_api():
    # Realizar la solicitud HTTP GET
    try:
        response = requests.get(url)
        response.raise_for_status()  # Verifica si la solicitud fue exitosa (código 200)
        return response.json()  # Convertir la respuesta JSON en un diccionario de Python
    except requests.exceptions.RequestException as e:
        print(f"Error al realizar la petición: {e}")
        return []

# Función para escribir los datos en un archivo CSV
def escribir_csv(datos):
    nombre_archivo = "'$SCRIPT_CSV_API'"
    
    # Encabezado del CSV
    encabezado = [
        "fecha reporte web", "ID de caso", "Fecha de notificación", "Código DIVIPOLA departamento", 
        "Nombre departamento", "Código DIVIPOLA municipio", "Nombre municipio", "Edad", 
        "Unidad de medida de edad", "Sexo", "Tipo de contagio", "Ubicación del caso", "Estado", 
        "Código ISO del país", "Nombre del país", "Recuperado", "Fecha de inicio de síntomas", 
        "Fecha de muerte", "Fecha de diagnóstico", "Fecha de recuperación", "Tipo de recuperación", 
        "Pertenencia étnica", "Nombre del grupo étnico"
    ]
    
    # Crear o abrir el archivo CSV
    with open(nombre_archivo, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=encabezado)
        
        # Si el archivo está vacío, escribir el encabezado
        if file.tell() == 0:
            writer.writeheader()
        
        # Escribir cada fila de datos
        for item in datos:
            # Escribir los datos
            writer.writerow({
                "fecha reporte web": item.get("fecha_reporte_web", ""),
                "ID de caso": item.get("id_de_caso", ""),
                "Fecha de notificación": item.get("fecha_de_notificaci_n", ""),
                "Código DIVIPOLA departamento": item.get("departamento", ""),
                "Nombre departamento": item.get("departamento_nom", ""),
                "Código DIVIPOLA municipio": item.get("ciudad_municipio", ""),
                "Nombre municipio": item.get("ciudad_municipio_nom", ""),
                "Edad": item.get("edad", ""),
                "Unidad de medida de edad": item.get("unidad_medida", ""),
                "Sexo": item.get("sexo", ""),
                "Tipo de contagio": item.get("fuente_tipo_contagio", ""),
                "Ubicación del caso": item.get("ubicacion", ""),
                "Estado": item.get("estado", ""),
                "Código ISO del país": "",
                "Nombre del país": "",
                "Recuperado": item.get("recuperado", ""),
                "Fecha de inicio de síntomas": item.get("fecha_inicio_sintomas", ""),
                "Fecha de muerte": item.get("fecha_muerte", ""),
                "Fecha de diagnóstico": item.get("fecha_diagnostico", ""),
                "Fecha de recuperación": item.get("fecha_recuperado", ""),
                "Tipo de recuperación": item.get("tipo_recuperacion", ""),
                "Pertenencia étnica": item.get("per_etn_", ""),
                "Nombre del grupo étnico": item.get("nom_grupo_", "")
            })

datos = obtener_datos_api()

if datos:
    escribir_csv(datos)
else:
    print("No se obtuvieron datos para escribir en el archivo CSV.")
' > $SCRIPT_PYTHON_API

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# PERMISOS Y DEPENDENCIAS
## Otorgar permisos de ejecución a los scripts
chmod +x $SCRIPT_PYTHON $SCRIPT_DAILY $SCRIPT_PYTHON_API

# ---------------------------------------------------------------------------------------------------------------------
## Instalar dependencias necesarias
echo "Instalando dependencias necesarias..."
pip3 install mysql-connector-python pandas requests

# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
# EJECUCIÓN DE LOS SCRIPTS
## Ejecutar los scripts de Python por primera vez.
echo "Ejecutando el script Python para exportar datos..."
python3 $SCRIPT_PYTHON
python3 $SCRIPT_PYTHON_API
$SCRIPT_DAILY

# Subir los archivo CSV generado por el script Python a S3
aws s3 cp $SCRIPT_CSV s3://covid19bucket-mauricio/Raw/
aws s3 cp $SCRIPT_CSV_API s3://covid19bucket-mauricio/Raw/

# Eliminar los archivo CSV después de subirlo a S3 (opcional)
rm $SCRIPT_CSV $SCRIPT_CSV_API

# Configurar tareas en cron
echo "Configurando tareas cron..."

# Crear un archivo cron con las configuraciones
echo "*/10 * * * * python3 $SCRIPT_PYTHON && aws s3 cp $SCRIPT_CSV s3://covid19bucket-mauricio/Raw/" > $CRON_FILE
echo "*/10 * * * * python3 $SCRIPT_PYTHON_API && aws s3 cp $SCRIPT_CSV_API s3://covid19bucket-mauricio/Raw/" >> $CRON_FILE
echo "0 0 * * * /bin/bash $SCRIPT_DAILY" >> $CRON_FILE

# Aplicar el archivo cron al usuario "hadoop"
crontab $CRON_FILE

# Limpiar el archivo temporal del cron
rm $CRON_FILE

echo "Tareas cron configuradas:"
echo " - Script de exportación de datos de la base de datos cada 10 minutos."
echo " - Script de exportación de datos de la API cada 10 minutos."
echo " - Script de descarga diaria todos los días a medianoche."
