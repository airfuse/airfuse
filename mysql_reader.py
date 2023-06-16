import mysql.connector
import pyarrow as pa
import pandas as pd
import duckdb
import yaml
import json 
from fastapi import FastAPI
import psycopg2


con = duckdb.connect()



# Cargar los parámetros y la consulta SQL desde el archivo YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

host = config["host"]
user = config["user"]
password = config["password"]
database = config["database"]
consulta = config["consulta"]

# Conectarse a la base de datos MySQL
conexion = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=database
)


# Crear un cursor para ejecutar consultas SQL
cursor = conexion.cursor()


# Ejecutar la consulta SQL
cursor.execute(consulta)

# Obtener todos los registros de la consulta
registros = cursor.fetchall()

# Obtener los nombres de las columnas de la tabla
columnas = [i[0] for i in cursor.description]

# Crear una lista de filas para DataFrame
filas = []

# Agregar los datos de cada registro a las filas
for registro in registros:
    filas.append(list(registro))

#################################################################################################
# TODO 
# Crear un DataFrame de pandas 
# ESTA ES LA PARTE A OPTIMIZAR , usar a DUCKDB EN EL MEDIO conlleva replicar toda la tabla !! 
# SOLUCION : usar ARROW CON :
# 1 ARROW FLIGHT  : PROBLEMA (no tenia porteado a python hasta el 7 y en desarrollo)
# 2 TURBOODBC : https://turbodbc.readthedocs.io/en/latest/pages/advanced_usage.html#apache-arrow-support
#               COMPATIBLE con : MySQL, PostgreSQL, EXASOL, and MSSQL 
# 3 POLARS 
dataframe = pd.DataFrame(filas, columns=columnas)
################################################################################################







# Crear la aplicación FastAPI
app = FastAPI()






# Definir la ruta del endpoint para obtener los datos en formato JSON
@app.get("/datos")
async def obtener_datos():

    # Crear una tabla de Arrow a partir del DataFrame
    tabla_arrow = pa.Table.from_pandas(dataframe)

    # Imprimir la tabla Arrow en memoria
    #print(tabla_arrow)

    # Cerrar el cursor y la conexión a la base de datos
    cursor.close()
    conexion.close()


    #esto aca hoy no tiene sentido porque es si queremos usar DUCKDB para dejar en la BD o usar sus apis 
    results = con.execute("SELECT * FROM tabla_arrow limit 10 ").arrow()

    #print(type(results))



    # Obtener los nombres de las columnas de la tabla
    columnas = tabla_arrow.column_names

    # Crear una lista para almacenar los registros como diccionarios
    registros = []

    # Recorrer cada fila de la tabla
    for i in range(len(tabla_arrow)):
        fila = {}
        # Recorrer cada columna de la fila y obtener el valor correspondiente
        for columna in columnas:
            valor = tabla_arrow[columna][i].as_py()
            fila[columna] = valor
        registros.append(fila)

    #print(registros)

    #hay que limpiar los campos, porque al crear el json no es 100% valido y te filtra decimales y cosas numericas
    def default_json(t):
        return f'{t}'

    # Convertir la lista de registros a formato JSON
    json_data = json.dumps(registros,default=default_json)

    # Imprimir el JSON
    return json_data





if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)