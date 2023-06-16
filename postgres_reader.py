import psycopg2
import yaml


# Cargar los parámetros y la consulta SQL desde el archivo YAML
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


# Establecer la conexión a la base de datos PostgreSQL
conexion = psycopg2.connect(
    host=config["host"],
    port="5432",
    database=config["database"],
    user=config["user"],
    password=config["password"]
)

# Crear un cursor para ejecutar consultas SQL
cursor = conexion.cursor()

# Consulta SQL para seleccionar todos los registros de una tabla
consulta = "SELECT *  FROM accounts "

# Ejecutar la consulta SQL
cursor.execute(consulta)

# Obtener todos los registros de la consulta
registros = cursor.fetchall()

# Imprimir los registros
for registro in registros:
    print(registro)

# Cerrar el cursor y la conexión a la base de datos
cursor.close()
conexion.close()
