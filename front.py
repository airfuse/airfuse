import streamlit as st
import requests

# URL del endpoint FastAPI
endpoint_url = "http://localhost:8000/datos"

# Realizar la solicitud GET al endpoint
response = requests.get(endpoint_url)

# Verificar el código de estado de la respuesta
if response.status_code == 200:
    # Obtener los datos en formato JSON
    json_data = response.json()

    # Mostrar los datos en Streamlit
    st.write("Datos obtenidos del endpoint:")
    st.write(json_data)
else:
    st.write("Error al obtener los datos del endpoint.")

# Iniciar la aplicación Streamlit
if __name__ == '__main__':
    st.title("Consumo de servicio REST con Streamlit")

