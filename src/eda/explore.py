import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import os
from dotenv import load_dotenv
import unicodedata
from src.etl import load as lo #con jupyter

load_dotenv()

#Llamamos a la variable de entorno
DB_NAME = os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST= os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def extraer_datos_bbdd_y_convertir_en_df (lista_nombre_columnas, query_extracción,dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extracción) #ejecuto la extracción
    resultados = cur.fetchall()
    df = pd.DataFrame(resultados, columns=lista_nombre_columnas) #creo el df
    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return df