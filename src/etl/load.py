import pandas as pd
import numpy as np
import psycopg2
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
from src.etl import load as lo #con jupyter
import os
from dotenv import load_dotenv

load_dotenv()

#Llamamos a la variable de entorno
DB_NAME = os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST= os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

    #creo la conexión:
def crear_conexión (dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT):

    conn = psycopg2.connect(
    dbname=dbname,
    user=user,
    password=password,
    host=host,
    port=port
    )
    #creo el cursor
    cur= conn.cursor()

    return conn, cur

def cerrar_conexion (conexion, cursor):
    cursor.close() #cierro el cursor
    conexion.close() #cierro la conexión

def insertar_datos_en_BBDD(insert_query,data_to_insert,dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):
    
    if len(data_to_insert)>0:
        conn, cur=lo.crear_conexión(dbname, user, password, host, port)

        if isinstance(data_to_insert,list): # si data_to_insert es una lista
            cur.executemany(insert_query,data_to_insert) #ejecuto la acción de subida a la BBDD
        else: # si data_to_insert no es una lista
            cur.execute(insert_query,data_to_insert) #ejecuto la acción de subida a la BBDD
        conn.commit() #guardo la subida

        lo.cerrar_conexion(conn, cur)

def extraer_datos_de_BBDD(query_extracción,dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extracción) #ejecuto la extracción
    diccionario = dict(cur.fetchall()) #creo un diccionario

    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return diccionario

def limpiar_texto(texto):
    # Dividir por comas
    lista_texto = texto.split(',')
    
    # Quitar espacios a cada palabra
    lista_limpia = []
    for palabra in lista_texto:
        palabra_sin_espacios = palabra.strip() #quitar espacios del principio y del final
        lista_limpia.append(palabra_sin_espacios)
    
    # Unir de nuevo por comas y espacio
    texto_final = ', '.join(lista_limpia)
    
    return texto_final

def convertir_si_no_a_boolean(df, nombre_columna):
    """
    Convierte una columna del DataFrame que contiene 'Si'/'No' a booleanos True/False.
    
    Parámetros:
    - df: pandas DataFrame
    - nombre_columna: string con el nombre de la columna a convertir
    
    Retorna:
    - df con la columna convertida
    """
    df[nombre_columna] = df[nombre_columna].str.lower().map({'si': True, 'no': False})
    return df

def actualizar_datos_en_bbdd(query_actualizacion,tupla_columnas, dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_actualizacion, tupla_columnas) #ejecuto la query de actualizacion

    lo.cerrar_conexion(conn, cur) #cierro la conexión

def extraer_tupla_datos_bbdd (query_extraccion_tupla, dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extraccion_tupla) #ejecuto la extracción
    existing_pairs = set(cur.fetchall()) #creo un diccionario

    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return existing_pairs