import pandas as pd
import requests as rq
import numpy as np
import pyarrow
from bs4 import BeautifulSoup as bs
import os
from dotenv import load_dotenv
import time
from datetime import datetime
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
from src.etl import extract as ex

load_dotenv()

RUTA_SERVICE = os.getenv("RUTA_SERVICE")
URL_ESCRAPEO_INICIAL = os.getenv("URL_ESCRAPEO_INICIAL")


url_destinos= URL_ESCRAPEO_INICIAL #url destinos totales
sopa_destinos = ex.crear_sopa(url_destinos) #creamos la sopa
#Extraemos todos los contenidos de la clase landing-destination-title
clase_landing_destination_title=sopa_destinos.find_all("div", {"class":"landing-destination-title"})
print(f'Existen {len(clase_landing_destination_title)} destinos diferentes')

df_destinos_totales = ex.extracción_destinos_con_url(sopa_destinos) #escrapeo los destinos totales

#añado los destinos actuales a los destinos anteriores y guardo el DF con los destinos totales acumulados
ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS = os.getenv("ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS")
if os.path.exists(ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS): #comprueba si existe un fichero ya con destinos
    df_existente_destinos = pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS) #en caso de existir lo importa
    df_combinado_destinos = pd.concat([df_existente_destinos,df_destinos_totales], ignore_index=True) #combina el fichero anterior con el nuevo obtenido en el escrapeo
else:
    df_combinado_destinos=df_destinos_totales #si no existe fichero previo, entonces, crea uno con el obtenido en el escrapeo
df_combinado_destinos["en_ultimo_escrapeo"] = df_combinado_destinos["nombre_pais"].apply(lambda x: ex.disponible_ultimo_escrapeo(x, df_destinos_totales, 'nombre_pais'))
df_combinado_destinos= df_combinado_destinos.drop_duplicates()
df_combinado_destinos.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS) #guarda el fichero en formato formato pickle

df_viajes_totales_destinos, df_continentes, df_opciones = ex.continentes_viajes_totales_destinos_url_y_opciones_1(df_destinos_totales)

#añado los viajes actuales a los viajes anteriores y guardo el DF con los viajes totales de todos los destinos acumulados
ARCHIVO_GUARDAR_ESCRAPEO_VIAJES = os.getenv("ARCHIVO_GUARDAR_ESCRAPEO_VIAJES")
if os.path.exists(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES): #comprueba si existe un fichero ya con viajes
    df_existente_viajes_totales= pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES)#en caso de existir lo importa
    df_combinado_viajes_totales = pd.concat([df_existente_viajes_totales,df_viajes_totales_destinos], ignore_index=True) #combina el fichero anterior con el nuevo obtenido en el escrapeo
else:
    df_combinado_viajes_totales=df_viajes_totales_destinos  #si no existe fichero previo, entonces, crea uno con el obtenido en el escrapeo
df_combinado_viajes_totales["en_ultimo_escrapeo"] = df_combinado_viajes_totales["url"].apply(lambda x: ex.disponible_ultimo_escrapeo(x, df_viajes_totales_destinos, 'url'))
df_combinado_viajes_totales =df_combinado_viajes_totales.drop_duplicates()
df_combinado_viajes_totales.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES) #guarda el fichero en formato pickle

#añado los continentes actuales a los continentes anteriores y guardo el DF con los continentes totales de todos los destinos acumulados
ARCHIVO_GUARDAR_CONTINENTES = os.getenv("ARCHIVO_GUARDAR_CONTINENTES")
if os.path.exists(ARCHIVO_GUARDAR_CONTINENTES): #comprueba si existe un fichero ya con continentes
    df_existente_continentes= pd.read_pickle(ARCHIVO_GUARDAR_CONTINENTES)#en caso de existir lo importa
    df_combinado_continentes = pd.concat([df_existente_continentes,df_continentes], ignore_index=True) #combina el fichero anterior con el nuevo obtenido en el escrapeo
else:
    df_combinado_continentes=df_continentes  #si no existe fichero previo, entonces, crea uno con el obtenido en el escrapeo
df_combinado_continentes["en_ultimo_escrapeo"] = df_combinado_continentes["continente"].apply(lambda x: ex.disponible_ultimo_escrapeo(x, df_continentes, 'continente'))
df_combinado_continentes.drop_duplicates(inplace=True)
df_combinado_continentes.to_pickle(ARCHIVO_GUARDAR_CONTINENTES) #guarda el fichero en formato pickle

#añado las opciones actuales a las opciones anteriores y guardo el DF con las opciones totales de todos los destinos acumulados
ARCHIVO_GUARDAR_OPCIONES_VIAJES = os.getenv("ARCHIVO_GUARDAR_OPCIONES_VIAJES")
if os.path.exists(ARCHIVO_GUARDAR_OPCIONES_VIAJES): #comprueba si existe un fichero ya con continentes
    df_existente_opciones= pd.read_pickle(ARCHIVO_GUARDAR_OPCIONES_VIAJES)#en caso de existir lo importa
    df_combinado_opciones_viajes = pd.concat([df_existente_opciones,df_opciones], ignore_index=True) #combina el fichero anterior con el nuevo obtenido en el escrapeo
else:
    df_combinado_opciones_viajes=df_opciones  #si no existe fichero previo, entonces, crea uno con el obtenido en el escrapeo
df_combinado_opciones_viajes["en_ultimo_escrapeo"] = df_combinado_opciones_viajes["url_opcion"].apply(lambda x: ex.disponible_ultimo_escrapeo(x, df_opciones, 'url_opcion'))
df_combinado_opciones_viajes= df_combinado_opciones_viajes.drop_duplicates()
df_combinado_opciones_viajes.to_pickle(ARCHIVO_GUARDAR_OPCIONES_VIAJES) #guarda el fichero en formato pickle