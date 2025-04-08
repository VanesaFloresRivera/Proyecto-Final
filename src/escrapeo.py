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
import src.escrapeo as es

def crear_sopa(url):
    res_pais=rq.get(url)# Accedemos a la url
    if res_pais.status_code ==200:
        sopa_pais=bs(res_pais.content,"html.parser") # Creamos la sopa
    return sopa_pais

def destinos_totales(clase):
    dictio_scrap_0 = {"nombre_pais": [],
                            "num_viajes_ofertados":[],
                            "fecha_escrapeo": datetime.now().date()}
    for destino in clase:
        nombre_pais=" ".join(destino.get_text().split(" ")[:-1])
        #print(nombre_pais)
        num_viajes_ofertados=int(destino.get_text().split(" ")[-1:][0].strip("(").strip(")"))
        #print(num_viajes_ofertados)
        dictio_scrap_0["nombre_pais"].append(nombre_pais)
        dictio_scrap_0["num_viajes_ofertados"].append(num_viajes_ofertados)
    return dictio_scrap_0

def escrapeo_viajes_paises (pais, clase, diccionario):
    for viaje in clase:
        nombre_viaje=viaje.find("div", {"class": "product-card-text-title"}).get_text().replace("\n","").rstrip(" ")
        #print(nombre_viaje)
        duracion_viaje=viaje.find("div", {"class": "product-card-text-duration"}).get_text().replace("\n","").rstrip(" ")
        #print(duracion_viaje)
        itinerario = viaje.find("div", {"class": "product-card-text-description"}).get_text().replace("\n","").rstrip(" ").lstrip("Itinerario: ")
        #print(itinerario)
        precio = int(viaje.find("div", {"class": "product-card-footer"}).get_text().replace("\n","").rstrip(" ").lstrip("desde ").split('€')[0].replace(".",""))
        #print(precio)
        diccionario["pais"].append(pais)
        diccionario["nombre_viaje"].append(nombre_viaje)
        diccionario["duracion_viaje"].append(duracion_viaje)
        diccionario["itinerario"].append(itinerario)
        diccionario["precio"].append(precio)
    return(diccionario)

def viajes_totales_destinos(destinos_totales):
    dictio_scrap_1 = {"pais": [],
                                    "nombre_viaje": [],
                                    "duracion_viaje":[],
                                    "itinerario":[],
                                    "precio": [],
                                    "fecha_escrapeo": datetime.now().date()}
    for pais in destinos_totales["nombre_pais"]:
        print(pais)
        URL_ESCRAPEO_VIAJES_PAIS=os.getenv("URL_ESCRAPEO_VIAJES_PAISES")+pais.replace(" ","-") #url original
        sopa_pais= es.crear_sopa(URL_ESCRAPEO_VIAJES_PAIS)
        clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
        es.escrapeo_viajes_paises(pais, clase_product_card, dictio_scrap_1)
        if len(clase_product_card) == 0: 
            URL_ESCRAPEO_VIAJES_PAIS_SIN_TILDE=os.getenv("URL_ESCRAPEO_VIAJES_PAISES")+pais.replace(" ","-").replace("á","a").replace("Á","a").replace("é","e").replace("É","e").replace("í","i").replace("Í","i").replace("ó","o").replace("Ó","o").replace("ú","u").replace("Ú","u").replace("ñ","n").replace("ç","c")
            sopa_pais= es.crear_sopa(URL_ESCRAPEO_VIAJES_PAIS_SIN_TILDE)
            clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
            es.escrapeo_viajes_paises(pais, clase_product_card, dictio_scrap_1)
            if len(clase_product_card) == 0: 
                URL_ESCRAPEO_VIAJES_PAIS_SIN_ESPACIO=os.getenv("URL_ESCRAPEO_VIAJES_PAISES")+pais.replace(" ","")
                sopa_pais= es.crear_sopa(URL_ESCRAPEO_VIAJES_PAIS_SIN_ESPACIO)
                clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
                es.escrapeo_viajes_paises(pais, clase_product_card, dictio_scrap_1)
                if len(clase_product_card) == 0: 
                    URL_ESCRAPEO_VIAJES_PAIS_SIN_ESPACIO_SIN_TILDE=os.getenv("URL_ESCRAPEO_VIAJES_PAISES")+pais.replace(" ","").replace("á","a").replace("Á","a").replace("é","e").replace("É","e").replace("í","i").replace("Í","i").replace("ó","o").replace("Ó","o").replace("ú","u").replace("Ú","u").replace("ñ","n").replace("ç","c")
                    sopa_pais= es.crear_sopa(URL_ESCRAPEO_VIAJES_PAIS_SIN_ESPACIO_SIN_TILDE)
                    clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
                    es.escrapeo_viajes_paises(pais, clase_product_card, dictio_scrap_1)
    print("El escrapeo ha finalizado, la extracción de la información de todos los destinos ha sido obtenida")
    return dictio_scrap_1

def tran_data(lista_col, dataframe):
    """_summary_

    Args:
        lista_col (_type_): _description_
        dataframe (_type_): _description_
    """
    for col in lista_col:
        dataframe[col] = pd.to_datetime(dataframe[col], errors="coerce")