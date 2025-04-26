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

def crear_sopa(url):
    res_pais=rq.get(url)# Accedemos a la url
    if res_pais.status_code ==200:
        sopa_pais=bs(res_pais.content,"html.parser") # Creamos la sopa
    return sopa_pais

#función para extraer en un solo bucle la url y el nombre del destino y número de viajes
def extracción_destinos_con_url(sopa):
    URL_ESCRAPEO_CORTA=os.getenv("URL_ESCRAPEO_CORTA")
    dictio_scrap_0 = {"nombre_pais": [],
                            "num_viajes_ofertados":[],
                            "url_destino": [],
                            "fecha_escrapeo": datetime.now().date()}
    destinos=sopa.find_all("div", class_="col-md-3 col-sm-6 col-xs-12 b_packages wow slideInUp landing-destination-box")

    for destino in destinos:
        enlace = destino.find("a")["href"]
        url = URL_ESCRAPEO_CORTA+enlace
        titulo = destino.find("div", class_="landing-destination-title").text.strip()
        nombre_pais= " ".join(titulo.split(" ")[:-1])
        num_viajes_ofertados=int(titulo.split(" ")[-1:][0].strip("(").strip(")"))
        dictio_scrap_0["nombre_pais"].append(nombre_pais)
        dictio_scrap_0["num_viajes_ofertados"].append(num_viajes_ofertados)
        dictio_scrap_0["url_destino"].append(url)
    df_destinos_totales = pd.DataFrame(dictio_scrap_0)
    df_destinos_totales['fecha_escrapeo']=pd.to_datetime(df_destinos_totales['fecha_escrapeo'], errors="coerce")
    return df_destinos_totales  


def url_y_opciones_viajes(pais, viaje,diccionario_opciones):
    URL_ESCRAPEO_CORTA=os.getenv("URL_ESCRAPEO_CORTA")
    enlace = viaje.find('a')['href']
    if "https" in enlace:
        url= enlace
    else:
        if "opciones" in enlace:
            enlace = enlace.split('"')[0]
            #print(enlace)
            nombre_viaje=viaje.find("div", {"class": "product-card-text-title"}).get_text().replace("\n","").rstrip(" ")
            #print(nombre_viaje)
            col_lg_9_elements = viaje.select('.col-lg-9')
            for opcion in col_lg_9_elements:
                enlace_opcion = opcion.find('a')['href']
                if "https" in enlace_opcion:
                    url_opcion=enlace_opcion
                else:
                    url_opcion = URL_ESCRAPEO_CORTA + opcion.find('a')['href']
                opcion=opcion.get_text(strip=True)
                #print(salida)
                diccionario_opciones["pais"].append(pais)
                diccionario_opciones["nombre_viaje"].append(nombre_viaje)
                diccionario_opciones["opcion"].append(opcion)
                diccionario_opciones["url_opcion"].append(url_opcion)
            col_lg_3_elements = viaje.select('.col-lg-3')
            for precio in col_lg_3_elements:
                precio = int(precio.get_text(strip=True).strip('€').replace(".",""))
                diccionario_opciones["precio"].append(precio)
        else:
            enlace=enlace
        url = URL_ESCRAPEO_CORTA+enlace
    #print("ENLACE EXTRAIDO")
    #print(enlace)
    df_opciones = pd.DataFrame(diccionario_opciones)
    return url, df_opciones


def escrapeo_viajes_paises_con_url (pais, clase, diccionario,diccionario_opciones):
    for viaje in clase:
        nombre_viaje=viaje.find("div", {"class": "product-card-text-title"}).get_text().replace("\n","").rstrip(" ")
        #print(nombre_viaje)
        duracion_viaje=viaje.find("div", {"class": "product-card-text-duration"}).get_text().replace("\n","").rstrip(" ")
        #print(duracion_viaje)
        itinerario = viaje.find("div", {"class": "product-card-text-description"}).get_text().replace("\n","").rstrip(" ").lstrip("Itinerario: ")
        #print(itinerario)
        precio = int(viaje.find("div", {"class": "product-card-footer"}).get_text().replace("\n","").rstrip(" ").lstrip("desde ").split('€')[0].replace(".",""))
        #print(precio)
        url_viaje, df_opciones = ex.url_y_opciones_viajes(pais, viaje,diccionario_opciones)
        diccionario["pais"].append(pais)
        diccionario["nombre_viaje"].append(nombre_viaje)
        diccionario["duracion_viaje"].append(duracion_viaje)
        diccionario["itinerario"].append(itinerario)
        diccionario["precio"].append(precio)
        diccionario["url"].append(url_viaje)
    return diccionario, df_opciones


def continentes_viajes_totales_destinos_url_y_opciones_1(df_destinos_totales):
    dictio_scrap_1 = {"pais": [],
                                    "nombre_viaje": [],
                                    "duracion_viaje":[],
                                    "itinerario":[],
                                    "precio": [],
                                    "url":[],
                                    "fecha_escrapeo": datetime.now().date()}
    diccionario_continente = {"continente": [],
                                    "pais": [],
                                    "fecha_escrapeo": datetime.now().date()}
    dictio_scrap_opciones = {"pais":[],
                                    "nombre_viaje": [],
                                    "opcion":[],
                                    "precio": [],
                                    "url_opcion" :[],
                                    "fecha_escrapeo": datetime.now().date()}
    for pais,url in zip(df_destinos_totales["nombre_pais"], df_destinos_totales["url_destino"]):
        sopa_pais= ex.crear_sopa(url)
        clase_hot_pag2_alp_tit=sopa_pais.find_all("div", {"class":"hot-page2-alp-tit"})  #Vamos a extraer todas los contenidos de la clase hot-page2-alp-tit
        continente= clase_hot_pag2_alp_tit[0].get_text().strip("\n").split("-")[0]
        diccionario_continente["pais"].append(pais)
        diccionario_continente["continente"].append(continente)
        clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
        ex.escrapeo_viajes_paises_con_url(pais, clase_product_card, dictio_scrap_1, dictio_scrap_opciones)
        print(f' Para {pais} en {continente} se ha encontrado {len(clase_product_card)} viajes diferentes')
        #print(dictio_scrap_opciones)
    print("El escrapeo ha finalizado, la extracción de la información de todos los destinos ha sido obtenida")
    df_viajes_totales_destinos = pd.DataFrame(dictio_scrap_1)
    df_viajes_totales_destinos['fecha_escrapeo']=pd.to_datetime(df_viajes_totales_destinos['fecha_escrapeo'], errors="coerce")
    df_continentes = pd.DataFrame(diccionario_continente)
    df_continentes['fecha_escrapeo']=pd.to_datetime(df_continentes['fecha_escrapeo'], errors="coerce")
    df_opciones = pd.DataFrame(dictio_scrap_opciones)
    df_opciones['fecha_escrapeo']=pd.to_datetime(df_opciones['fecha_escrapeo'], errors="coerce")
    return df_viajes_totales_destinos, df_continentes, df_opciones


def incorporar_información_df_original (dataframe_a_rellenar, df_valores_correctos, columna_union, columna_valores_correctos, columna_a_rellenar, filtro_df_original=None):
    """
    Completa valores en un DataFrame a partir de otro DataFrame de referencia usando un diccionario de mapeo.

    Args:
        dataframe_a_rellenar (pd.DataFrame): DataFrame donde se rellenarán los valores.
        df_valores_correctos (pd.DataFrame): DataFrame con los valores correctos de referencia.
        columna_union (str): Nombre de la columna clave utilizada para la unión entre ambos DataFrames.
        columna_valores_correctos (str): Nombre de la columna en 'df_valores_correctos' con los valores a insertar.
        columna_a_rellenar (str): Nombre de la columna en 'dataframe_a_rellenar' donde se colocarán los valores correctos.
        filtro_df_original (pd.Series, optional): Filtro booleano para aplicar la actualización solo a ciertas filas. 
                                                  Si es None, se actualizará toda la columna. Default es None.

    Returns:
        None: Modifica 'dataframe_a_rellenar' directamente.
    """
    keys= df_valores_correctos[columna_union].to_list()
    values = df_valores_correctos[columna_valores_correctos].to_list()
    diccionario_creado =dict(zip(keys,values))
    if filtro_df_original is not None:
        dataframe_a_rellenar.loc[filtro_df_original,columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)
    else:
        dataframe_a_rellenar[columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)

#función para extraer los datos de la API
def extraccion_datos_api(): 
    #Creamos el end point de la api
    URL_API= os.getenv("URL_API")
    url = URL_API
    headers = {'accept': 'application/octet-stream'}

    #obtenemos la respuesta de la api:
    respuesta = rq.get(url, headers=headers)

    ARCHIVO_GUARDAR_DATOS_API= os.getenv("ARCHIVO_GUARDAR_DATOS_API")
    ruta_destino = os.path.abspath(os.path.join(os.getcwd(), ARCHIVO_GUARDAR_DATOS_API))

    # Guardar el archivo si la respuesta fue exitosa
    if respuesta.status_code == 200:
        with open(ruta_destino, 'wb') as f:
            f.write(respuesta.content)
        print("Archivo descargado correctamente.")
    else:
        print(f"Error en la descarga. Código de estado: {respuesta.status_code}")

# Indicar en una nueva columna si aparece en el último escrapeo
def disponible_ultimo_escrapeo (valor, df, columna):
    if valor in df[columna].to_list():
        return "Si"
    else:
        return "No"