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
#from src.etl import extract as ex #con jupyter
import extract as ex ## con main.py

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


def url_y_opciones_viajes_1(pais, viaje,diccionario_opciones_detallado, diccionario_url_opcion_nombre_opcion):
    URL_ESCRAPEO_CORTA=os.getenv("URL_ESCRAPEO_CORTA")
    enlace = viaje.find('a')['href']
    if "https" in enlace:
        url= enlace
    else:
        if "opciones" in enlace:
            enlace = enlace.split('"')[0]
            url = URL_ESCRAPEO_CORTA+enlace
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
                diccionario_url_opcion_nombre_opcion["nombre_opcion"].append(opcion)
                diccionario_url_opcion_nombre_opcion["url_viaje_opcion"].append(url_opcion)
            sopa_opcion=ex.crear_sopa(url)
            clase_product_card_opcion=sopa_opcion.find_all("div", {"class":"product-card"})
            for viaje_opcion in clase_product_card_opcion:
                enlace_opcion_detalle = viaje_opcion.find('a')['href']
                if "https" in enlace_opcion_detalle:
                    url_opcion= enlace_opcion_detalle
                else:
                    url_opcion = URL_ESCRAPEO_CORTA+enlace_opcion_detalle
                nombre_viaje_opcion=viaje_opcion.find("div", {"class": "product-card-text-title"}).get_text().replace("\n","").rstrip(" ")
                duracion_viaje=viaje_opcion.find("div", {"class": "product-card-text-duration"}).get_text().replace("\n","").rstrip(" ")
                itinerario = viaje_opcion.find("div", {"class": "product-card-text-description"}).get_text().replace("\n","").rstrip(" ").lstrip("Itinerario: ")
                precio = int(viaje_opcion.find("div", {"class": "product-card-footer"}).get_text().replace("\n","").rstrip(" ").lstrip("desde ").split('€')[0].replace(".",""))
                diccionario_opciones_detallado["pais"].append(pais)
                diccionario_opciones_detallado["nombre_viaje"].append(nombre_viaje)
                diccionario_opciones_detallado["url_viaje"].append(url)
                diccionario_opciones_detallado["nombre_viaje_opcion"].append(nombre_viaje_opcion)
                diccionario_opciones_detallado["duracion_viaje"].append(duracion_viaje)
                diccionario_opciones_detallado["itinerario"].append(itinerario)
                diccionario_opciones_detallado["precio"].append(precio)
                diccionario_opciones_detallado["url_viaje_opcion"].append(url_opcion)
        else:
            enlace=enlace
        url = URL_ESCRAPEO_CORTA+enlace
    #print("ENLACE EXTRAIDO")
    #print(enlace)
    df_opciones = pd.DataFrame(diccionario_opciones_detallado)
    df_opciones_url_nombre_opcion= pd.DataFrame(diccionario_url_opcion_nombre_opcion)
    return url, df_opciones, df_opciones_url_nombre_opcion

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

def escrapeo_viajes_paises_con_url_1 (pais, clase, diccionario,diccionario_opciones_detallado,diccionario_url_opcion_nombre_opcion):
    for viaje in clase:
        nombre_viaje=viaje.find("div", {"class": "product-card-text-title"}).get_text().replace("\n","").rstrip(" ")
        #print(nombre_viaje)
        duracion_viaje=viaje.find("div", {"class": "product-card-text-duration"}).get_text().replace("\n","").rstrip(" ")
        #print(duracion_viaje)
        itinerario = viaje.find("div", {"class": "product-card-text-description"}).get_text().replace("\n","").rstrip(" ").lstrip("Itinerario: ")
        #print(itinerario)
        precio = int(viaje.find("div", {"class": "product-card-footer"}).get_text().replace("\n","").rstrip(" ").lstrip("desde ").split('€')[0].replace(".",""))
        #print(precio)
        url_viaje, df_opciones, df_opciones_url_nombre_opcion = ex.url_y_opciones_viajes_1(pais, viaje,diccionario_opciones_detallado, diccionario_url_opcion_nombre_opcion)
        diccionario["pais"].append(pais)
        diccionario["nombre_viaje"].append(nombre_viaje)
        diccionario["duracion_viaje"].append(duracion_viaje)
        diccionario["itinerario"].append(itinerario)
        diccionario["precio"].append(precio)
        diccionario["url"].append(url_viaje)
    return diccionario, df_opciones, df_opciones_url_nombre_opcion

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

def continentes_viajes_totales_destinos_url_y_opciones_2(df_destinos_totales):
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
                                    "url_viaje":[],
                                    "nombre_viaje_opcion":[],                   
                                    "duracion_viaje":[],                                    
                                    "itinerario":[],
                                    "precio": [],
                                    'url_viaje_opcion':[],
                                    "fecha_escrapeo": datetime.now().date()}
    dictio_scrap_urlopcion_nombreopcion = {"nombre_opcion":[],
                                    "url_viaje_opcion" :[],
                                    "fecha_escrapeo": datetime.now().date()}
    for pais,url in zip(df_destinos_totales["nombre_pais"], df_destinos_totales["url_destino"]):
        sopa_pais= ex.crear_sopa(url)
        clase_hot_pag2_alp_tit=sopa_pais.find_all("div", {"class":"hot-page2-alp-tit"})  #Vamos a extraer todas los contenidos de la clase hot-page2-alp-tit
        continente= clase_hot_pag2_alp_tit[0].get_text().strip("\n").split("-")[0]
        diccionario_continente["pais"].append(pais)
        diccionario_continente["continente"].append(continente)
        clase_product_card=sopa_pais.find_all("div", {"class":"product-card"})  #Vamos a extraer todas los contenidos de la clase product-card
        ex.escrapeo_viajes_paises_con_url_1(pais, clase_product_card, dictio_scrap_1, dictio_scrap_opciones, dictio_scrap_urlopcion_nombreopcion)
        print(f' Para {pais} en {continente} se ha encontrado {len(clase_product_card)} viajes diferentes')
        #print(dictio_scrap_opciones)
    print("El escrapeo ha finalizado, la extracción de la información de todos los destinos ha sido obtenida")
    df_viajes_totales_destinos = pd.DataFrame(dictio_scrap_1)
    df_viajes_totales_destinos['fecha_escrapeo']=pd.to_datetime(df_viajes_totales_destinos['fecha_escrapeo'], errors="coerce")
    df_continentes = pd.DataFrame(diccionario_continente)
    df_continentes['fecha_escrapeo']=pd.to_datetime(df_continentes['fecha_escrapeo'], errors="coerce")
    df_opciones = pd.DataFrame(dictio_scrap_opciones)
    df_opciones_url_nombre_opcion = pd.DataFrame(dictio_scrap_urlopcion_nombreopcion)
    df_opciones['fecha_escrapeo']=pd.to_datetime(df_opciones['fecha_escrapeo'], errors="coerce")
    df_opciones["opcion"]=None
    ex.incorporar_información_df_original(df_opciones, df_opciones_url_nombre_opcion, 'url_viaje_opcion', 'nombre_opcion', 'opcion')
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
        print("Archivo de la api con el turismo emisor de España descargado y guardado correctamente.")
    else:
        print(f"Error en la descarga. Código de estado: {respuesta.status_code}")
    print("LA EXTRACCIÓN DE LA API HA FINALIZADO")
   
# Indicar en una nueva columna si aparece en el último escrapeo
def disponible_ultimo_escrapeo (valor, df, columna):
    if valor in df[columna].to_list():
        return "Si"
    else:
        return "No"


def guardar_escrapeo(ARCHIVO_GUARDAR_ESCRAPEO, df_escrapeado, columna_union):
    if os.path.exists(ARCHIVO_GUARDAR_ESCRAPEO): #comprueba si existe un fichero ya con destinos
        df_existente = pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO) #en caso de existir lo importa
        df_combinado = pd.concat([df_existente,df_escrapeado], ignore_index=True) #combina el fichero anterior con el nuevo obtenido en el escrapeo
    else:
        df_combinado =df_escrapeado #si no existe fichero previo, entonces, crea uno con el obtenido en el escrapeo
    df_combinado["en_ultimo_escrapeo"] = df_combinado[columna_union].apply(lambda x: ex.disponible_ultimo_escrapeo(x, df_escrapeado, columna_union))
    df_combinado= df_combinado.drop_duplicates()
    df_combinado.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO) #guarda el fichero en formato formato pickle 
    return df_combinado

def escrapeo_total ():
    load_dotenv()
    RUTA_SERVICE = os.getenv("RUTA_SERVICE")
    URL_ESCRAPEO_INICIAL = os.getenv("URL_ESCRAPEO_INICIAL")

    url_destinos= URL_ESCRAPEO_INICIAL #url destinos totales
    sopa_destinos = ex.crear_sopa(url_destinos) #creamos la sopa

    df_destinos_totales = ex.extracción_destinos_con_url(sopa_destinos) #escrapeo los destinos totales

    #añado los destinos actuales a los destinos anteriores y guardo el DF con los destinos totales acumulados
    ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS = os.getenv("ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS")
    df_combinado_destinos= ex.guardar_escrapeo(ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS,df_destinos_totales, "nombre_pais")
    print(f'El fichero con todos los destinos acumulados ha sido actualizado con {df_destinos_totales.shape[0]} destinos. Existen un total de {df_combinado_destinos.shape[0]} destinos')

    df_viajes_totales_destinos, df_continentes, df_opciones = ex.continentes_viajes_totales_destinos_url_y_opciones_2(df_destinos_totales) #escrapeo el resto de información

    #añado los viajes actuales a los viajes anteriores y guardo el DF con los viajes totales de todos los destinos acumulados
    ARCHIVO_GUARDAR_ESCRAPEO_VIAJES = os.getenv("ARCHIVO_GUARDAR_ESCRAPEO_VIAJES")
    df_combinado_viajes_totales= ex.guardar_escrapeo(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES,df_viajes_totales_destinos, "url")
    print(f'El fichero con todos los viajes acumulados ha sido actualizado con {df_viajes_totales_destinos.shape[0]} viajes. Existen un total de {df_combinado_viajes_totales.shape[0]} viajes')

    #añado los continentes actuales a los continentes anteriores y guardo el DF con los continentes totales de todos los destinos acumulados
    ARCHIVO_GUARDAR_CONTINENTES = os.getenv("ARCHIVO_GUARDAR_CONTINENTES")
    df_combinado_continentes= ex.guardar_escrapeo(ARCHIVO_GUARDAR_CONTINENTES,df_continentes, "continente")
    print(f'El fichero con todos los continentes acumulados ha sido actualizado con {df_continentes.shape[0]} continentes. Existen un total de {df_combinado_continentes.shape[0]} continentes')

    #añado las opciones actuales a las opciones anteriores y guardo el DF con las opciones totales de todos los destinos acumulados
    ARCHIVO_GUARDAR_OPCIONES_VIAJES = os.getenv("ARCHIVO_GUARDAR_OPCIONES_VIAJES")
    df_combinado_opciones_viajes= ex.guardar_escrapeo(ARCHIVO_GUARDAR_OPCIONES_VIAJES,df_opciones, "url_viaje_opcion")
    print(f'El fichero con todos las opciones acumuladas ha sido actualizado con {df_opciones.shape[0]} opciones. Existen un total de {df_combinado_opciones_viajes.shape[0]} opciones ')
    print("EL ESCRAPEO HA FINALIZADO")