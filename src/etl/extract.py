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
    """
    Realiza una petición HTTP a una URL y devuelve una sopa (objeto BeautifulSoup) del contenido HTML si la respuesta es exitosa.

    Parámetros:
    -----------
    url : str
        URL desde la que se desea obtener el contenido HTML.

    Retorna:
    --------
    bs4.BeautifulSoup
        Objeto BeautifulSoup con el contenido HTML parseado, si la respuesta es exitosa (status code 200).
    
    None
        Si la respuesta no es exitosa, no se devuelve ningún valor.
    """
    res_pais=rq.get(url)# Accedemos a la url
    if res_pais.status_code ==200:
        sopa_pais=bs(res_pais.content,"html.parser") # Creamos la sopa
    return sopa_pais

#función para extraer en un solo bucle la url y el nombre del destino y número de viajes
def extracción_destinos_con_url(sopa):
    """
    Extrae los destinos turísticos disponibles desde una sopa HTML y construye un DataFrame con la información.

    Parámetros:
    -----------
    sopa : bs4.BeautifulSoup
        Objeto BeautifulSoup que contiene el HTML de la página web de TUI ya parseado.

    Retorna:
    --------
    pandas.DataFrame
        DataFrame con las siguientes columnas:
        - 'nombre_pais': nombre del país destino.
        - 'num_viajes_ofertados': número de viajes ofertados a ese destino.
        - 'url_destino': URL completa del destino.
        - 'fecha_escrapeo': fecha en la que se realizó el scraping (fecha actual).

    Notas:
    ------
    - Requiere que la variable de entorno `URL_ESCRAPEO_CORTA` esté definida, ya que se utiliza como base para construir las URLs completas.
    - Los datos se extraen de elementos HTML con clases específicas según el diseño del sitio web de TUI.
    """
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
    """
    Extrae la URL principal de un viaje y, si el viaje contiene opciones, agrega los detalles de cada opción
    (nombre, opción, URL y precio) al diccionario proporcionado, devolviendo también la URL del viaje principal.

    Parámetros:
    -----------
    pais : str
        Nombre del país al que pertenece el viaje. Se almacena en el diccionario de opciones.
    
    viaje : bs4.element.Tag
        Objeto BeautifulSoup que representa un contenedor HTML de un viaje individual.
    
    diccionario_opciones : dict
        Diccionario que almacena los datos de las opciones del viaje. Se espera que tenga como claves:
        'pais', 'nombre_viaje', 'opcion', 'url_opcion', 'precio'.

    Retorna:
    --------
    tuple:
        - url : str
            URL principal del viaje.
        - df_opciones : pandas.DataFrame
            DataFrame con las opciones de viaje (si existen) extraídas del diccionario actualizado.

    Notas:
    ------
    - Si el enlace del viaje contiene "opciones", se extraerán todas las subopciones con sus respectivos precios.
    - La variable de entorno `URL_ESCRAPEO_CORTA` debe estar definida para construir URLs relativas.
    - El parámetro `diccionario_opciones` es actualizado dentro de la función (efecto colateral).
    """
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
    """
    Extrae la URL principal de un viaje y, si contiene subopciones, obtiene información detallada de cada una:
    nombre, duración, itinerario, precio y URLs, rellenando dos diccionarios con dicha información.

    Parámetros:
    -----------
    pais : str
        Nombre del país al que pertenece el viaje (usado para etiquetar los registros).
    
    viaje : bs4.element.Tag
        Objeto BeautifulSoup que representa un bloque HTML de un viaje individual con posibles subopciones.
    
    diccionario_opciones_detallado : dict
        Diccionario que se llenará con los detalles completos de cada opción de viaje. Espera las claves:
        - 'pais', 'nombre_viaje', 'url_viaje', 'nombre_viaje_opcion', 
          'duracion_viaje', 'itinerario', 'precio', 'url_viaje_opcion'.
    
    diccionario_url_opcion_nombre_opcion : dict
        Diccionario más simple que recoge la relación entre nombre de opción y su URL.
        Espera las claves: 'nombre_opcion', 'url_viaje_opcion'.

    Retorna:
    --------
    tuple:
        - url : str
            URL principal del viaje (la página base del viaje con opciones).
        
        - df_opciones : pandas.DataFrame
            DataFrame con el contenido de `diccionario_opciones_detallado`, con todos los detalles de las subopciones.
        
        - df_opciones_url_nombre_opcion : pandas.DataFrame
            DataFrame con el contenido de `diccionario_url_opcion_nombre_opcion`, útil para una vista más simple.

    Notas:
    ------
    - Si el viaje contiene subopciones, la función accede a la URL del viaje, parsea la nueva sopa y extrae información de cada tarjeta de viaje (`.product-card`).
    - Depende de la variable de entorno `URL_ESCRAPEO_CORTA` para construir URLs relativas.
    - Esta función modifica los diccionarios pasados como parámetros (efecto colateral).
    """
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
    """
    Realiza el scraping de los viajes de un país y extrae su información principal (nombre, duración, itinerario, precio y URL),
    almacenándola en un diccionario. Además, extrae también las opciones de cada viaje mediante una función auxiliar.

    Parámetros:
    -----------
    pais : str
        Nombre del país al que pertenecen los viajes.
    
    clase : list of bs4.element.Tag
        Lista de elementos HTML (tarjetas de viaje) que contienen la información de cada viaje.
    
    diccionario : dict
        Diccionario donde se almacena la información principal de los viajes. Se espera que tenga estas claves:
        - 'pais', 'nombre_viaje', 'duracion_viaje', 'itinerario', 'precio', 'url_viaje'.
    
    diccionario_opciones : dict
        Diccionario que se actualiza con las opciones de cada viaje, a través de la función `url_y_opciones_viajes`.

    Retorna:
    --------
    tuple:
        - diccionario : dict
            Diccionario actualizado con los datos de los viajes del país.
        
        - df_opciones : pandas.DataFrame
            DataFrame generado por la función `url_y_opciones_viajes`, que contiene las opciones asociadas a los viajes.

    Notas:
    ------
    - Utiliza la función `ex.url_y_opciones_viajes` para obtener las URLs completas y opciones de cada viaje.
    - La función tiene efectos colaterales sobre los diccionarios proporcionados.
    """
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
        diccionario["url_viaje"].append(url_viaje)
    return diccionario, df_opciones

def escrapeo_viajes_paises_con_url_1 (pais, clase, diccionario,diccionario_opciones_detallado,diccionario_url_opcion_nombre_opcion):
    """
    Realiza el scraping de los viajes de un país, extrayendo la información principal de cada viaje (nombre, duración,
    itinerario, precio y URL) y también la información detallada de las distintas opciones de viaje.

    Parámetros:
    -----------
    pais : str
        Nombre del país al que pertenecen los viajes.
    
    clase : list of bs4.element.Tag
        Lista de elementos HTML que representan tarjetas individuales de viajes dentro del país.

    diccionario : dict
        Diccionario donde se almacena la información general de los viajes. Se espera que contenga las claves:
        - 'pais', 'nombre_viaje', 'duracion_viaje', 'itinerario', 'precio', 'url_viaje'.

    diccionario_opciones_detallado : dict
        Diccionario donde se almacenan los detalles completos de cada opción de viaje, incluyendo:
        - 'pais', 'nombre_viaje', 'url_viaje', 'nombre_viaje_opcion', 'duracion_viaje',
          'itinerario', 'precio', 'url_viaje_opcion'.

    diccionario_url_opcion_nombre_opcion : dict
        Diccionario que guarda la relación entre los nombres de las opciones y sus URLs.
        Debe contener las claves: 'nombre_opcion' y 'url_viaje_opcion'.

    Retorna:
    --------
    tuple:
        - diccionario : dict
            Diccionario actualizado con los datos generales de los viajes del país.
        
        - df_opciones : pandas.DataFrame
            DataFrame con los datos detallados de las opciones del viaje.
        
        - df_opciones_url_nombre_opcion : pandas.DataFrame
            DataFrame que relaciona nombre de opción con su URL, útil para análisis o navegación.

    Notas:
    ------
    - Esta función invoca a `ex.url_y_opciones_viajes_1` para realizar un scraping detallado de cada viaje.
    - Los diccionarios proporcionados se actualizan dentro de la función (efectos colaterales).
    - Asegúrate de inicializar los diccionarios con listas vacías antes de llamar a esta función.
    """
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
        diccionario["url_viaje"].append(url_viaje)
    return diccionario, df_opciones, df_opciones_url_nombre_opcion

def continentes_viajes_totales_destinos_url_y_opciones_1(df_destinos_totales):
    """
    Realiza el scraping de todos los viajes a partir del DataFrame de destinos, extrayendo información detallada
    sobre los viajes, sus opciones y los continentes asociados a cada país.

    Parámetros:
    -----------
    df_destinos_totales : pandas.DataFrame
        DataFrame con las columnas 'nombre_pais' y 'url_destino', obtenido previamente por scraping,
        que indica para cada país el enlace a la página de viajes.

    Retorna:
    --------
    tuple:
        - df_viajes_totales_destinos : pandas.DataFrame
            Contiene los detalles generales de cada viaje, con columnas como:
            'pais', 'nombre_viaje', 'duracion_viaje', 'itinerario', 'precio', 'url_viaje', 'fecha_escrapeo'.
        
        - df_continentes : pandas.DataFrame
            Relación entre países y continentes, con columnas: 'continente', 'pais', 'fecha_escrapeo'.
        
        - df_opciones : pandas.DataFrame
            Información de las distintas opciones disponibles por viaje, con columnas:
            'pais', 'nombre_viaje', 'opcion', 'precio', 'url_opcion', 'fecha_escrapeo'.

    Notas:
    ------
    - Utiliza la función `ex.crear_sopa(url)` para cargar el HTML de cada país.
    - Utiliza `ex.escrapeo_viajes_paises_con_url` para obtener los detalles de cada viaje y sus opciones.
    - El continente se extrae de la sección `.hot-page2-alp-tit` del HTML de cada país.
    - Se imprimen mensajes por consola para seguimiento del progreso del scraping.
    - Todas las fechas de escrapeo se registran con la fecha actual.

    Advertencias:
    -------------
    - Se espera que las clases HTML utilizadas (`.hot-page2-alp-tit` y `.product-card`) estén presentes
      y bien estructuradas en la web de origen. Si cambian, podría romper el scraping.
    """
    dictio_scrap_1 = {"pais": [],
                                    "nombre_viaje": [],
                                    "duracion_viaje":[],               
                                    "itinerario":[],
                                    "precio": [],
                                    "url_viaje":[],
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
    """
    Realiza el scraping completo de viajes a partir de un DataFrame de destinos, obteniendo:
    - Información general de los viajes,
    - Relación país–continente,
    - Detalles completos de las opciones de viaje,
    - Asociación entre nombre de opción y URL de la opción.

    Parámetros:
    -----------
    df_destinos_totales : pandas.DataFrame
        DataFrame con las columnas 'nombre_pais' y 'url_destino', indicando los destinos de TUI y sus URLs.

    Retorna:
    --------
    tuple:
        - df_viajes_totales_destinos : pandas.DataFrame
            Contiene los detalles generales de cada viaje:
            'pais', 'nombre_viaje', 'duracion_viaje', 'itinerario', 'precio', 'url_viaje', 'fecha_escrapeo'.
        
        - df_continentes : pandas.DataFrame
            Asocia cada país con su continente. Contiene:
            'continente', 'pais', 'fecha_escrapeo'.
        
        - df_opciones : pandas.DataFrame
            Detalles completos de las distintas opciones por viaje, con columnas como:
            'pais', 'nombre_viaje', 'url_viaje', 'nombre_viaje_opcion',
            'duracion_viaje', 'itinerario', 'precio', 'url_viaje_opcion', 'opcion', 'fecha_escrapeo'.

    Notas:
    ------
    - Utiliza `ex.crear_sopa` para extraer el HTML de cada página de país.
    - Extrae el continente desde la clase `.hot-page2-alp-tit` del HTML.
    - Usa `ex.escrapeo_viajes_paises_con_url_1` para extraer los viajes y sus opciones detalladas.
    - También llama a `ex.incorporar_información_df_original` para asociar cada URL de opción con su nombre descriptivo.
    - Todos los registros llevan la fecha de escrapeo (fecha del sistema al momento de ejecución).
    - Se imprimen mensajes informativos por consola para seguimiento.

    Advertencias:
    -------------
    - Las clases HTML utilizadas están sujetas a cambios por parte de la web, lo que puede romper el scraping.
    - La variable de entorno `URL_ESCRAPEO_CORTA` debe estar correctamente configurada.
    """
    dictio_scrap_1 = {"pais": [],
                                    "nombre_viaje": [],
                                    "duracion_viaje":[],
                                    "itinerario":[],
                                    "precio": [],
                                    "url_viaje":[],
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


def incorporar_información_df_original (dataframe_a_rellenar, df_valores_correctos, columna_union, columna_valores_correctos, columna_a_rellenar, filtro_df_a_rellenar=None):
    """
    Completa valores en un DataFrame a partir de otro DataFrame de referencia usando un diccionario de mapeo.

    Args:
        dataframe_a_rellenar (pd.DataFrame): DataFrame donde se rellenarán los valores.
        df_valores_correctos (pd.DataFrame): DataFrame con los valores correctos de referencia.
        columna_union (str): Nombre de la columna clave utilizada para la unión entre ambos DataFrames.
        columna_valores_correctos (str): Nombre de la columna en 'df_valores_correctos' con los valores a insertar.
        columna_a_rellenar (str): Nombre de la columna en 'dataframe_a_rellenar' donde se colocarán los valores correctos.
        filtro_df_a_rellenar (pd.Series, optional): Filtro booleano para aplicar la actualización solo a ciertas filas. 
                                                  Si es None, se actualizará toda la columna. Default es None.

    Returns:
        None: Modifica 'dataframe_a_rellenar' directamente.
    """
    keys= df_valores_correctos[columna_union].to_list()
    values = df_valores_correctos[columna_valores_correctos].to_list()
    diccionario_creado =dict(zip(keys,values))
    if filtro_df_a_rellenar is not None:
        dataframe_a_rellenar.loc[filtro_df_a_rellenar,columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)
    else:
        dataframe_a_rellenar[columna_a_rellenar] = dataframe_a_rellenar[columna_union].map(diccionario_creado)

#función para extraer los datos de la API
def extraccion_datos_api(): 
    """
    Descarga un archivo desde una API REST pública (formato binario) y lo guarda en la ruta indicada
    mediante variables de entorno.

    Parámetros:
    -----------
    No recibe parámetros directamente. Utiliza variables de entorno para configurar:
    - URL_API: URL del endpoint de la API.
    - ARCHIVO_GUARDAR_DATOS_API: ruta o nombre del archivo donde se guardará el contenido descargado.

    Proceso:
    --------
    - Realiza una solicitud GET a la URL indicada, con cabecera 'accept: application/octet-stream'.
    - Si la descarga es exitosa (status 200), guarda el contenido binario en el archivo indicado.
    - Imprime mensajes informativos sobre el éxito o fallo de la operación.

    Retorna:
    --------
    None

    Notas:
    ------
    - Se espera que el archivo descargado sea un `.csv`, `.xlsx` u otro archivo binario manejable localmente.
    - Utiliza `os.getenv` para mayor seguridad y flexibilidad en despliegues.
    - La ruta absoluta se construye automáticamente con `os.path.abspath`.

    Advertencias:
    -------------
    - Si las variables de entorno no están definidas correctamente, la función podría fallar.
    - Si la API devuelve un código de estado distinto de 200, no se guarda ningún archivo.
    """
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
    """
    Indica si un valor dado aparece en una columna específica de un DataFrame.

    Parámetros:
    -----------
    valor : any
        Valor que se desea buscar dentro de la columna del DataFrame.

    df : pandas.DataFrame
        DataFrame donde se realiza la búsqueda (por ejemplo, resultados del último escrapeo).

    columna : str
        Nombre de la columna dentro del DataFrame donde se buscará el valor.

    Retorna:
    --------
    str
        "Si" si el valor se encuentra en la columna del DataFrame.
        "No" en caso contrario.

    Ejemplo de uso:
    ---------------
    df["disponible"] = df_apariciones["nombre_viaje"].apply(lambda x: disponible_ultimo_escrapeo(x, df_actual, "nombre_viaje"))
    """
    if valor in df[columna].to_list():
        return "Si"
    else:
        return "No"


def guardar_escrapeo(ARCHIVO_GUARDAR_ESCRAPEO, df_escrapeado, columna_union):
    """
    Guarda el resultado del escrapeo actual en un archivo `.pkl`, combinándolo con datos anteriores si existen,
    e indicando qué registros pertenecen al último escrapeo.

    Parámetros:
    -----------
    ARCHIVO_GUARDAR_ESCRAPEO : str
        Ruta (relativa o absoluta) donde se almacenará el archivo `.pkl` con el historial del scraping.

    df_escrapeado : pandas.DataFrame
        DataFrame con los datos recién obtenidos por scraping.

    columna_union : str
        Nombre de la columna que se usará como identificador único para comparar registros entre
        el scraping anterior y el nuevo (por ejemplo, 'nombre_viaje').

    Retorna:
    --------
    df_combinado : pandas.DataFrame
        DataFrame resultante de combinar el scraping anterior (si existe) con el nuevo,
        con una nueva columna `en_ultimo_escrapeo` que indica si el registro pertenece al scraping más reciente.

    Proceso:
    --------
    - Si el archivo existe, se carga y combina con los nuevos datos.
    - Se añade una columna `en_ultimo_escrapeo` con valores "Si" o "No" según pertenezcan al scraping actual.
    - Se eliminan duplicados.
    - El resultado se guarda nuevamente en formato pickle.

    Notas:
    ------
    - Requiere la función `disponible_ultimo_escrapeo()` de `ex` para marcar los registros recientes.
    - El formato `.pkl` permite conservar estructuras de datos complejas de pandas de forma eficiente.
    """
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
    """
    Ejecuta el proceso completo de scraping del portal de TUI para clientes españoles, incluyendo:
    - Extracción de destinos,
    - Extracción de viajes por país,
    - Extracción de continentes asociados,
    - Extracción de opciones de viaje detalladas.

    El flujo automatiza la descarga de datos actuales, los guarda en archivos acumulados,
    y marca qué registros pertenecen al último escrapeo.

    Parámetros:
    -----------
    No recibe parámetros. Las rutas, URLs y configuraciones se cargan desde variables de entorno usando `dotenv`.

    Proceso:
    --------
    1. Carga la URL inicial del portal de destinos TUI.
    2. Extrae los destinos ofertados y los guarda en un archivo acumulado.
    3. Por cada destino, realiza scraping de:
        - Continente asociado.
        - Lista de viajes ofertados con información detallada.
        - Opciones disponibles para cada viaje.
    4. Guarda cada conjunto de datos en archivos `.pkl` acumulativos:
        - Destinos (`ARCHIVO_GUARDAR_ESCRAPEO_DESTINOS`)
        - Viajes (`ARCHIVO_GUARDAR_ESCRAPEO_VIAJES`)
        - Continentes (`ARCHIVO_GUARDAR_CONTINENTES`)
        - Opciones de viaje (`ARCHIVO_GUARDAR_OPCIONES_VIAJES`)
    5. Añade una columna `en_ultimo_escrapeo` en cada archivo para marcar registros actuales.

    Retorna:
    --------
    None

    Notas:
    ------
    - Requiere definir previamente las variables de entorno con `dotenv`, incluyendo URLs y rutas de guardado.
    - Usa funciones auxiliares del módulo `ex` para modularizar el scraping y la combinación de datos.
    - Se imprime información del progreso y resumen de cada parte del proceso.

    Archivos generados:
    -------------------
    - Un `.pkl` por tipo de dato (destinos, viajes, continentes, opciones), con datos acumulados y marcadores de actualización.
    """
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
    df_combinado_viajes_totales= ex.guardar_escrapeo(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES,df_viajes_totales_destinos, "url_viaje")
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