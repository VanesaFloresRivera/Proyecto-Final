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
from thefuzz import fuzz
#from src.etl import transform as tr #con jupyter
#from src.etl import extract as ex #con jupyter
import extract as ex ## con main.py
import transform as tr ## con main.py
import unicodedata
import re
import warnings
from pandas.errors import SettingWithCopyWarning


#funcion para obtener pais, continente, latitud y longitud
def obtener_pais_continente_lat_long (municipio,API_KEY):
    url= f'https://api.opencagedata.com/geocode/v1/json?q= {municipio}&key={API_KEY}&language=es'
    response =rq.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            # Usamos .get() para acceder de forma segura a las claves
            continente = data['results'][0]['components'].get('continent', 'Información desconocida')
            pais = data['results'][0]['components'].get('country', 'Información desconocida')
            lat = data['results'][0]['geometry'].get('lat', 'Información desconocida')
            lon = data['results'][0]['geometry'].get('lng', 'Información desconocida')
            return continente,pais, lat, lon 
        else:
            return 'Información desconocida', 'Información desconocida','Información desconocida','Información desconocida'
    else:
        print("Error de conexión")
    print(f'Se han obtenido datos de geolocalización de la api')

def obtener_pais_continente_lat_long_con_pais (municipio,pais,API_KEY):
    url= f'https://api.opencagedata.com/geocode/v1/json?q= {municipio}+{pais}&key={API_KEY}&language=es'
    response =rq.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            #print('Encuentra data results')
            # Usamos .get() para acceder de forma segura a las claves
            continente = data['results'][0]['components'].get('continent', 'Información desconocida')
            #print(continente)
            if continente == 'Información desconocida':
                url= f'https://api.opencagedata.com/geocode/v1/json?q= {municipio}&key={API_KEY}&language=es'
                response =rq.get(url)
                if response.status_code == 200:
                    data = response.json()
                    if data['results']:
                                continente = data['results'][0]['components'].get('continent', 'Ciudad no encontrada')
                                pais_api = data['results'][0]['components'].get('country', 'Ciudad no encontrada')
                                lat = data['results'][0]['geometry'].get('lat', 'Ciudad no encontrada')
                                lon = data['results'][0]['geometry'].get('lng', 'Ciudad no encontrada')
                                resultado = 'Ciudad encontrada en otro pais'
                                #print('sigue el camino del tercer if')
                                return continente,pais_api, lat, lon, resultado                        
            else:
                pais_api = data['results'][0]['components'].get('country', 'Información desconocida')
                #print(f' el pais que me devuelve la api es {pais_api}')
                lat = data['results'][0]['geometry'].get('lat', 'Información desconocida')
                lon = data['results'][0]['geometry'].get('lng', 'Información desconocida')
                resultado ='Ciudad y pais encontrado'
                return continente,pais_api, lat, lon, resultado
        else:
            resultado = 'Ciudad no encontrada'
            return 'No existe resultado', 'No existe resultado','No existe resultado','No existe resultado', resultado  
    else:
        resultado = 'Error'
        print("Error de conexión")
    print(f'Se han obtenido datos de geolocalización de la api')
    

def normalizar_texto(x):
    if isinstance(x, str):
        # Poner en minúscula
        x = x.lower()
        # Quitar tildes y caracteres especiales
        x = unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode('utf-8')
        # Sustituir espacios múltiples por uno solo
        x = re.sub(r'\s+', ' ', x)
        # Quitar espacios al principio y al final
        x = x.strip()
    return x

def capitalizar_texto(x):
    if isinstance(x, str):
        # Poner en mayúscula
        #x = ' '.join([palabra.capitalize() for palabra in x.split()])
        return x.title()
    return x


def limpieza_fichero_turismo_emisor(df_turismo_emisor):
    indice_otros= df_turismo_emisor[df_turismo_emisor.PAIS_DESTINO.str.contains('Otros')].index.tolist() #elimino los que no indica un pais concreto
    indice_total=df_turismo_emisor[df_turismo_emisor.PAIS_DESTINO.str.contains('Total')].index.tolist() #elimino los totales
    indices_eliminar = indice_otros+indice_total #uno ambos indices
    df_turismo_emisor.drop(indices_eliminar,inplace=True)

    #Reemplazo los valores que al unirlo con el DF de escrapeo da lugar a duplicados:
    df_turismo_emisor.PAIS_DESTINO= df_turismo_emisor.PAIS_DESTINO.replace(
    {'Antigua y Barbuda': 'Antigua',
    'Estados Unidos de América':'Estados Unidos',
    'Bhután': 'Bután',
    'Corea':'Corea Del Sur',
    'Zimbabwe':'Zimbabue'})


    ARCHIVO_GUARDAR_DATOS_API_PROCESADOS=os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS')
    df_turismo_emisor.to_csv(ARCHIVO_GUARDAR_DATOS_API_PROCESADOS) #guardo el fichero
    print(f'Los datos de turismo emisor han sido procesados y guardados: {len(df_turismo_emisor)}')
    return df_turismo_emisor

def obtención_continentes_correctos(df_continentes, lista_paises_islas, ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY):
    if len(lista_paises_islas)>0:
    #obtengo los datos ya obtenidos de la api de geolocalización
        if os.path.exists(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE):
            df_continentes_correctos_anterior= pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE)
        
            df_continentes_correctos_anterior_filtrado=df_continentes_correctos_anterior[df_continentes_correctos_anterior['pais'].notna()]
            #incorporo la información en la tabla continentes original:
            ex.incorporar_información_df_original(df_continentes, df_continentes_correctos_anterior_filtrado,
                                            'pais', 'continente_api', 'continente', 
                                            df_continentes.continente.isin(['Caribe', 'Oriente Medio', 'Islas Exóticas']))
            df_continentes_reducido = df_continentes[['continente', 'pais']].drop_duplicates()
            lista_paises_islas = df_continentes_reducido[df_continentes_reducido.continente.isin(['Caribe', 'Oriente Medio', 'Islas Exóticas'])].pais.tolist()
        if len(lista_paises_islas)>0:
            #obtengo los datos de la api
            df_continentes_correctos = pd.DataFrame()
            df_continentes_correctos['pais']=lista_paises_islas
            df_continentes_correctos[['continente_api','pais_api', 'latitud', 'longitud']]=(
                df_continentes_correctos.apply(lambda fila:pd.Series(
                    tr.obtener_pais_continente_lat_long(fila['pais'], API_KEY)), axis=1)
            )
            #sustituyo los valores dados en Inglés por los valores en Español
            df_continentes_correctos.continente_api= df_continentes_correctos.continente_api.replace({'North America':'América',
                                                            'South America': 'América',
                                                            'Africa': 'África',
                                                            'Europe': 'Europa',
                                                            'Oceania':'Oceanía'})
            ex.incorporar_información_df_original(df_continentes, df_continentes_correctos,
                                        'pais', 'continente_api', 'continente', 
                                        df_continentes.continente.isin(['Caribe', 'Oriente Medio', 'Islas Exóticas']))
            df_continentes_correctos['ciudad']=None
            df_continentes_agrupado_geolocalizacion_api = pd.concat([df_continentes_correctos_anterior,df_continentes_correctos],axis=0) #uno los 2 DF de la api de geolocalizacion
        else:
            df_continentes_agrupado_geolocalizacion_api=df_continentes_correctos_anterior
        df_continentes_agrupado_geolocalizacion_api.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE) #guardo los datos de geolocalización
        print(f'Los datos de geolocalizacion han sido guardados: {len(df_continentes_agrupado_geolocalizacion_api)}')
    
    ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS=os.getenv("ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS") #guardo el fichero resultante
    df_continentes.to_pickle(ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS)
    print(f'Los datos de los continentes escrapeados procesados han sido guardados: {len(df_continentes)}')
    return df_continentes_agrupado_geolocalizacion_api, df_continentes

def limpieza_continentes_escrapeados(df_continentes,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY):
    df_continentes_reducido = df_continentes[['continente', 'pais']].drop_duplicates() #elimino duplicados
    #obtengo los paises/islas que están en continentes ficticios:
    lista_paises_islas = df_continentes_reducido[df_continentes_reducido.continente.isin(['Caribe', 'Oriente Medio', 'Islas Exóticas'])].pais.tolist()
    df_continentes_agrupado_geolocalizacion_api, df_continentes= tr.obtención_continentes_correctos(df_continentes, lista_paises_islas, ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY)
    print('La limpieza de los continentes escrapeados ha finalizado')
    return df_continentes_agrupado_geolocalizacion_api,df_continentes

def correccion_mismonombre_diferenteurl(df_viajes_agrupados):
    dict_cambios_url= {"nombre_viaje":[],'url_anterior':[], 'url_nueva':[], 'url_anterior_en_ultimo_escrapeo':[], 
                   'url_nueva_en_ultimo_escrapeo':[]}
    for viaje in df_viajes_agrupados.nombre_viaje.unique():
        df_filtrado = df_viajes_agrupados[df_viajes_agrupados.nombre_viaje == viaje] #creo un DF para cada viaje
        if len(df_filtrado.url_viaje.unique()) >1:
            url1= df_filtrado.url_viaje.unique()[-2]
            url2=df_filtrado.url_viaje.unique()[-1]
            if len(df_filtrado.en_ultimo_escrapeo.unique())>1 and len(df_filtrado.duracion_viaje.unique())==1 and (len(df_filtrado.itinerario.unique()))==1:
                en_ultimo_escrapeo_anterior = df_filtrado.en_ultimo_escrapeo.unique()[-2]
                en_ultimo_escrapeo_nuevo = df_filtrado.en_ultimo_escrapeo.unique()[-1]
                dict_cambios_url['nombre_viaje'].append(viaje)
                dict_cambios_url['url_anterior'].append(url1)
                dict_cambios_url['url_nueva'].append(url2)
                dict_cambios_url['url_anterior_en_ultimo_escrapeo'].append(en_ultimo_escrapeo_anterior)
                dict_cambios_url['url_nueva_en_ultimo_escrapeo'].append(en_ultimo_escrapeo_nuevo)
    df_cambios_url = pd.DataFrame(dict_cambios_url)
    print(f'Los viajes duplicados con urls diferentes han sido actualizados: {len(df_cambios_url)}')
    return dict_cambios_url,df_cambios_url  

def correccion_duplicados_mismaurl_diferente_nombre(df_viajes_agrupados):
    dict_cambios_nombre_viaje= {"url_viaje":[],'nombre_anterior':[], 'nombre_nuevo':[]}
    for url in df_viajes_agrupados.url_viaje.unique():
        df_filtrado = df_viajes_agrupados[df_viajes_agrupados.url_viaje == url]
        if len(df_filtrado.nombre_viaje.unique()) >1:
            #print(url)
            #print(len(df_filtrado.nombre_viaje.unique()))
            nombre1= df_filtrado.nombre_viaje.unique()[-2]
            nombre2=df_filtrado.nombre_viaje.unique()[-1]
            #print(nombre1)
            #print(nombre2)
            dict_cambios_nombre_viaje['url_viaje'].append(url)
            dict_cambios_nombre_viaje['nombre_anterior'].append(nombre1)
            dict_cambios_nombre_viaje['nombre_nuevo'].append(nombre2)
    df_cambios_nombre_viaje = pd.DataFrame(dict_cambios_nombre_viaje)
    print(f'Los viajes duplicados con nombres diferentes han sido actualizados: {len(df_cambios_nombre_viaje)}')
    return dict_cambios_nombre_viaje, df_cambios_nombre_viaje

def limpieza_viajes_finales (df_viajes, df_opciones):
    #elimino los viajes que tienen la palabra opcion ya que luego incorporaré las opciones como viaje
    indices_viajes_opciones = df_viajes[df_viajes.url_viaje.str.contains('opciones',na=False)].index.tolist()
    df_viajes=df_viajes.drop(indices_viajes_opciones)

    #creo un df similar para opciones
    df_viajes_opciones= df_opciones[['pais', 'nombre_viaje_opcion', 'duracion_viaje', 'itinerario', 'precio',
       'url_viaje_opcion', 'fecha_escrapeo', 'en_ultimo_escrapeo']]
    df_viajes_opciones = df_viajes_opciones.rename(columns={'nombre_viaje_opcion':'nombre_viaje','url_viaje_opcion':'url_viaje'})

    #agrupo los 2 df:
    df_viajes_agrupados = pd.concat([df_viajes,df_viajes_opciones],axis=0).reset_index(drop=True)

    #corrijo los viajes duplicados con mismo nombre y distinta url e incorporo la informacion al df de viajes agrupados
    dict_cambios_url,df_cambios_url = tr.correccion_mismonombre_diferenteurl(df_viajes_agrupados)
    ex.incorporar_información_df_original(df_viajes_agrupados, df_cambios_url,
                                      'nombre_viaje', 'url_nueva', 'url_viaje', 
                                      df_viajes_agrupados.url_viaje.isin(dict_cambios_url['url_anterior']))
    ex.incorporar_información_df_original(df_viajes_agrupados, df_cambios_url,
                                      'nombre_viaje', 'url_nueva_en_ultimo_escrapeo', 'en_ultimo_escrapeo', 
                                      df_viajes_agrupados.url_viaje.isin(dict_cambios_url['url_anterior']))
    
    #corrijo duplicados con misma url y diferente nombre df de viajes agrupados e incorporo la informacion al df de viajes agrupados
    dict_cambios_nombre_viaje, df_cambios_nombre_viaje = tr.correccion_duplicados_mismaurl_diferente_nombre(df_viajes_agrupados)
    ex.incorporar_información_df_original(df_viajes_agrupados, df_cambios_nombre_viaje,
                                      'url_viaje', 'nombre_nuevo', 'nombre_viaje', 
                                      df_viajes_agrupados.nombre_viaje.isin(dict_cambios_nombre_viaje['nombre_anterior']))

    #elimino los que tiene valores nulos en duracion_Viaje y precio, porque no tengo forma de obtenerlo
    #  ya que son viajes antiguos y las urls ya no están disponibles
    df_viajes_agrupados=df_viajes_agrupados.dropna().reset_index(drop=True)

    #Limpio la columna itinerario de cara a la futura extracción de las ciudades
    df_viajes_agrupados.itinerario = df_viajes_agrupados.itinerario.str.replace('.','')
    #reemplazo los paréntesis y las ' y ' y ' e ' por una coma para dividir todas las ciudades:
    df_viajes_agrupados['itinerario_modificado_para_dividir'] = df_viajes_agrupados.itinerario.str.replace(r'[\(\)]| y | e ', ' , ', regex=True)

    #guardo el df resultante, con todos los viajes:
    ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS=os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS')
    df_viajes_agrupados.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS)
    print(f"Se ha guardado el fichero de viajes totales: {len(df_viajes_agrupados)}")
    return df_viajes_agrupados

def conversion_itineario_en_lista (valor):
    valor = valor.split(',')  # convertir string en lista, separando por comas
    valor = [v.strip() for v in valor]  # quitar espacios extra
    return valor

def asignacion_pais_itinerarios_con_un_pais(df_itinerarios):
    dict_pais_correcto_itinerario_un_pais = {'itinerario_modificado_para_dividir':[],'lista_itinerario':[], 'pais_correcto':[]}
    for itinerario_modificado in df_itinerarios.itinerario_modificado_para_dividir.unique().tolist():
        #print(itinerario)
        df_filtrado = df_itinerarios[df_itinerarios.itinerario_modificado_para_dividir == itinerario_modificado]
        #display(df_filtrado)
        if len(df_filtrado.pais.unique())==1:
            lista_itinerario = df_filtrado['lista_itinerario'].tolist()[0]
            dict_pais_correcto_itinerario_un_pais['itinerario_modificado_para_dividir'].append(itinerario_modificado)
            dict_pais_correcto_itinerario_un_pais['lista_itinerario'].append(lista_itinerario)
            dict_pais_correcto_itinerario_un_pais['pais_correcto'].append(df_filtrado.pais.unique()[0])
    df_pais_correcto_itinerario_un_pais=pd.DataFrame(dict_pais_correcto_itinerario_un_pais)
    return df_pais_correcto_itinerario_un_pais

def division_ciudades (df_pais_correcto_itinerario_a_dividir):
    #divido las ciudades para asignar el pais a las ciudades:
    df_pais_correcto_itinerario_ciudad_dividida= df_pais_correcto_itinerario_a_dividir.explode('lista_itinerario') # Explode para deshacer las listas en filas
    df_pais_correcto_itinerario_ciudad_dividida = df_pais_correcto_itinerario_ciudad_dividida.rename(columns={'lista_itinerario': 'ciudad'}) # Renombro la columna a ciudad
    df_pais_correcto_itinerario_ciudad_dividida['ciudad'] = df_pais_correcto_itinerario_ciudad_dividida['ciudad'].str.replace(r'\s+', ' ', regex=True).str.strip() #eliminar espacios de delante y duplicados entre dos palabras
    return df_pais_correcto_itinerario_ciudad_dividida

def asignacion_pais_itinerarios_duplicados_en_paises (df_itinerarios_duplicados_en_paises,
                                                      API_KEY,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE):
    df_itinerarios_duplicados_en_paises=df_itinerarios_duplicados_en_paises.reset_index(drop=True)
    lista_ciudades_sin_paises = df_itinerarios_duplicados_en_paises[pd.isnull(df_itinerarios_duplicados_en_paises.pais_correcto)].ciudad.unique().tolist()
    if len(lista_ciudades_sin_paises)>0:
        #obtengo los datos ya obtenidos de la api de geolocalización
        if os.path.exists(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE):
            df_geolocalizacion_correcta_anterior= pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE).reset_index(drop=True)
            df_geolocalizacion_correcta_anterior_filtrado= df_geolocalizacion_correcta_anterior[df_geolocalizacion_correcta_anterior['ciudad'].notna()]
            #incorporo la información en la tabla continentes original:
            ex.incorporar_información_df_original(df_itinerarios_duplicados_en_paises, df_geolocalizacion_correcta_anterior_filtrado,
                                            'ciudad', 'pais_api', 'pais_correcto',
                                            pd.isnull(df_itinerarios_duplicados_en_paises.pais_correcto))
            lista_ciudades_sin_paises= df_itinerarios_duplicados_en_paises[pd.isnull(df_itinerarios_duplicados_en_paises.pais_correcto)].ciudad.unique().tolist()
        if len(lista_ciudades_sin_paises)>0:
            #obtengo los datos de la api
            df_paises_correctos = pd.DataFrame()
            df_paises_correctos['ciudad']=lista_ciudades_sin_paises
            df_paises_correctos[['continente_api','pais_api', 'latitud', 'longitud']]=(
                df_paises_correctos.apply(lambda fila:pd.Series(
                    tr.obtener_pais_continente_lat_long(fila['ciudad'], API_KEY)), axis=1))
            #sustituyo los valores dados en Inglés por los valores en Español
            df_paises_correctos.continente_api= df_paises_correctos.continente_api.replace({'North America':'América',
                                                            'South America': 'América',
                                                            'Africa': 'África',
                                                            'Europe': 'Europa',
                                                            'Oceania':'Oceanía'})
            ex.incorporar_información_df_original(df_itinerarios_duplicados_en_paises, df_paises_correctos,
                                            'ciudad', 'pais_api', 'pais_correcto', 
                                            pd.isnull(df_itinerarios_duplicados_en_paises.pais_correcto))
            df_agrupado_geolocalizacion_api = pd.concat([df_geolocalizacion_correcta_anterior,df_paises_correctos],axis=0) #uno los 2 DF de la api de geolocalizacion
        else:
            df_agrupado_geolocalizacion_api=df_geolocalizacion_correcta_anterior
        df_agrupado_geolocalizacion_api.to_pickle(ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE)
        df_itinerarios_duplicados_en_paises = df_itinerarios_duplicados_en_paises[['itinerario_modificado_para_dividir','ciudad','pais_correcto']]
        return df_itinerarios_duplicados_en_paises, df_agrupado_geolocalizacion_api

def desglosar_ciudades_itinerarios (df_viajes_agrupados,df_agrupado_geolocalizacion_api,API_KEY,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE ):
    df_itinerarios = df_viajes_agrupados[['pais','itinerario_modificado_para_dividir']].drop_duplicates().reset_index(drop=True) #elimino duplicados
    df_itinerarios['lista_itinerario']= df_itinerarios.itinerario_modificado_para_dividir.apply(tr.conversion_itineario_en_lista) #separo las ciudades
    df_pais_correcto_itinerario_un_pais= tr.asignacion_pais_itinerarios_con_un_pais(df_itinerarios) #asignación pais a la ciudad
    df_pais_correcto_itinerario_ciudad_dividida_un_pais=tr.division_ciudades(df_pais_correcto_itinerario_un_pais) #division de los itinerarios en ciudades
    print(f'Se ha asignado el pais a las ciudades que están en itinerarios asociados a un pais único:{len(df_pais_correcto_itinerario_ciudad_dividida_un_pais)}')

    df_itinerarios_duplicados_en_paises= df_itinerarios[
    ~df_itinerarios.itinerario_modificado_para_dividir.isin(df_pais_correcto_itinerario_un_pais.itinerario_modificado_para_dividir.tolist())]

    df_itinerarios_duplicados_en_paises_dividido=tr.division_ciudades(df_itinerarios_duplicados_en_paises) #division de los itinerarios en ciudades
    df_itinerarios_duplicados_en_paises_dividido['pais_correcto'] = np.nan
    ex.incorporar_información_df_original(df_itinerarios_duplicados_en_paises_dividido, df_pais_correcto_itinerario_ciudad_dividida_un_pais,'ciudad','pais_correcto', 'pais_correcto', )
    df_itinerarios_duplicados_en_paises_dividido_completo, df_agrupado_geolocalizacion_api= tr.asignacion_pais_itinerarios_duplicados_en_paises(df_itinerarios_duplicados_en_paises_dividido,
                                                      API_KEY,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE)
    print(f'Se ha asignado el pais a las ciudades que están en itinerarios duplicados: {len(df_itinerarios_duplicados_en_paises_dividido_completo)}')
    
    df_itinerario_ciudades_completo = pd.concat([df_pais_correcto_itinerario_ciudad_dividida_un_pais,df_itinerarios_duplicados_en_paises_dividido_completo], axis=0)

    #Reemplazo los valores que al unirlo con el DF de escrapeo da lugar a duplicados:
    df_itinerario_ciudades_completo.pais_correcto= df_itinerario_ciudades_completo.pais_correcto.replace(
    {'Estados Unidos de América':'Estados Unidos'})


    ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS')
    df_itinerario_ciudades_completo.to_pickle(ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS)
    print(f'Se ha guardado la informacion de itinerarios y ciudades:{len(df_itinerario_ciudades_completo)} ')
    return df_itinerario_ciudades_completo



def desglosar_ciudades_itinerarios_2 (df_viajes_agrupados,API_KEY,ARCHIVO_GUARDAR_TOTAL_CIUDADES_API, ARCHIVO_EXTRACCION_API_DUPLICADOS):
    df_itinerarios = df_viajes_agrupados[['pais','itinerario_modificado_para_dividir']].drop_duplicates().reset_index(drop=True) #elimino duplicados
    df_itinerarios['lista_itinerario']= df_itinerarios.itinerario_modificado_para_dividir.apply(tr.conversion_itineario_en_lista) #separo las ciudades
    df_itinerarios_ciudades = df_itinerarios.explode('lista_itinerario').reset_index(drop=True) #divido el DF por ciudades
    df_itinerarios_ciudades=df_itinerarios_ciudades.rename(columns={'lista_itinerario':'ciudad'}) #renombro la columna a ciudad

    #normalizo y capitalizo la columna ciudad:
    df_itinerarios_ciudades['ciudad'] = df_itinerarios_ciudades['ciudad'].map(tr.normalizar_texto)
    df_itinerarios_ciudades['ciudad'] = df_itinerarios_ciudades['ciudad'].map(tr.capitalizar_texto)

    indices_sin_ciudades = df_itinerarios_ciudades[df_itinerarios_ciudades.ciudad == ''].index #creo una lista con los indices de las filas sin ciudad
    #elimino las filas sin ciudad:
    df_itinerarios_ciudades= df_itinerarios_ciudades.drop(indices_sin_ciudades).reset_index(drop=True)
    #obtengo los datos de la api extraidos anteriormente
    df_paises_api_todas_ciudades= pd.read_pickle(ARCHIVO_GUARDAR_TOTAL_CIUDADES_API)# '../data/data_raw/geolocalizacion_api_total_ciudades.pkl'

    #normalizo y capitalizo la columna ciudad
    df_paises_api_todas_ciudades['ciudad'] = df_paises_api_todas_ciudades['ciudad'].map(tr.normalizar_texto)
    df_paises_api_todas_ciudades['ciudad'] = df_paises_api_todas_ciudades['ciudad'].map(tr.capitalizar_texto)

    df_itinerarios_ciudades['pais_api']=None #creo la columna pais_api para incorporar la información de la api
    ex.incorporar_información_df_original(df_itinerarios_ciudades,df_paises_api_todas_ciudades,'ciudad', 'pais_api', 'pais_api')
    df_itinerarios_ciudades_sin_lista = df_itinerarios_ciudades[['pais', 'ciudad', 'pais_api']] # me quedo solo con pais, ciudad y pais api
    df_itinerarios_ciudades_sin_lista=df_itinerarios_ciudades_sin_lista.drop_duplicates().reset_index(drop=True) #elimino duplicados

    #Normalizo la columna paises:
    df_itinerarios_ciudades['pais'] = df_itinerarios_ciudades['pais'].map(tr.normalizar_texto)
    df_itinerarios_ciudades['pais'] = df_itinerarios_ciudades['pais'].map(tr.capitalizar_texto)
    df_itinerarios_ciudades['pais_api'] = df_itinerarios_ciudades['pais_api'].map(tr.normalizar_texto)
    df_itinerarios_ciudades['pais_api'] = df_itinerarios_ciudades['pais_api'].map(tr.capitalizar_texto)

    #comparo si el pais de la api coincide con el pais del viaje:
    df_itinerarios_ciudades_sin_lista['resultado']= None
    for i,row in df_itinerarios_ciudades_sin_lista.iterrows():
        if row['pais'] == row["pais_api"]:
            df_itinerarios_ciudades_sin_lista.loc[i,'resultado'] ='Pais correcto'
        else:
            df_itinerarios_ciudades_sin_lista.loc[i,'resultado'] ='Pais no coincide'

    #creo DF con los paises correctos  (pais viaje = pais api):
    df_itinerarios_ciudades_sin_lista_paises_correctos = df_itinerarios_ciudades_sin_lista[df_itinerarios_ciudades_sin_lista.resultado=='Pais correcto']
    
    print(f'{len(df_itinerarios_ciudades_sin_lista_paises_correctos)} paises correctos')
    #creo una lista de ciudades con paises correctas para añadir la información al DF
    lista_ciudades_pais_correcto = df_itinerarios_ciudades_sin_lista_paises_correctos['ciudad'].unique().tolist()
 

    df_itinerarios_ciudades_sin_lista['resultado_2']= None
    for i,row in df_itinerarios_ciudades_sin_lista.iterrows():
        if row['ciudad'] in lista_ciudades_pais_correcto:
            df_itinerarios_ciudades_sin_lista.loc[i,'resultado_2'] ='Pais_correcto_identificado'
        else:
            df_itinerarios_ciudades_sin_lista.loc[i,'resultado_2'] ='Pais a identificar por API'

    #incorporo la informacion de los paises correctos al df de ciudades paises inicial
    df_itinerarios_ciudades_sin_lista['pais_final']=None #creo la columna con el pais definitivo
    ex.incorporar_información_df_original(df_itinerarios_ciudades_sin_lista,df_itinerarios_ciudades_sin_lista_paises_correctos,
                                          'ciudad','pais','pais_final',df_itinerarios_ciudades_sin_lista.resultado_2=='Pais_correcto_identificado')
    print(f'Se ha asignado el pais a las ciudades cuyo pais de la api de la ciudad coincide con el pais del viaje: {len(lista_ciudades_pais_correcto)}')


    #me quedo con la parte del DF que hay que identificar por API:
    df_itinerarios_a_identificar_por_api =df_itinerarios_ciudades_sin_lista[df_itinerarios_ciudades_sin_lista.resultado_2=='Pais a identificar por API']

    #importo el df con los datos extraidos de la api anteriormente:
    df_paises_api_ciudades_duplicadas= pd.read_pickle(ARCHIVO_EXTRACCION_API_DUPLICADOS)
    
    ex.incorporar_información_df_original(df_itinerarios_ciudades_sin_lista, df_paises_api_ciudades_duplicadas,
                                            'ciudad', 'pais_api', 'pais_final', 
                                            df_itinerarios_ciudades_sin_lista.resultado_2=='Pais a identificar por API')
    print(f'Se ha asignado el pais a las ciudades cuyo pais de la api de la ciudad no coincide con el pais del viaje: {len(df_paises_api_ciudades_duplicadas)}')


    #me quedo con la parte del DF que hay que identificar por API final:
    df_itinerarios_a_identificar_por_api = df_itinerarios_ciudades_sin_lista[pd.isnull(df_itinerarios_ciudades_sin_lista.pais_final)] #COMPROBADO HASTA AQUI
    warnings.simplefilter(action='ignore', category=SettingWithCopyWarning)
    #Extraigo los datos de la API:
    if len(df_itinerarios_a_identificar_por_api)>0:
        #print(len(df_itinerarios_a_identificar_por_api))
        #df_itinerarios_ciudades_sin_duplicados = pd.DataFrame()
        df_itinerarios_ciudades_sin_duplicados=df_itinerarios_a_identificar_por_api[['pais','ciudad']].copy()
        df_itinerarios_ciudades_sin_duplicados= df_itinerarios_ciudades_sin_duplicados.drop_duplicates()
        #columnas_destino = ['continente_api','pais_api', 'latitud', 'longitud','resultado']
        #for col in columnas_destino:
            #df_itinerarios_ciudades_sin_duplicados[col] = None

        #df_itinerarios_ciudades_sin_duplicados.loc[:,columnas_destino]=(
        resultados_api = df_itinerarios_ciudades_sin_duplicados.apply(
                lambda fila:pd.Series(
                    tr.obtener_pais_continente_lat_long_con_pais(fila['ciudad'],fila['pais'], API_KEY),
                    index=['continente_api','pais_api', 'latitud', 'longitud','resultado']),
                    axis=1)
        
        # Concatenar los resultados al DataFrame original
        df_itinerarios_ciudades_sin_duplicados = pd.concat([df_itinerarios_ciudades_sin_duplicados, resultados_api], axis=1)

        print(f'Se ha extraido la informacion de: {len(df_itinerarios_ciudades_sin_duplicados)} ciudades de la API')



        #normalizo y capitalizo el texto
        for col in df_itinerarios_ciudades_sin_duplicados.columns:
            df_itinerarios_ciudades_sin_duplicados[col] = df_itinerarios_ciudades_sin_duplicados[col].map(tr.normalizar_texto)
            df_itinerarios_ciudades_sin_duplicados[col] = df_itinerarios_ciudades_sin_duplicados[col].map(tr.capitalizar_texto)

        df_itinerarios_a_identificar_por_api_nuevo = pd.concat([df_paises_api_ciudades_duplicadas,df_itinerarios_ciudades_sin_duplicados], axis=0)
        df_itinerarios_a_identificar_por_api_nuevo.to_pickle(ARCHIVO_EXTRACCION_API_DUPLICADOS)

        ex.incorporar_información_df_original(df_itinerarios_ciudades_sin_lista,
                                             df_itinerarios_ciudades_sin_duplicados,'ciudad','pais_api','pais_final',
                                        pd.isnull(df_itinerarios_ciudades_sin_lista.pais_final))
        print(f'Se ha asignado el pais a las ciudades con la información extraida de la API:{len(df_itinerarios_ciudades_sin_duplicados)}')

    #Reemplazo los valores que al unirlo con el DF de escrapeo da lugar a duplicados:
    df_itinerarios_ciudades_sin_lista.pais_final= df_itinerarios_ciudades_sin_lista.pais_final.replace(
            {'Estados Unidos de América':'Estados Unidos',
             'Spain': 'España',
             'Arabia Saudita': 'Arabia Saudí',
            'Fiyi': 'Fiji',
            'Francia': 'France',
            'Islas Turcas y Caicos': 'Turks and Caicos',
            'Papua-Nueva Guinea': 'papua nueva guinea',
            'Zimbawe': 'Zimbabue',
            'Chipre Del Norte': 'Chipre',
            'Chipre del Norte / Chipre': 'Chipre'})

    #incorporo la información al df_itinerarios_original
    df_itinerarios_ciudades_completo = df_itinerarios_ciudades[['itinerario_modificado_para_dividir', 'ciudad']]
    df_itinerarios_ciudades_completo['pais_correcto']= None
    ex.incorporar_información_df_original(df_itinerarios_ciudades_completo, df_itinerarios_ciudades_sin_lista,
                                           'ciudad','pais_final','pais_correcto')
    print(f'Se ha informado el pais correcto a cada ciudad de cada itinerario:{len(df_itinerarios_ciudades_completo)}')


    ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')
    df_itinerarios_ciudades_sin_lista.to_pickle(ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1)
    print(f'Se ha guardado la informacion de itinerarios y ciudades:{len(df_itinerarios_ciudades_completo)} ')
    return df_itinerarios_ciudades_completo
  

def transformacion_total():
    load_dotenv()

    #importo los df
    RUTA_SERVICE = os.getenv("RUTA_SERVICE")
    ARCHIVO_GUARDAR_ESCRAPEO_VIAJES=os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES')
    ARCHIVO_GUARDAR_CONTINENTES = os.getenv('ARCHIVO_GUARDAR_CONTINENTES')
    ARCHIVO_GUARDAR_OPCIONES_VIAJES =os.getenv('ARCHIVO_GUARDAR_OPCIONES_VIAJES')
    ARCHIVO_GUARDAR_DATOS_API=os.getenv('ARCHIVO_GUARDAR_DATOS_API')
    df_continentes = pd.read_pickle(ARCHIVO_GUARDAR_CONTINENTES)
    df_viajes= pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES)
    df_opciones = pd.read_pickle(ARCHIVO_GUARDAR_OPCIONES_VIAJES)
    df_turismo_emisor = pd.read_csv(ARCHIVO_GUARDAR_DATOS_API,delimiter=';',encoding='latin1')

    df_turismo_emisor_procesado= tr.limpieza_fichero_turismo_emisor(df_turismo_emisor) #limpieza fichero turismo emisor

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE=os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE')

    API_KEY= os.getenv("API_KEY")

    df_continentes_agrupado_geolocalizacion_api,df_continentes_procesado= tr.limpieza_continentes_escrapeados(
        df_continentes,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY) #limpieza continentes escrapeados
    
    df_viajes_agrupados= tr.limpieza_viajes_finales(df_viajes, df_opciones)

    df_itinerario_ciudades_completo = tr.desglosar_ciudades_itinerarios(df_viajes_agrupados,df_continentes_agrupado_geolocalizacion_api,API_KEY,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE )

    print('LA TRANSFORMACION HA FINALIZADO')
    return df_turismo_emisor_procesado, df_continentes_procesado, df_viajes_agrupados, df_itinerario_ciudades_completo

    

def transformacion_total_1():
    load_dotenv()

    #importo los df
    RUTA_SERVICE = os.getenv("RUTA_SERVICE")
    ARCHIVO_GUARDAR_ESCRAPEO_VIAJES=os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES')
    ARCHIVO_GUARDAR_CONTINENTES = os.getenv('ARCHIVO_GUARDAR_CONTINENTES')
    ARCHIVO_GUARDAR_OPCIONES_VIAJES =os.getenv('ARCHIVO_GUARDAR_OPCIONES_VIAJES')
    ARCHIVO_GUARDAR_DATOS_API=os.getenv('ARCHIVO_GUARDAR_DATOS_API')
    df_continentes = pd.read_pickle(ARCHIVO_GUARDAR_CONTINENTES)
    df_viajes= pd.read_pickle(ARCHIVO_GUARDAR_ESCRAPEO_VIAJES)
    df_opciones = pd.read_pickle(ARCHIVO_GUARDAR_OPCIONES_VIAJES)
    df_turismo_emisor = pd.read_csv(ARCHIVO_GUARDAR_DATOS_API,delimiter=';',encoding='latin1')

    df_turismo_emisor_procesado= tr.limpieza_fichero_turismo_emisor(df_turismo_emisor) #limpieza fichero turismo emisor

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE=os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE')

    API_KEY= os.getenv("API_KEY")

    df_continentes_agrupado_geolocalizacion_api,df_continentes_procesado= tr.limpieza_continentes_escrapeados(
        df_continentes,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY) #limpieza continentes escrapeados
    
    df_viajes_agrupados= tr.limpieza_viajes_finales(df_viajes, df_opciones)

    ARCHIVO_GUARDAR_TOTAL_CIUDADES_API = os.getenv('ARCHIVO_GUARDAR_TOTAL_CIUDADES_API')
    ARCHIVO_EXTRACCION_API_DUPLICADOS = os.getenv('ARCHIVO_EXTRACCION_API_DUPLICADOS')

    df_itinerario_ciudades_completo = tr.desglosar_ciudades_itinerarios_2(df_viajes_agrupados,API_KEY,ARCHIVO_GUARDAR_TOTAL_CIUDADES_API, ARCHIVO_EXTRACCION_API_DUPLICADOS )


    print('LA TRANSFORMACION HA FINALIZADO')
    return df_turismo_emisor_procesado, df_continentes_procesado, df_viajes_agrupados, df_itinerario_ciudades_completo





