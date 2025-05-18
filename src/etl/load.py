import pandas as pd
import numpy as np
import psycopg2
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import os
from dotenv import load_dotenv
#from src.etl import transform as tr #con jupyter
import transform as tr #con main.py
#from src.etl import load as lo #con jupyter
import load as lo #con main.py

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

def normalizar_capitalizar_columna (df,columna):
    df[columna] = df[columna].map(tr.normalizar_texto)
    df[columna] = df[columna].map(tr.capitalizar_texto)
    return df

def extraccion_unicos_pais_continente_turismo_emisor(archivo_guardar_datos_api_procesados = os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS')):
    df_turismo_emisor_procesado=pd.read_csv(archivo_guardar_datos_api_procesados) #importe el fichero procesado
    #me quedon con las columnas que me interesan y elimino duplicados:
    df_paises_continentes_turismos_emisor=df_turismo_emisor_procesado[['PAIS_DESTINO','CONTINENTE_DESTINO']]
    df_paises_continentes_turismos_emisor=df_paises_continentes_turismos_emisor.drop_duplicates()

    #renombro las columnas: 
    df_paises_continentes_turismos_emisor= df_paises_continentes_turismos_emisor.rename(
    columns={'PAIS_DESTINO':'nombre_pais_destino', 'CONTINENTE_DESTINO':'nombre_continente'}
    ).reset_index(drop=True)

    #normalizo y capitalizo la columna nombre_pais_destino:
    df_paises_continentes_turismos_emisor = lo.normalizar_capitalizar_columna(df_paises_continentes_turismos_emisor, 'nombre_pais_destino')
    return df_paises_continentes_turismos_emisor

def extraccion_unicos_paises_escrapeados(archivo_guardar_continentes_procesados = os.getenv('ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS')):
    df_paises_continentes_escrapeo = pd.read_pickle(archivo_guardar_continentes_procesados)
    
    #me quedon con las columnas que me interesan y elimino duplicados:
    df_paises_continentes_escrapeo=df_paises_continentes_escrapeo[['pais','continente']]
    df_paises_continentes_escrapeo= df_paises_continentes_escrapeo.drop_duplicates()

    #renombro las columnas y ordeno: 
    df_paises_continentes_escrapeo= df_paises_continentes_escrapeo.rename(
    columns={'pais':'nombre_pais_destino', 'continente':'nombre_continente'}
    ).reset_index(drop=True)

    #normalizo y capitalizo la columna nombre_pais_destino:
    df_paises_continentes_escrapeo = lo.normalizar_capitalizar_columna(df_paises_continentes_escrapeo, 'nombre_pais_destino')
    return df_paises_continentes_escrapeo

def extraccion_unicos_paises_itinerarios(archivo_guardar_itinerarios_procesados_1 = os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')):
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)

    #me quedo solo con los datos que me interesan
    df_itinerarios_ciudades_pais_continentes = pd.DataFrame()
    df_itinerarios_ciudades_pais_continentes['pais'] = df_itinerarios_ciudades['pais_correcto'].drop_duplicates().reset_index(drop=True)

    #normalizo y capitalizo la columna pais:
    df_itinerarios_ciudades_pais_continentes = lo.normalizar_capitalizar_columna(df_itinerarios_ciudades_pais_continentes, 'pais')

    return df_itinerarios_ciudades_pais_continentes

def extraccion_informacion_obtenida_api_opencage(archivo_guardar_total_ciudades_api = os.getenv('ARCHIVO_GUARDAR_TOTAL_CIUDADES_API'),
                                                                                               archivo_extraccion_api_duplicados = os.getenv('ARCHIVO_EXTRACCION_API_DUPLICADOS')):
    #importo el DF de la geolocalizacion de ciudades obtenidas de la api de opencage:
    df_opencage_ciudades_totales = pd.read_pickle(archivo_guardar_total_ciudades_api)

    #sustituyo los valores dados en Inglés por los valores en Español en la columna continente_api:
    df_opencage_ciudades_totales.continente_api= df_opencage_ciudades_totales.continente_api.replace({'North America':'América',
                                                    'South America': 'América',
                                                    'Africa': 'África',
                                                    'Europe': 'Europa',
                                                    'Oceania':'Oceanía'})
    
    #normalizo y capitalizo la columna pais_api:
    df_opencage_ciudades_totales = lo.normalizar_capitalizar_columna(df_opencage_ciudades_totales, 'pais_api')

    #importo el df con los paies duplicados en diferentes continentes:
    df_paises_api_duplicados = pd.read_pickle(archivo_extraccion_api_duplicados)
    df_paises_api_duplicados.continente_api= df_paises_api_duplicados.continente_api.replace({'North America':'América',
                                                    'South America': 'América',
                                                    'Africa': 'África',
                                                    'Europe': 'Europa',
                                                    'Oceania':'Oceanía'})
    return df_opencage_ciudades_totales,df_paises_api_duplicados
    

def obtencion_paises_continentes_no_incluidos_turismo_emisor_escrapeo(df_paises_continentes_turismos_emisor, df_paises_continentes_escrapeo,df_itinerarios_ciudades_pais_continentes,df_opencage_ciudades_totales,df_paises_api_duplicados):
    dict_paises_continentes_ciudades = {'pais':[], 'continente':[]}
    for pais in df_itinerarios_ciudades_pais_continentes.pais:
        if pais in df_paises_continentes_escrapeo.nombre_pais_destino.tolist():
            pass
        else:
            if pais in df_paises_continentes_turismos_emisor.nombre_pais_destino.tolist():
                pass
            else:
                if pais in df_opencage_ciudades_totales.pais_api.tolist():
                    #print(f'{pais} ya está en continentes opencage')
                    dict_paises_continentes_ciudades['pais'].append(pais)
                    continente = (df_opencage_ciudades_totales[df_opencage_ciudades_totales.pais_api == pais]).continente_api.unique()[0]
                    dict_paises_continentes_ciudades['continente'].append(continente)
                else:
                        if pais in df_paises_api_duplicados.pais_api.tolist():
                            #print(f'{pais} ya está en continentes opencage')
                            dict_paises_continentes_ciudades['pais'].append(pais)
                            continente = (df_paises_api_duplicados[df_paises_api_duplicados.pais_api == pais]).continente_api.unique()[0]
                            dict_paises_continentes_ciudades['continente'].append(continente)
                        else:
                            pass
        df_paises_continentes_ciudades = pd.DataFrame(dict_paises_continentes_ciudades)
        df_paises_continentes_ciudades

        #renombro las columnas:
        df_paises_continentes_ciudades= df_paises_continentes_ciudades.rename(
        columns={'pais':'nombre_pais_destino', 'continente':'nombre_continente'})

        return df_paises_continentes_ciudades

def obtencion_paises_continentes_unicos (df_paises_continentes_turismos_emisor,df_paises_continentes_escrapeo,df_paises_continentes_ciudades):
    df_paises_continentes_totales=pd.concat(
        [df_paises_continentes_turismos_emisor,df_paises_continentes_escrapeo, df_paises_continentes_ciudades], axis=0)
    df_paises_continentes_totales = df_paises_continentes_totales.drop_duplicates(['nombre_pais_destino', 'nombre_continente']).reset_index(drop=True)
    df_paises_continentes_totales= df_paises_continentes_totales.sort_values(by='nombre_pais_destino').reset_index(drop=True)

    #obtengo los paises duplicados en diferentes continentes:
    df_contador_paises_duplicados = pd.DataFrame(df_paises_continentes_totales.nombre_pais_destino.value_counts()).reset_index()
    df_contador_paises_duplicados =df_contador_paises_duplicados[df_contador_paises_duplicados['count']>1]
    lista_paises_duplicados = df_contador_paises_duplicados.nombre_pais_destino.tolist()

    #Le asigno el pais correcto y elimino los duplicados:
    for indice,pais in enumerate(df_paises_continentes_totales.nombre_pais_destino):
        if pais in lista_paises_duplicados:
            df_paises_continentes_totales.loc[indice, "nombre_continente"]  = 'Asia'

    df_paises_continentes_totales = df_paises_continentes_totales.drop_duplicates()
    return df_paises_continentes_totales

def obtencion_tabla_paises_continentes_unicos():
    df_paises_continentes_turismos_emisor= lo.extraccion_unicos_pais_continente_turismo_emisor(archivo_guardar_datos_api_procesados = os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS'))
    df_paises_continentes_escrapeo = lo.extraccion_unicos_paises_escrapeados(archivo_guardar_continentes_procesados = os.getenv('ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS'))
    df_itinerarios_ciudades_pais_continentes = lo.extraccion_unicos_paises_itinerarios(archivo_guardar_itinerarios_procesados_1 = os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'))
    df_opencage_ciudades_totales,df_paises_api_duplicados=lo.extraccion_informacion_obtenida_api_opencage(archivo_guardar_total_ciudades_api = os.getenv('ARCHIVO_GUARDAR_TOTAL_CIUDADES_API'),
                                                                                               archivo_extraccion_api_duplicados = os.getenv('ARCHIVO_EXTRACCION_API_DUPLICADOS'))
    df_paises_continentes_ciudades = lo.obtencion_paises_continentes_no_incluidos_turismo_emisor_escrapeo(df_paises_continentes_turismos_emisor, 
                                                                                                          df_paises_continentes_escrapeo,df_itinerarios_ciudades_pais_continentes,df_opencage_ciudades_totales,df_paises_api_duplicados)
    df_paises_continentes_totales= lo.obtencion_paises_continentes_unicos (df_paises_continentes_turismos_emisor,df_paises_continentes_escrapeo,df_paises_continentes_ciudades)
    print(f'Existen {len(df_paises_continentes_totales)} paises únicos')
    return df_paises_continentes_totales

def carga_tabla_pais_destino(df_paises_continentes_totales):
    #Extraigo la información de la BBDD
    query_extraccion = "SELECT nombre_pais_destino, id_pais_destino FROM pais_destino"
    pais_destino_dict=lo.extraer_datos_de_BBDD(query_extraccion)
    print(f' Existen {len(pais_destino_dict)} registros en la tabla pais_destino')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_paises_continentes_totales.iterrows():
        nombre_pais_destino = row["nombre_pais_destino"]
        if nombre_pais_destino in  pais_destino_dict.keys():
            pass
        else: 
            nombre_continente = row["nombre_continente"]
            data_to_insert.append([nombre_pais_destino, nombre_continente])
    
    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO pais_destino(nombre_pais_destino, nombre_continente)
    VALUES (%s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query, data_to_insert)

    print(f'Se han añadido {len(data_to_insert)} paises en la tabla pais_destino')

def carga_tabla_itinerario (archivo_guardar_itinerarios_procesados_1=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')):
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)

    #limpio los espacios entre las comas que sobran
    df_itinerarios_ciudades['itinerario_modificado_para_dividir'] = df_itinerarios_ciudades['itinerario_modificado_para_dividir'].apply(lo.limpiar_texto)

    #obtengo la lista de itinerarios únicos:
    lista_itinerarios_unicos = df_itinerarios_ciudades.itinerario_modificado_para_dividir.sort_values().unique()
    print(f'Existe {len(lista_itinerarios_unicos)} itinerarios únicos')

    #CARGA BBDD:

    #Extraigo la información de la BBDD
    query_extraccion = "SELECT detalle_itinerario, id_itinerario FROM itinerario"
    itinerario_dict=lo.extraer_datos_de_BBDD(query_extraccion)
    print(f' Existen {len(itinerario_dict)} registros en la tabla itinerario')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for itinerario in lista_itinerarios_unicos:
        if itinerario in itinerario_dict.keys():
            pass
        else: 
            data_to_insert.append((itinerario,)) 

    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO itinerario(detalle_itinerario)
    VALUES (%s)
    """
    lo.insertar_datos_en_BBDD(insert_query, data_to_insert)

    print(f'Se han añadido {len(data_to_insert)} itinerarios nuevos en la tabla pais_destino')


def carga_tabla_ciudad (archivo_guardar_itinerarios_procesados_1=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')):
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)

    #creo el DF con la ciudad y el pais para preparar los datos para la carga y elimino duplicados:
    df_ciudad_pais_unicos = df_itinerarios_ciudades[['ciudad','pais_correcto']].drop_duplicates().reset_index(drop=True)    
    print(f'Existen {len(df_ciudad_pais_unicos)} ciudades únicas')

    #CARGA BBDD:
    #Extraigo la información de la BBDD existente en la tabla Ciudad
    query_extraccion = "SELECT nombre_ciudad, id_ciudad FROM ciudad"
    ciudad_dict=lo.extraer_datos_de_BBDD(query_extraccion)
    print(f' Existen {len(ciudad_dict)} registros en la tabla ciudad')

    #Extraigo el id_pais_destino de la tabla pais_destino de la BBDD:
    query_extraccion = "SELECT nombre_pais_destino, id_pais_destino FROM pais_destino"
    pais_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_ciudad_pais_unicos.iterrows():
        nombre_ciudad = row["ciudad"]
        if nombre_ciudad in ciudad_dict.keys():
            pass
        else:
            pais_correcto = row["pais_correcto"]
            id_pais_destino = pais_dict.get(pais_correcto)
            data_to_insert.append([nombre_ciudad, id_pais_destino])

    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO ciudad (nombre_ciudad,id_pais_destino)
    VALUES (%s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query, data_to_insert)

    print(f'Se han añadido {len(data_to_insert)} ciudades nuevas en la tabla ciudad')

def carga_tabla_ciudad_itinerario(archivo_guardar_itinerarios_procesados_1=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')):
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)

    #limpio los espacios entre las comas que sobran
    df_itinerarios_ciudades['itinerario_modificado_para_dividir'] = df_itinerarios_ciudades['itinerario_modificado_para_dividir'].apply(lo.limpiar_texto)

    #normalizo y capitalizo la columna ciudad
    df_itinerarios_ciudades=lo.normalizar_capitalizar_columna(df_itinerarios_ciudades,'ciudad')

    #me quedo con los datos que me interesan y eliminos duplicados:
    df_itinerario_ciudad_unicos = df_itinerarios_ciudades[['itinerario_modificado_para_dividir','ciudad']].drop_duplicates().reset_index(drop=True)
    print(f' Existen {len(df_itinerario_ciudad_unicos)} combinaciones de itinerario_ciudad')


    #CARGA BBDD:
    #Extraigo la información de la BBDD existente en la tabla Ciudad
    query_extraccion = "SELECT nombre_ciudad, id_ciudad FROM ciudad"
    ciudad_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Extraigo la información de la BBDD
    query_extraccion = "SELECT detalle_itinerario, id_itinerario FROM itinerario"
    itinerario_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    # Obtengo todas las combinaciones existentes (id_ciudad, id_itinerario) de la tabla
    query_extraccion_tupla="SELECT id_ciudad, id_itinerario FROM ciudad_itinerario"
    existing_pairs=lo.extraer_tupla_datos_bbdd(query_extraccion_tupla)
    print(f' Existen {len(existing_pairs)} registros en la tabla ciudad_itinerario')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_itinerario_ciudad_unicos.iterrows():
        detalle_itinerario = row["itinerario_modificado_para_dividir"]
        nombre_ciudad = row["ciudad"]
        id_ciudad = ciudad_dict.get(nombre_ciudad)
        id_itinerario = itinerario_dict.get(detalle_itinerario)
        pair = (id_ciudad, id_itinerario) #creo la tupla de combinación de id_ciudad e id_itinerario
        if pair not in existing_pairs:
            data_to_insert.append([id_ciudad, id_itinerario])

    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO ciudad_itinerario (id_ciudad,id_itinerario)
    VALUES (%s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query,data_to_insert)
    print(f'Se han añadido {len(data_to_insert)} registros nuevos en la tabla ciudad_itinerario')

def carga_tabla_viaje (archivo_guardar_escrapeo_viajes_procesados = os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS')):
    #importo el fichero procesado de viajes
    df_viajes_total = pd.read_pickle(archivo_guardar_escrapeo_viajes_procesados)

    #limpio los datos de la columna itinerario_modificado_para_dividir
    df_viajes_total['itinerario_modificado_para_dividir'] = df_viajes_total['itinerario_modificado_para_dividir'].apply(lo.limpiar_texto)

    #me quedo con los viajes escrapeados la última vez para indicar si el viaje está aún activo o no:
    viajes_ultimo_escrapeo = df_viajes_total[df_viajes_total.fecha_escrapeo == max(df_viajes_total.fecha_escrapeo) ]
    lista_nombre_ultimo_escrapeo = viajes_ultimo_escrapeo.nombre_viaje.unique().tolist()
    lista_url_ultimo_escrapeo = viajes_ultimo_escrapeo.url_viaje.unique().tolist()

    #me quedo con las columnas que me interesan:
    df_viajes_unicos = df_viajes_total[[ 'url_viaje','nombre_viaje','duracion_viaje','itinerario_modificado_para_dividir','en_ultimo_escrapeo']]
    #elimino duplicados
    df_viajes_unicos = df_viajes_unicos.drop_duplicates('url_viaje').reset_index(drop=True)
    len(df_viajes_unicos)

    #actualizo la informacion de la disponibilidad del viaje
    for _,row in df_viajes_unicos.iterrows():
        nombre_viaje = row["nombre_viaje"]
        url_viaje = row["url_viaje"]
        if nombre_viaje in lista_nombre_ultimo_escrapeo or url_viaje in lista_url_ultimo_escrapeo:
            row['en_ultimo_escrapeo'] = 'Si'
        else:
            row['en_ultimo_escrapeo'] = 'No'

    #elimino duplicados
    df_viajes_unicos = df_viajes_unicos.drop_duplicates().reset_index(drop=True)

    #divido la columna duracion_viaje para extraer los días y las noches en diferentes columnas
    df_viajes_unicos[['duracion_dias','duracion_noches']]=df_viajes_unicos['duracion_viaje'].str.split(" / ", expand = True)
    df_viajes_unicos['duracion_dias']= (df_viajes_unicos['duracion_dias'].str.split(' ', expand = True)[0]).astype(int)
    df_viajes_unicos['duracion_noches']= (df_viajes_unicos['duracion_noches'].str.split(' ', expand = True)[0]).astype(int)

    #convierto la columna en_ultimo_escrapeo en booleana
    df_viajes_unicos = lo.convertir_si_no_a_boolean(df_viajes_unicos, 'en_ultimo_escrapeo')

    #elimino la columna duracion_viaje
    df_viajes_unicos= df_viajes_unicos.drop(columns='duracion_viaje')

    #elimino duplicados
    df_viajes_unicos = df_viajes_unicos.drop_duplicates().reset_index(drop=True)
    print(f'Existen {len(df_viajes_unicos)} viajes únicos')

    #CARGA BBDD:
    #Extraigo la información de la BBDD existente en la tabla viaje
    query_extraccion = "SELECT url_viaje, id_viaje FROM viaje"
    url_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Extraigo la información de la BBDD existente en la tabla itinerarios
    query_extraccion = "SELECT detalle_itinerario, id_itinerario FROM itinerario"
    itinerario_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #creo la tupla de combinación para no repetir viajes en las futuras cargas
    query_extraccion_tupla="SELECT url_viaje, nombre_viaje, viaje_activo FROM viaje"
    existing_tupla = lo.extraer_tupla_datos_bbdd(query_extraccion_tupla)  # set de tuplas (url_viaje, nombre_viaje,viaje_activo)
    print(f' Existen {len(existing_tupla)} registros en la tabla viaje')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    data_to_update=[]
    for _,row in df_viajes_unicos.iterrows():
        url_viaje = row["url_viaje"]
        if url_viaje not in url_dict.keys():
            nombre_viaje = row['nombre_viaje']
            duracion_dias = row['duracion_dias']
            duracion_noches = row['duracion_noches']
            detalle_itinerario = row['itinerario_modificado_para_dividir']
            id_itinerario = itinerario_dict.get(detalle_itinerario)
            viaje_activo = row['en_ultimo_escrapeo']
            data_to_insert.append([url_viaje,nombre_viaje,duracion_dias,duracion_noches,id_itinerario,viaje_activo])
        else:
            nombre_viaje = row['nombre_viaje']
            viaje_activo = row['en_ultimo_escrapeo']
            tupla_combinacion = (url_viaje, nombre_viaje, viaje_activo) #creo la tupla de combinación url_viaje, nombre_viaje, viaje_activo
            if tupla_combinacion not in existing_tupla:
                data_to_update.append([url_viaje,nombre_viaje,viaje_activo])

    #ejecuto las querys de inserción y actualización
    insert_query = """
    INSERT INTO viaje (url_viaje,nombre_viaje,duracion_dias,duracion_noches,id_itinerario,viaje_activo)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query, data_to_insert)

    if (len(data_to_update))>0:
        for url_viaje, nombre_viaje, viaje_activo in data_to_update:
            update_query = """
            UPDATE viaje 
            SET nombre_viaje = %s,
                viaje_activo=%s
            where url_viaje = %s
            """
            lo.actualizar_datos_en_bbdd(update_query, ((nombre_viaje, viaje_activo, url_viaje)))
    print(f'Se han cargado {len(data_to_insert)} viajes nuevos en la BBDD')
    print(f'Se han actualizado {len(data_to_update)} viajes existentes en la BBDD')

def carga_tabla_precio_viaje(archivo_guardar_escrapeo_viajes_procesados = os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS')):
    #importo el fichero procesado de viajes
    df_viajes_total = pd.read_pickle(archivo_guardar_escrapeo_viajes_procesados)

    #me quedo con las columnas que me interesan y elimino duplicados:
    df_precio_viaje = df_viajes_total[['url_viaje','precio','fecha_escrapeo' ]].drop_duplicates()
    print(f'Existen {len(df_precio_viaje)} registros de precios diferentes')

    #CARGA EN BBDD:
    #Extraigo la información de la BBDD existente en la tabla viaje
    query_extraccion = "SELECT url_viaje, id_viaje FROM viaje"
    url_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Extraigo la información de la bbdd de los datos ya existentes:
    query_extraccion_tupla= "SELECT precio_viaje, fecha_precio_viaje, id_viaje FROM precio_viaje"
    existing_tupla = lo.extraer_tupla_datos_bbdd(query_extraccion_tupla)  # set de tuplas (precio_viaje, fecha_precio_viaje, id_viaje)
    print(f'Existen {len(existing_tupla)} registros en la tabla precio_viaje')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_precio_viaje.iterrows():
        url_viaje = row["url_viaje"]
        id_viaje = url_dict.get(url_viaje)
        precio_viaje = row['precio']
        fecha_precio_viaje = row['fecha_escrapeo'].date()
        tupla_combinacion = (precio_viaje, fecha_precio_viaje, id_viaje) #creo la tupla de combinación precio_viaje, fecha_precio_viaje, id_viaje
        if tupla_combinacion not in existing_tupla:
            data_to_insert.append([precio_viaje,fecha_precio_viaje,id_viaje])

    #inserto los datos en la BBDD:
    insert_query = """
    INSERT INTO precio_viaje (precio_viaje,fecha_precio_viaje,id_viaje)
    VALUES (%s, %s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query, data_to_insert)
    print(f'Se han insertado {len(data_to_insert)} registros en la tabla precio_viaje')

def carga_tabla_combinacion_destino_viaje(archivo_guardar_escrapeo_viajes_procesados = os.getenv('ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS')):
    #importo el fichero procesado de viajes
    df_viajes_total = pd.read_pickle(archivo_guardar_escrapeo_viajes_procesados)

    #normalizo y capitalizo la columna pais:
    df_viajes_total=lo.normalizar_capitalizar_columna(df_viajes_total,'pais')

    #me quedo con las columnas que me interesan:
    df_combinacion_destino_viaje = df_viajes_total[['url_viaje', 'pais']]

    #eliminos duplicados
    df_combinacion_destino_viaje = df_combinacion_destino_viaje.drop_duplicates()
    print(f'Existen {len(df_combinacion_destino_viaje)} combinaciones de destinos_viajes en total')

    #CARGA EN BBDD:
    #Extraigo la información de la BBDD existente en la tabla viaje
    query_extraccion = "SELECT url_viaje, id_viaje FROM viaje"
    viaje_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Extraigo la información de la BBDD existente en la tabla pais_destino:
    query_extraccion = "SELECT nombre_pais_destino, id_pais_destino FROM pais_destino"
    pais_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    # Obtengo todas las combinaciones existentes (id_viaje, id_pais_destino) de la tabla
    query_extraccion_tupla= "SELECT id_viaje, id_pais_destino FROM combinacion_destino_viaje"
    existing_pairs = lo.extraer_tupla_datos_bbdd(query_extraccion_tupla)  # set de tuplas (id_viaje, id_pais_destino)
    print(f'Existen {len(existing_pairs)} registros en la tabla combinacion_destino_viaje')

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_combinacion_destino_viaje.iterrows():
        url_viaje = row["url_viaje"]
        pais_destino = row["pais"]
        id_viaje = viaje_dict.get(url_viaje)
        id_pais_destino = pais_dict.get(pais_destino)
        pair = (id_viaje, id_pais_destino) #creo la tupla de combinación de id_viaje, id_pais_destino
        if pair not in existing_pairs:
            data_to_insert.append([id_viaje, id_pais_destino])

    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO combinacion_destino_viaje (id_viaje,id_pais_destino)
    VALUES (%s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query,data_to_insert)
    print(f'Se han añadido {len(data_to_insert)} registros en la tabla combinacion_destino_viaje')

def carga_tabla_turismo_emisor(archivo_guardar_datos_api_procesados = os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS')):
    df_turismo_emisor_procesado=pd.read_csv(archivo_guardar_datos_api_procesados) #importe el fichero procesado

    #normalizo y capitalizo la columna PAIS_DESTINO:
    df_turismo_emisor_procesado=lo.normalizar_capitalizar_columna(df_turismo_emisor_procesado, 'PAIS_DESTINO')

    #transformo las columnas pernoctaciones y turistas en enteros:
    for column in ['PERNOCTACIONES', 'TURISTAS']:
        df_turismo_emisor_procesado[column]=df_turismo_emisor_procesado[column].str.replace(',','.').astype(float).astype(int)

    #construyo el DF con los datos que me interesa:
    df_turismo_emisor_unico = df_turismo_emisor_procesado[['CCAA_ORIGEN','PAIS_DESTINO', 'TURISTAS','PERNOCTACIONES','AÑO','MES' ]]
    print(f'Existen {len(df_turismo_emisor_unico)} registros únicos en el fichero de turismo emisor')

    #CARGA EN BBDD:
    #extraigo los datos cargados en la BBDD para no duplicar los datos en las futuras subidas:
    query_extraccion_tupla= "SELECT ccaa_origen, id_pais_destino, num_turistas, num_pernoctaciones, ano, mes FROM turismo_emisor"
    existing_tupla = lo.extraer_tupla_datos_bbdd(query_extraccion_tupla)  # set de tuplas (ccaa_origen, id_pais_destino, num_turistas, num_pernoctaciones, ano, mes)
    print(f'Existen {len(existing_tupla)} registros en la tabla turismo_emisor')

    #Extraigo el id_pais_destino de la tabla pais_destino de la BBDD:
    query_extraccion = "SELECT nombre_pais_destino, id_pais_destino FROM pais_destino"
    pais_dict=lo.extraer_datos_de_BBDD(query_extraccion)

    #Creo los datos para insertarlos en la BBDD:
    data_to_insert=[]
    for _,row in df_turismo_emisor_unico.iterrows():
        ccaa_origen = row["CCAA_ORIGEN"]
        pais_destino = row['PAIS_DESTINO']
        id_pais_destino = pais_dict.get(pais_destino)
        num_turistas = row['TURISTAS']
        num_pernoctaciones = row['PERNOCTACIONES']
        ano = row['AÑO']
        mes = row['MES']
        pair = (ccaa_origen, id_pais_destino, num_turistas, num_pernoctaciones, ano, mes) #creo la tupla de combinación
        if pair not in existing_tupla:
            data_to_insert.append([ccaa_origen, id_pais_destino, num_turistas, num_pernoctaciones, ano, mes])

    #Subo la información a la BBDD:
    insert_query = """
    INSERT INTO turismo_emisor (ccaa_origen, id_pais_destino, num_turistas, num_pernoctaciones, ano, mes)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    lo.insertar_datos_en_BBDD(insert_query,data_to_insert)
    print(f'Se han añadido {len(data_to_insert)} registros en la tabla turismo_emisor')

def carga_total():
    print('TABLA PAIS_DESTINO')
    df_paises_continentes_totales= lo.obtencion_tabla_paises_continentes_unicos()
    lo.carga_tabla_pais_destino(df_paises_continentes_totales)

    print('TABLA ITINERARIO')
    lo.carga_tabla_itinerario()
    
    print('TABLA CIUDAD')
    lo.carga_tabla_ciudad()

    print('TABLA CIUDAD_ITINEARIO')
    lo.carga_tabla_ciudad_itinerario()

    print('TABLA VIAJE')
    lo.carga_tabla_viaje()

    print('TABLA PRECIO_VIAJE')
    lo.carga_tabla_precio_viaje()

    print('TABLA COMBINACION_DESTINO_VIAJE')
    lo.carga_tabla_combinacion_destino_viaje()

    print('TABLA TURISMO_EMISOR')
    lo.carga_tabla_turismo_emisor()

    print('LA CARGA EN BBDD HA FINALIZADO')