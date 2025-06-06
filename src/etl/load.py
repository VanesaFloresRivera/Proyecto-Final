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
    """
    Crea una conexión a una base de datos PostgreSQL y devuelve tanto la conexión como el cursor.

    Parámetros:
    -----------
    dbname : str, opcional
        Nombre de la base de datos a la que conectarse (por defecto, valor de `DB_NAME`).
    
    user : str, opcional
        Usuario de la base de datos (por defecto, valor de `DB_USER`).
    
    password : str, opcional
        Contraseña del usuario (por defecto, valor de `DB_PASSWORD`).
    
    host : str, opcional
        Dirección del servidor de la base de datos (por defecto, valor de `DB_HOST`).
    
    port : int o str, opcional
        Puerto de conexión al servidor de la base de datos (por defecto, valor de `DB_PORT`).

    Retorna:
    --------
    tuple
        Una tupla `(conn, cur)` donde:
        - `conn` es la conexión activa a la base de datos.
        - `cur` es el cursor para ejecutar comandos SQL sobre esa conexión.

    Notas:
    ------
    - Se requiere tener `psycopg2` instalado para usar esta función.
    - Asegúrate de cerrar la conexión con `conn.close()` y el cursor con `cur.close()` tras finalizar el uso.
    """

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
    """
    Cierra de forma ordenada el cursor y la conexión a la base de datos.

    Parámetros:
    -----------
    conexion : psycopg2.extensions.connection
        Objeto de conexión a la base de datos.

    cursor : psycopg2.extensions.cursor
        Objeto cursor asociado a la conexión, utilizado para ejecutar consultas SQL.

    Retorna:
    --------
    None
    """
    cursor.close() #cierro el cursor
    conexion.close() #cierro la conexión

def insertar_datos_en_BBDD(insert_query,data_to_insert,dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):
    """
    Inserta uno o varios registros en una base de datos PostgreSQL mediante una consulta parametrizada.

    Parámetros:
    -----------
    insert_query : str
        Sentencia SQL de inserción preparada (con parámetros %s), utilizada para insertar datos.

    data_to_insert : list[tuple] o tuple
        Datos a insertar en la base de datos. Puede ser:
        - Una lista de tuplas (para insertar múltiples registros con `executemany`)
        - Una única tupla (para insertar un solo registro con `execute`)

    dbname : str, opcional
        Nombre de la base de datos. Por defecto, se toma de la variable global `DB_NAME`.

    user : str, opcional
        Nombre de usuario de la base de datos. Por defecto, `DB_USER`.

    password : str, opcional
        Contraseña del usuario. Por defecto, `DB_PASSWORD`.

    host : str, opcional
        Dirección del servidor de base de datos. Por defecto, `DB_HOST`.

    port : str o int, opcional
        Puerto de acceso al servidor. Por defecto, `DB_PORT`.

    Retorna:
    --------
    None

    Notas:
    ------
    - Solo ejecuta la inserción si `data_to_insert` contiene al menos un elemento.
    - La conexión y el cursor se abren y cierran automáticamente dentro de la función.
    - Se usa `executemany()` si `data_to_insert` es una lista de tuplas y `execute()` si es un solo registro.
    - La transacción se confirma con `commit()` tras la ejecución.
    """  
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
    """
    Ejecuta una consulta SQL sobre una base de datos PostgreSQL y devuelve los resultados en forma de diccionario.

    Parámetros:
    -----------
    query_extracción : str
        Consulta SQL que debe devolver dos columnas: clave y valor, para poder convertir el resultado a diccionario.

    dbname : str, opcional
        Nombre de la base de datos. Por defecto, `DB_NAME`.

    user : str, opcional
        Usuario de acceso a la base de datos. Por defecto, `DB_USER`.

    password : str, opcional
        Contraseña del usuario. Por defecto, `DB_PASSWORD`.

    host : str, opcional
        Dirección del servidor de la base de datos. Por defecto, `DB_HOST`.

    port : int o str, opcional
        Puerto del servidor. Por defecto, `DB_PORT`.

    Retorna:
    --------
    dict
        Diccionario con los datos extraídos, donde cada fila de la consulta se convierte en un par clave–valor.

    Notas:
    ------
    - La consulta debe retornar exactamente dos columnas para que `dict(cur.fetchall())` funcione correctamente.
    - La función abre y cierra automáticamente la conexión a la base de datos.
    - Si la consulta no devuelve datos o el formato no es compatible, se lanzará un error.
    """

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extracción) #ejecuto la extracción
    diccionario = dict(cur.fetchall()) #creo un diccionario

    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return diccionario

def limpiar_texto(texto):
    """
    Limpia una cadena de texto separada por comas, eliminando espacios innecesarios alrededor de cada elemento
    y devolviendo el texto limpio, separado por ', '.

    Parámetros:
    -----------
    texto : str
        Cadena de texto que contiene elementos separados por comas (ej. " París ,Londres,  Roma ").

    Retorna:
    --------
    str
        Cadena limpia con los elementos separados por coma y un solo espacio, sin espacios sobrantes.

    Ejemplo:
    --------
    >>> limpiar_texto(" París ,Londres,  Roma ")
    'París, Londres, Roma'
    """
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
    """
    Ejecuta una consulta de actualización (UPDATE) en una base de datos PostgreSQL con parámetros dinámicos.

    Parámetros:
    -----------
    query_actualizacion : str
        Sentencia SQL de actualización parametrizada, con marcadores `%s` para los valores.

    tupla_columnas : tuple
        Tupla con los valores que se deben insertar en la sentencia SQL.

    dbname : str, opcional
        Nombre de la base de datos (por defecto, `DB_NAME`).

    user : str, opcional
        Usuario de la base de datos (por defecto, `DB_USER`).

    password : str, opcional
        Contraseña del usuario (por defecto, `DB_PASSWORD`).

    host : str, opcional
        Host del servidor (por defecto, `DB_HOST`).

    port : str o int, opcional
        Puerto de acceso a la base de datos (por defecto, `DB_PORT`).

    Retorna:
    --------
    None

    Notas:
    ------
    - La conexión y el cursor se abren y cierran automáticamente.
    - La función no hace commit, por lo que si el UPDATE no tiene efecto inmediato,
      puede ser conveniente incluir `conn.commit()` si se requiere confirmación explícita.
    - Asegúrate de que la tupla tenga el mismo número de elementos que los parámetros en la query.
    """
    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_actualizacion, tupla_columnas) #ejecuto la query de actualizacion

    lo.cerrar_conexion(conn, cur) #cierro la conexión

def extraer_tupla_datos_bbdd (query_extraccion_tupla, dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):
    """
    Ejecuta una consulta SQL sobre una base de datos PostgreSQL y devuelve los resultados como un conjunto de tuplas.

    Parámetros:
    -----------
    query_extraccion_tupla : str
        Consulta SQL que debe devolver múltiples columnas (por ejemplo, para verificar combinaciones existentes).

    dbname : str, opcional
        Nombre de la base de datos (por defecto, `DB_NAME`).

    user : str, opcional
        Usuario de acceso a la base de datos (por defecto, `DB_USER`).

    password : str, opcional
        Contraseña del usuario (por defecto, `DB_PASSWORD`).

    host : str, opcional
        Dirección del servidor de la base de datos (por defecto, `DB_HOST`).

    port : int o str, opcional
        Puerto del servidor de la base de datos (por defecto, `DB_PORT`).

    Retorna:
    --------
    set of tuple
        Conjunto de tuplas con los resultados de la consulta. Útil para verificar si una combinación de valores ya existe.

    Notas:
    ------
    - La función abre y cierra automáticamente la conexión a la base de datos.
    - Ideal para evitar inserciones duplicadas mediante validaciones previas.
    """
    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extraccion_tupla) #ejecuto la extracción
    existing_pairs = set(cur.fetchall()) #creo un diccionario

    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return existing_pairs

def normalizar_capitalizar_columna (df,columna):
    """
    Aplica una transformación de normalización y capitalización a una columna de un DataFrame.

    Parámetros:
    -----------
    df : pandas.DataFrame
        DataFrame que contiene la columna a transformar.

    columna : str
        Nombre de la columna del DataFrame sobre la que se aplicarán las transformaciones.

    Retorna:
    --------
    pandas.DataFrame
        El mismo DataFrame con la columna transformada.

    Notas:
    ------
    - La función `tr.normalizar_texto` debe encargarse de eliminar tildes, caracteres especiales, etc.
    - La función `tr.capitalizar_texto` debe encargarse de convertir a mayúscula solo la primera letra de cada palabra.
    - Ambas funciones deben estar definidas previamente en el módulo `tr`.

    Ejemplo:
    --------
    >>> df = normalizar_capitalizar_columna(df, "nombre_ciudad")
    """
    df[columna] = df[columna].map(tr.normalizar_texto)
    df[columna] = df[columna].map(tr.capitalizar_texto)
    return df

def extraccion_unicos_pais_continente_turismo_emisor(archivo_guardar_datos_api_procesados = os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS')):
    """
    Extrae y limpia una tabla única de países y continentes a partir de los datos procesados del turismo emisor.

    Parámetros:
    -----------
    archivo_guardar_datos_api_procesados : str, opcional
        Ruta del archivo CSV procesado con datos de turismo emisor.
        Por defecto, se toma desde la variable de entorno 'ARCHIVO_GUARDAR_DATOS_API_PROCESADOS'.

    Retorna:
    --------
    pandas.DataFrame
        DataFrame con dos columnas:
        - 'nombre_pais_destino' (normalizada y capitalizada),
        - 'nombre_continente'.

    Proceso:
    --------
    - Lee el archivo CSV procesado de la API de turismo emisor.
    - Selecciona solo las columnas de país y continente.
    - Elimina duplicados.
    - Renombra las columnas a un formato más estandarizado.
    - Aplica normalización y capitalización al nombre del país destino.

    Notas:
    ------
    - Requiere que las funciones `normalizar_texto` y `capitalizar_texto` estén accesibles desde `lo`.
    - La columna `'nombre_pais_destino'` queda lista para integrarse con otras fuentes que usen formato normalizado.
    """
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
    """
    Extrae y limpia una tabla única de países y continentes a partir de los datos acumulados por scraping.

    Parámetros:
    -----------
    archivo_guardar_continentes_procesados : str, opcional
        Ruta del archivo `.pkl` con los resultados acumulados del scraping de continentes por país.
        Por defecto, se toma desde la variable de entorno 'ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS'.

    Retorna:
    --------
    pandas.DataFrame
        DataFrame con las columnas:
        - 'nombre_pais_destino': nombre del país, normalizado y capitalizado.
        - 'nombre_continente': continente correspondiente.

    Proceso:
    --------
    - Carga el DataFrame guardado con los resultados del scraping.
    - Filtra solo las columnas relevantes ('pais', 'continente').
    - Elimina duplicados y renombra las columnas.
    - Aplica una limpieza de texto con normalización y capitalización al nombre del país.

    Notas:
    ------
    - Requiere que la función `lo.normalizar_capitalizar_columna` esté disponible.
    - Este DataFrame se puede usar para comparar la cobertura de scraping frente a los datos de la API oficial.
    """
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
    """
    Extrae una lista única de países desde los itinerarios procesados, normalizando y corrigiendo nombres específicos.

    Parámetros:
    -----------
    archivo_guardar_itinerarios_procesados_1 : str, opcional
        Ruta del archivo `.pkl` que contiene los datos de itinerarios procesados.
        Por defecto, se toma desde la variable de entorno 'ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'.

    Retorna:
    --------
    pandas.DataFrame
        DataFrame con una única columna:
        - 'pais': nombres de países únicos, corregidos, normalizados y capitalizados.

    Proceso:
    --------
    - Carga los datos procesados de itinerarios desde un archivo `.pkl`.
    - Realiza una corrección específica de nombre ('Chequia' → 'Republica Checa').
    - Extrae los países únicos desde la columna `pais_correcto`.
    - Aplica transformación de texto: normalización y capitalización.

    Notas:
    ------
    - La función `lo.normalizar_capitalizar_columna` debe estar definida en el entorno.
    - Este conjunto puede usarse para comparar los países incluidos en los itinerarios frente a los destinos o datos oficiales.
    """
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)
    df_itinerarios_ciudades.pais_correcto= df_itinerarios_ciudades.pais_correcto.replace({
    'Chequia':'Republica Checa'
                                            })

    #me quedo solo con los datos que me interesan
    df_itinerarios_ciudades_pais_continentes = pd.DataFrame()
    df_itinerarios_ciudades_pais_continentes['pais'] = None
    df_itinerarios_ciudades_pais_continentes['pais'] = df_itinerarios_ciudades['pais_correcto'].drop_duplicates().reset_index(drop=True)

    #normalizo y capitalizo la columna pais:
    df_itinerarios_ciudades_pais_continentes = lo.normalizar_capitalizar_columna(df_itinerarios_ciudades_pais_continentes, 'pais')
    #print(df_itinerarios_ciudades_pais_continentes)
    return df_itinerarios_ciudades_pais_continentes


def extraccion_informacion_obtenida_api_opencage(archivo_guardar_total_ciudades_api = os.getenv('ARCHIVO_GUARDAR_TOTAL_CIUDADES_API'),
                                                                                               archivo_extraccion_api_duplicados = os.getenv('ARCHIVO_EXTRACCION_API_DUPLICADOS')):
    """
    Extrae y limpia la información geográfica obtenida desde la API de OpenCage, incluyendo ciudades geolocalizadas
    y países con registros duplicados en distintos continentes.

    Parámetros:
    -----------
    archivo_guardar_total_ciudades_api : str, opcional
        Ruta del archivo `.pkl` que contiene los datos geográficos completos obtenidos por API.
        Por defecto, se carga desde la variable de entorno 'ARCHIVO_GUARDAR_TOTAL_CIUDADES_API'.

    archivo_extraccion_api_duplicados : str, opcional
        Ruta del archivo `.pkl` que contiene los países detectados como duplicados en más de un continente.
        Por defecto, se carga desde la variable de entorno 'ARCHIVO_EXTRACCION_API_DUPLICADOS'.

    Retorna:
    --------
    tuple
        - df_opencage_ciudades_totales : pandas.DataFrame  
            Contiene los datos de geolocalización de ciudades, con nombres de continentes en español
            y columna `pais_api` normalizada y capitalizada.
        
        - df_paises_api_duplicados : pandas.DataFrame  
            Subconjunto de países que aparecen con distintos continentes en los datos de OpenCage.

    Notas:
    ------
    - Se reemplazan los nombres de continentes en inglés por su equivalente en español en ambas tablas.
    - La columna `pais_api` se limpia con normalización y capitalización mediante `lo.normalizar_capitalizar_columna`.
    - Este proceso es útil para validar la coherencia geográfica de los datos antes de su integración.
    """
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
    """
    Identifica los países presentes en los itinerarios que no están en los datos oficiales del turismo emisor
    ni en los resultados del scraping, y trata de completar su continente desde los datos de la API de OpenCage.

    Parámetros:
    -----------
    df_paises_continentes_turismos_emisor : pandas.DataFrame
        Países y continentes obtenidos desde la fuente oficial del turismo emisor (Dataestur).

    df_paises_continentes_escrapeo : pandas.DataFrame
        Países y continentes obtenidos por scraping desde la web de TUI.

    df_itinerarios_ciudades_pais_continentes : pandas.DataFrame
        Países únicos extraídos de los itinerarios de los viajes (fuente interna del proyecto).

    df_opencage_ciudades_totales : pandas.DataFrame
        Datos de ciudades y países geolocalizados a través de la API de OpenCage.

    df_paises_api_duplicados : pandas.DataFrame
        Subconjunto de países detectados con múltiples continentes en la API de OpenCage.

    Retorna:
    --------
    pandas.DataFrame
        DataFrame con las columnas:
        - 'nombre_pais_destino': países que no estaban en las otras fuentes y que se han podido asignar a un continente.
        - 'nombre_continente': continente correspondiente, obtenido desde OpenCage o sus duplicados.

    Notas:
    ------
    - Se hace una comparación país a país de la fuente de itinerarios contra las demás fuentes para detectar ausencias.
    - Solo se incluyen países que pueden encontrarse en `df_opencage_ciudades_totales` o en `df_paises_api_duplicados`.
    - Se normaliza y capitaliza la columna `nombre_pais_destino` para mantener consistencia en los nombres.
    - El código incluye condiciones especiales (por ejemplo, un `print` de depuración para 'Catar') que podrían eliminarse en versión final.
    - Esta función ayuda a completar la tabla de países y continentes antes de un análisis o cruce final.
    """
    dict_paises_continentes_ciudades = {'pais':[], 'continente':[]}
    #print(df_itinerarios_ciudades_pais_continentes.pais.unique())
    #print(df_itinerarios_ciudades_pais_continentes[df_itinerarios_ciudades_pais_continentes.pais=='Chequia']) #depurar el código, por algún motivo, no coge el pais Chequia, Sovereign Base Areas Of Akrotiri And Dhekelia,No Existe Resultado y Catar. En jupyter si funciona
    for pais in df_itinerarios_ciudades_pais_continentes.pais.tolist():
        if pais=='Catar ':
            print('Catar')
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

        #renombro las columnas:
        df_paises_continentes_ciudades= df_paises_continentes_ciudades.rename(
        columns={'pais':'nombre_pais_destino', 'continente':'nombre_continente'})

        #normalizo y capitalizo la columna pais_api:
        df_paises_continentes_ciudades = lo.normalizar_capitalizar_columna(df_paises_continentes_ciudades, 'nombre_pais_destino')
        #print(dict_paises_continentes_ciudades)

        return df_paises_continentes_ciudades

def obtencion_paises_continentes_unicos (df_paises_continentes_turismos_emisor,df_paises_continentes_escrapeo,df_paises_continentes_ciudades):
    """
    Une las tablas de países–continentes procedentes de varias fuentes y elimina duplicados,
    resolviendo manualmente las asignaciones inconsistentes (países asignados a más de un continente).

    Parámetros:
    -----------
    df_paises_continentes_turismos_emisor : pandas.DataFrame
        Datos oficiales de países y continentes obtenidos desde el turismo emisor (Dataestur).
    
    df_paises_continentes_escrapeo : pandas.DataFrame
        Datos de países y continentes obtenidos por scraping desde TUI.

    df_paises_continentes_ciudades : pandas.DataFrame
        Datos de países y continentes recuperados a partir de la geolocalización de ciudades (OpenCage u otros).

    Retorna:
    --------
    pandas.DataFrame
        DataFrame único con los países y sus continentes, sin duplicados, y con conflictos resueltos manualmente.

    Proceso:
    --------
    - Concatena los tres DataFrames en uno solo.
    - Elimina duplicados por combinación de país y continente.
    - Identifica los países que aparecen más de una vez (asignados a varios continentes).
    - Reasigna manualmente esos países a un continente por defecto ('Asia').
    - Elimina los duplicados resultantes de la corrección.

    Notas:
    ------
    - Esta función asume que cualquier país duplicado debe pertenecer a Asia (esto puede personalizarse).
    - Las entradas deben tener las columnas 'nombre_pais_destino' y 'nombre_continente'.
    - Es una etapa final de consolidación útil antes de alimentar un modelo o visualizar los datos.
    """
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
    """
    Genera la tabla final consolidada de países y continentes únicos, combinando y depurando datos de múltiples fuentes:
    API de turismo emisor, scraping, itinerarios y geolocalización de ciudades (OpenCage).

    Parámetros:
    -----------
    No recibe parámetros directamente. Las rutas de los archivos de entrada se leen desde variables de entorno:
    - ARCHIVO_GUARDAR_DATOS_API_PROCESADOS
    - ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS
    - ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1
    - ARCHIVO_GUARDAR_TOTAL_CIUDADES_API
    - ARCHIVO_EXTRACCION_API_DUPLICADOS

    Retorna:
    --------
    pandas.DataFrame
        DataFrame final con las columnas:
        - 'nombre_pais_destino': nombre del país, limpio, normalizado y capitalizado.
        - 'nombre_continente': continente correspondiente, con conflictos resueltos.

    Proceso:
    --------
    1. Extrae los países y continentes desde:
       - Turismo emisor (Dataestur).
       - Scraping de destinos TUI.
       - Itinerarios de los viajes.
       - Datos de geolocalización vía OpenCage.
    2. Detecta países no presentes en fuentes oficiales ni scraping, y completa con OpenCage.
    3. Une y deduplica todos los registros.
    4. Resuelve manualmente duplicados por país en varios continentes (se asignan a Asia).
    5. Aplica normalización y capitalización a los nombres de países.

    Notas:
    ------
    - El resultado final es clave para unir datos de distintas fuentes geográficas sin inconsistencias.
    - Es recomendable guardar el resultado como tabla de referencia maestra de países y continentes.
    """
    df_paises_continentes_turismos_emisor= lo.extraccion_unicos_pais_continente_turismo_emisor(archivo_guardar_datos_api_procesados = os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS'))
    df_paises_continentes_escrapeo = lo.extraccion_unicos_paises_escrapeados(archivo_guardar_continentes_procesados = os.getenv('ARCHIVO_GUARDAR_CONTINENTES_PROCESADOS'))
    df_itinerarios_ciudades_pais_continentes = lo.extraccion_unicos_paises_itinerarios(archivo_guardar_itinerarios_procesados_1 = os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'))
    df_opencage_ciudades_totales,df_paises_api_duplicados=lo.extraccion_informacion_obtenida_api_opencage(archivo_guardar_total_ciudades_api = os.getenv('ARCHIVO_GUARDAR_TOTAL_CIUDADES_API'),
                                                                                               archivo_extraccion_api_duplicados = os.getenv('ARCHIVO_EXTRACCION_API_DUPLICADOS'))
    df_paises_continentes_ciudades = lo.obtencion_paises_continentes_no_incluidos_turismo_emisor_escrapeo(df_paises_continentes_turismos_emisor, 
                                                                                                          df_paises_continentes_escrapeo,df_itinerarios_ciudades_pais_continentes,df_opencage_ciudades_totales,df_paises_api_duplicados)
    df_paises_continentes_totales= lo.obtencion_paises_continentes_unicos (df_paises_continentes_turismos_emisor,df_paises_continentes_escrapeo,df_paises_continentes_ciudades)
    print(f'Existen {len(df_paises_continentes_totales)} paises únicos')
   #normalizo y capitalizo la columna pais_api:
    df_paises_continentes_totales = lo.normalizar_capitalizar_columna(df_paises_continentes_totales, 'nombre_pais_destino').drop_duplicates()
    return df_paises_continentes_totales

def carga_tabla_pais_destino(df_paises_continentes_totales):
    """
    Inserta en la base de datos los países y continentes que aún no están registrados en la tabla `pais_destino`.

    Parámetros:
    -----------
    df_paises_continentes_totales : pandas.DataFrame
        DataFrame con los países y continentes únicos, ya normalizados y listos para ser insertados.

    Retorna:
    --------
    None

    Proceso:
    --------
    - Extrae los registros actuales de la tabla `pais_destino` mediante `lo.extraer_datos_de_BBDD()`.
    - Compara con los países del DataFrame recibido para detectar cuáles aún no están insertados.
    - Construye una lista `data_to_insert` con los nuevos pares [nombre_pais_destino, nombre_continente].
    - Inserta esos registros con la función `lo.insertar_datos_en_BBDD()` usando una sentencia SQL parametrizada.
    - Imprime cuántos países se han añadido.

    Notas:
    ------
    - Evita duplicados comparando contra los registros existentes en la base de datos.
    - Esta función no retorna nada, pero informa del número de inserciones realizadas por consola.
    - Se asume que la conexión a la base de datos y las funciones auxiliares (`extraer_datos_de_BBDD`, `insertar_datos_en_BBDD`)
      están correctamente definidas en el módulo `lo`.
    """
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
    """
    Inserta en la base de datos los itinerarios únicos que aún no están registrados en la tabla `itinerario`.

    Parámetros:
    -----------
    archivo_guardar_itinerarios_procesados_1 : str, opcional
        Ruta al archivo `.pkl` que contiene los itinerarios procesados.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'.

    Retorna:
    --------
    None

    Proceso:
    --------
    - Carga los itinerarios desde el archivo de entrada.
    - Corrige valores específicos (ej. 'Chequia' → 'Republica Checa').
    - Aplica limpieza a los textos de itinerario (quita espacios innecesarios entre comas).
    - Extrae la lista de itinerarios únicos.
    - Compara contra los ya existentes en la tabla `itinerario` para evitar duplicados.
    - Inserta en la base de datos los nuevos itinerarios utilizando `lo.insertar_datos_en_BBDD`.

    Notas:
    ------
    - Utiliza `lo.limpiar_texto()` para limpiar los itinerarios.
    - Usa `lo.extraer_datos_de_BBDD()` para consultar los itinerarios ya insertados.
    - La inserción se realiza con una sentencia parametrizada.
    - Informa por consola de cuántos itinerarios se encontraron y cuántos se insertaron.
    - Asume que la tabla `itinerario` tiene una columna `detalle_itinerario` y una clave primaria `id_itinerario`.
    """
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)
    df_itinerarios_ciudades.pais_correcto= df_itinerarios_ciudades.pais_correcto.replace({
    'Chequia':'Republica Checa'
                                            })

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
    """
    Inserta en la base de datos las ciudades únicas asociadas a sus países correspondientes
    que aún no están registradas en la tabla `ciudad`.

    Parámetros:
    -----------
    archivo_guardar_itinerarios_procesados_1 : str, opcional
        Ruta al archivo `.pkl` que contiene los itinerarios y las ciudades procesadas.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'.

    Retorna:
    --------
    None

    Proceso:
    --------
    - Carga las ciudades y países desde el archivo de entrada.
    - Realiza correcciones de nombres (ej. 'Chequia' → 'Republica Checa').
    - Deduplica las combinaciones ciudad–país.
    - Normaliza y capitaliza ambas columnas.
    - Consulta la base de datos para obtener:
        - Las ciudades ya registradas (`ciudad`).
        - El `id_pais_destino` correspondiente a cada país (`pais_destino`).
    - Identifica las ciudades nuevas que no están en la tabla y prepara la lista de inserción.
    - Inserta los nuevos registros usando `lo.insertar_datos_en_BBDD`.

    Notas:
    ------
    - Utiliza funciones auxiliares del módulo `lo` para extracción e inserción de datos (`extraer_datos_de_BBDD`, `insertar_datos_en_BBDD`).
    - Asume que la clave foránea `id_pais_destino` ya existe para cada país correspondiente.
    - Informa por consola del número de ciudades únicas encontradas y cuántas han sido insertadas.
    """
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)
    df_itinerarios_ciudades.pais_correcto= df_itinerarios_ciudades.pais_correcto.replace({
    'Chequia':'Republica Checa'
                                            })


    #creo el DF con la ciudad y el pais para preparar los datos para la carga y elimino duplicados:
    df_ciudad_pais_unicos = df_itinerarios_ciudades[['ciudad','pais_correcto']].drop_duplicates().reset_index(drop=True)    
    print(f'Existen {len(df_ciudad_pais_unicos)} ciudades únicas')

     #normalizo y capitalizo la columna ciudad y pais correcto:
    df_ciudad_pais_unicos = lo.normalizar_capitalizar_columna(df_ciudad_pais_unicos, 'ciudad')
    df_ciudad_pais_unicos = lo.normalizar_capitalizar_columna(df_ciudad_pais_unicos, 'pais_correcto')

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
    """
    Inserta en la base de datos las combinaciones únicas de ciudad e itinerario que aún no están registradas
    en la tabla intermedia `ciudad_itinerario`.

    Parámetros:
    -----------
    archivo_guardar_itinerarios_procesados_1 : str, opcional
        Ruta al archivo `.pkl` con los datos de itinerarios y ciudades ya procesados.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1'.

    Retorna:
    --------
    None

    Proceso:
    --------
    - Carga los datos procesados con las columnas `ciudad` e `itinerario_modificado_para_dividir`.
    - Corrige valores conocidos (ej. 'Chequia' → 'Republica Checa').
    - Limpia los textos de itinerario y normaliza/capitaliza los nombres de ciudad.
    - Deduplica las combinaciones ciudad–itinerario.
    - Extrae de la base de datos:
        - Las ciudades (`ciudad`: nombre → id).
        - Los itinerarios (`itinerario`: texto → id).
        - Las combinaciones ya registradas (`ciudad_itinerario`: tuplas de ids).
    - Inserta solo las combinaciones nuevas de `(id_ciudad, id_itinerario)`.

    Notas:
    ------
    - Utiliza funciones auxiliares del módulo `lo`, incluyendo:
        - `extraer_datos_de_BBDD`
        - `extraer_tupla_datos_bbdd`
        - `insertar_datos_en_BBDD`
        - `limpiar_texto` y `normalizar_capitalizar_columna`
    - Informa por consola cuántas combinaciones únicas se encontraron y cuántas se insertaron.
    - Garantiza que no haya duplicados en la tabla final.
    - Se asume que `ciudad` y `itinerario` están previamente cargados en la base de datos.
    """
    df_itinerarios_ciudades = pd.read_pickle(archivo_guardar_itinerarios_procesados_1)
    df_itinerarios_ciudades.pais_correcto= df_itinerarios_ciudades.pais_correcto.replace({
    'Chequia':'Republica Checa'
                                            })

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
    """
    Carga y actualiza la tabla `viaje` en la base de datos con los viajes únicos extraídos por scraping.
    Se insertan los nuevos viajes y se actualiza el estado de "viaje activo" para los ya existentes.

    Parámetros:
    -----------
    archivo_guardar_escrapeo_viajes_procesados : str, opcional
        Ruta al archivo `.pkl` que contiene los datos de viajes ya procesados.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS'.

    Retorna:
    --------
    None

    Proceso:
    --------
    1. Carga los viajes desde archivo y limpia la columna `itinerario_modificado_para_dividir`.
    2. Identifica qué viajes están activos (último escrapeo) y lo marca en la columna `en_ultimo_escrapeo`.
    3. Deduplica los viajes por URL.
    4. Divide la duración en días y noches en columnas separadas.
    5. Convierte la columna `en_ultimo_escrapeo` a tipo booleano.
    6. Consulta la base de datos para obtener:
        - URLs ya existentes (`url_viaje`),
        - Itinerarios con sus IDs,
        - Combinaciones existentes de `url_viaje`, `nombre_viaje`, `viaje_activo`.
    7. Prepara:
        - Datos a insertar (nuevos viajes),
        - Datos a actualizar (estado activo o nombre del viaje cambiado).
    8. Inserta los nuevos registros en la tabla `viaje`.
    9. Actualiza los existentes si ha cambiado su estado o nombre.

    Notas:
    ------
    - Utiliza funciones auxiliares del módulo `lo`:
        - `limpiar_texto`, `convertir_si_no_a_boolean`,
        - `extraer_datos_de_BBDD`, `extraer_tupla_datos_bbdd`,
        - `insertar_datos_en_BBDD`, `actualizar_datos_en_bbdd`.
    - Imprime el número de viajes nuevos insertados y viajes existentes actualizados.
    - Asegura que no se repitan viajes ya cargados, y mantiene actualizado el estado de los existentes.
    """
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
    """
    Inserta en la base de datos los registros de precios de los viajes, extraídos por scraping, evitando duplicados.

    Parámetros:
    -----------
    archivo_guardar_escrapeo_viajes_procesados : str, opcional
        Ruta al archivo `.pkl` que contiene los datos de viajes procesados.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS'.

    Retorna:
    --------
    None

    Proceso:
    --------
    1. Carga los datos de scraping desde el archivo de viajes procesados.
    2. Extrae las columnas relevantes: `url_viaje`, `precio`, `fecha_escrapeo`, y elimina duplicados.
    3. Consulta en la base de datos:
        - Los `id_viaje` asociados a cada `url_viaje`.
        - Los registros ya existentes en la tabla `precio_viaje` como tuplas `(precio, fecha, id_viaje)`.
    4. Prepara la lista de registros a insertar, descartando los que ya existen.
    5. Inserta los nuevos precios en la base de datos usando `lo.insertar_datos_en_BBDD`.

    Notas:
    ------
    - La función garantiza que no se insertan precios duplicados para un mismo viaje y fecha.
    - Utiliza funciones auxiliares del módulo `lo`:
        - `extraer_datos_de_BBDD`
        - `extraer_tupla_datos_bbdd`
        - `insertar_datos_en_BBDD`
    - Informa por consola del número de precios únicos encontrados y cuántos han sido insertados.
    """
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
    """
    Inserta en la base de datos las combinaciones únicas de viajes y países de destino 
    que aún no están registradas en la tabla `combinacion_destino_viaje`.

    Parámetros:
    -----------
    archivo_guardar_escrapeo_viajes_procesados : str, opcional
        Ruta al archivo `.pkl` que contiene los datos de viajes procesados con información de países.
        Por defecto, se obtiene desde la variable de entorno 'ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS'.

    Retorna:
    --------
    None

    Proceso:
    --------
    1. Carga los viajes procesados desde el archivo.
    2. Normaliza y capitaliza la columna `pais` para asegurar consistencia con la base de datos.
    3. Filtra las columnas relevantes: `url_viaje` y `pais`, y elimina duplicados.
    4. Consulta en la base de datos:
        - Los IDs de los viajes (`viaje`).
        - Los IDs de los países (`pais_destino`).
        - Las combinaciones ya existentes (`combinacion_destino_viaje`).
    5. Prepara las combinaciones `(id_viaje, id_pais_destino)` que aún no existen.
    6. Inserta las nuevas combinaciones en la tabla intermedia.

    Notas:
    ------
    - Utiliza funciones auxiliares del módulo `lo`, como:
        - `normalizar_capitalizar_columna`
        - `extraer_datos_de_BBDD`
        - `extraer_tupla_datos_bbdd`
        - `insertar_datos_en_BBDD`
    - Asegura que no se generen duplicados en la tabla intermedia.
    - Informa por consola el número de combinaciones nuevas insertadas.
    """
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
    """
    Inserta en la base de datos los registros únicos de turismo emisor español, 
    evitando duplicados y relacionando los países con sus IDs en la tabla `pais_destino`.

    Parámetros:
    -----------
    archivo_guardar_datos_api_procesados : str, opcional
        Ruta al archivo CSV con los datos de turismo emisor ya procesados.
        Por defecto, se toma desde la variable de entorno 'ARCHIVO_GUARDAR_DATOS_API_PROCESADOS'.

    Retorna:
    --------
    None

    Proceso:
    --------
    1. Carga el archivo CSV con los datos oficiales del turismo emisor.
    2. Normaliza y capitaliza los nombres de países destino.
    3. Convierte las columnas `TURISTAS` y `PERNOCTACIONES` a enteros (limpiando formato europeo).
    4. Construye un DataFrame con las columnas necesarias: `CCAA_ORIGEN`, `PAIS_DESTINO`, `TURISTAS`, `PERNOCTACIONES`, `AÑO`, `MES`.
    5. Consulta en la base de datos:
        - Combinaciones ya existentes para evitar duplicados.
        - Diccionario con `nombre_pais_destino` → `id_pais_destino`.
    6. Prepara las combinaciones nuevas (no existentes en BBDD).
    7. Inserta los nuevos registros en la tabla `turismo_emisor`.

    Notas:
    ------
    - Utiliza funciones auxiliares del módulo `lo`, como:
        - `normalizar_capitalizar_columna`
        - `extraer_datos_de_BBDD`
        - `extraer_tupla_datos_bbdd`
        - `insertar_datos_en_BBDD`
    - Los registros duplicados no se insertan, lo que garantiza consistencia histórica.
    - Informa por consola cuántos registros únicos había en el CSV y cuántos se han insertado.
    """
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
    """
    Ejecuta de forma secuencial la carga completa del modelo de datos en la base de datos.
    Incluye todas las tablas de dimensiones, relaciones y hechos, asegurando que no se generen duplicados.

    Parámetros:
    -----------
    None

    Retorna:
    --------
    None

    Proceso:
    --------
    1. Genera la tabla única de países y continentes (`df_paises_continentes_totales`).
    2. Inserta o actualiza las tablas de dimensiones:
        - `pais_destino`
        - `itinerario`
        - `ciudad`
        - `ciudad_itinerario`
    3. Inserta o actualiza las tablas fact:
        - `viaje`
        - `precio_viaje`
        - `combinacion_destino_viaje`
        - `turismo_emisor`
    4. Cada subfunción interna se encarga de evitar duplicados y resolver claves foráneas.
    5. Imprime por consola el progreso y confirma la finalización.

    Notas:
    ------
    - Esta función es el punto de entrada ideal para ejecutar el proceso de carga completo tras el scraping y procesamiento.
    - Depende del módulo `lo`, que debe contener todas las funciones de carga (`carga_tabla_*`) y la de obtención de países–continentes.
    - Asegura el orden lógico de carga respetando dependencias (por ejemplo, ciudades después de países).
    """
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