import pandas as pd
import requests as rq
import numpy as np
import os
from dotenv import load_dotenv
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
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
    """
    Consulta la API de OpenCage para obtener el país, continente y coordenadas geográficas (latitud y longitud) 
    correspondientes a un municipio.

    Parámetros:
    -----------
    municipio : str
        Nombre de la ciudad o localidad a geolocalizar.

    API_KEY : str
        Clave de acceso para autenticar la consulta a la API de OpenCage.

    Retorna:
    --------
    tuple
        Una tupla con 4 elementos:
        - continente : str
        - país : str
        - latitud : float o str
        - longitud : float o str

        Si no se encuentra información, se devuelve 'Información desconocida' en cada campo.

    Proceso:
    --------
    - Realiza una consulta HTTP a la API de OpenCage con el nombre del municipio.
    - Si la respuesta es válida (`status_code == 200`) y contiene resultados:
        - Extrae el continente, país, latitud y longitud desde el primer resultado.
        - Usa `.get()` para evitar errores si falta alguna clave.
    - Si no hay resultados, retorna valores por defecto indicando que no se obtuvo información.
    - Si hay un error de conexión, imprime un mensaje.

    Notas:
    ------
    - El idioma de la respuesta se fija en español (`language=es`).
    - Es recomendable implementar control de errores adicional para entornos productivos (ej. `try/except`).
    - Para uso masivo, se recomienda controlar el límite de peticiones de OpenCage (por defecto gratuito: 2.500 peticiones/día).
    """
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
    """
    Consulta la API de OpenCage para obtener el continente, país, latitud y longitud de una ciudad 
    utilizando tanto el nombre del municipio como del país. Si no se obtiene un resultado satisfactorio, 
    realiza una segunda consulta solo con el municipio.

    Parámetros:
    -----------
    municipio : str
        Nombre de la ciudad o localidad a geolocalizar.

    pais : str
        Nombre del país al que supuestamente pertenece el municipio.

    API_KEY : str
        Clave de acceso para autenticar la consulta a la API de OpenCage.

    Retorna:
    --------
    tuple
        Una tupla con cinco elementos:
        - continente : str
        - país : str (devuelto por la API)
        - latitud : float o str
        - longitud : float o str
        - resultado : str, indicador del estado del resultado
            - 'Ciudad y pais encontrado'
            - 'Ciudad encontrada en otro pais'
            - 'Ciudad no encontrada'
            - 'Error'

    Proceso:
    --------
    1. Realiza una consulta a la API con `municipio + pais` como término de búsqueda.
    2. Si no encuentra el continente en la respuesta:
        - Realiza una segunda consulta solo con el `municipio`.
        - Extrae y retorna los datos con una etiqueta de que se ha encontrado en otro país.
    3. Si en la primera consulta el continente está presente:
        - Extrae y retorna la información completa.
    4. Si no hay resultados en ninguna de las consultas:
        - Devuelve valores de error indicando 'Ciudad no encontrada'.
    5. Si hay error de conexión:
        - Imprime un mensaje y devuelve un resultado de tipo 'Error'.

    Notas:
    ------
    - El idioma de la respuesta se fija en español (`language=es`).
    - Se usan `.get()` para acceder de forma segura a los campos del JSON.
    - Ideal para validar y complementar datos geográficos de scraping o APIs incompletas.
    - El parámetro `resultado` es útil para trazabilidad o debugging.

    Ejemplo de uso:
    ---------------
    >>> obtener_pais_continente_lat_long_con_pais("Praga", "Republica Checa", API_KEY)
    ('Europa', 'Chequia', 50.0755, 14.4378, 'Ciudad y pais encontrado')
    """
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
    """
    Normaliza una cadena de texto eliminando tildes, pasando todo a minúsculas y limpiando espacios innecesarios.

    Parámetros:
    -----------
    x : str
        Cadena de texto a normalizar.

    Retorna:
    --------
    str
        Cadena normalizada. Si el parámetro no es un string, se devuelve tal cual.

    Proceso:
    --------
    - Convierte todo el texto a minúsculas.
    - Elimina tildes y caracteres especiales usando `unicodedata`.
    - Sustituye múltiples espacios por uno solo.
    - Elimina espacios en blanco al principio y al final.

    Ejemplo:
    --------
    >>> normalizar_texto("   Córdoba  ")
    'cordoba'
    
    >>> normalizar_texto("niÑo   JUGÓ")
    'nino jugo'
    """
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
    """
    Convierte una cadena de texto a formato "Title Case", es decir, con la primera letra de cada palabra en mayúscula.

    Parámetros:
    -----------
    x : str
        Cadena de texto a transformar.

    Retorna:
    --------
    str
        Texto transformado con cada palabra capitalizada. 
        Si el valor no es una cadena (`str`), se devuelve tal como está.

    Proceso:
    --------
    - Usa `.title()` para poner en mayúscula la primera letra de cada palabra.
    - Mantiene los demás caracteres en minúscula automáticamente (excepto siglas o abreviaturas).

    Ejemplo:
    --------
    >>> capitalizar_texto("ciudad de méxico")
    'Ciudad De México'

    >>> capitalizar_texto("niÑo jugó en el parque")
    'Niño Jugó En El Parque'
    """
    if isinstance(x, str):
        # Poner en mayúscula
        #x = ' '.join([palabra.capitalize() for palabra in x.split()])
        return x.title()
    return x


def limpieza_fichero_turismo_emisor(df_turismo_emisor):
    """
    Limpia y estandariza un DataFrame de turismo emisor eliminando registros irrelevantes 
    y ajustando los nombres de países para evitar duplicados en procesos posteriores.

    Parámetros:
    -----------
    df_turismo_emisor : pandas.DataFrame
        DataFrame original con los datos brutos del turismo emisor (extraídos de Dataestur).

    Retorna:
    --------
    pandas.DataFrame
        DataFrame limpio y procesado, listo para ser cargado en base de datos.

    Proceso:
    --------
    1. Elimina registros cuyo destino sea 'Total' o 'Otros' (no representan países concretos).
    2. Sustituye nombres de países específicos para alinearlos con los nombres usados en el scraping:
        - Ej: 'Estados Unidos de América' → 'Estados Unidos'
    3. Guarda el DataFrame limpio como CSV en la ruta definida por la variable de entorno `ARCHIVO_GUARDAR_DATOS_API_PROCESADOS`.

    Notas:
    ------
    - Esta función modifica el DataFrame original `inplace=True`.
    - Es una etapa previa necesaria para asegurar integridad y consistencia en la unión de fuentes (API vs Web Scraping).
    - La lista de reemplazos puede ajustarse si se detectan más conflictos de nomenclatura en el futuro.

    Ejemplo de uso:
    ---------------
    >>> df_limpio = limpieza_fichero_turismo_emisor(df_raw)
    """
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
    'Zimbabwe':'Zimbabue',
    'Botswana':'Botsuana'})


    ARCHIVO_GUARDAR_DATOS_API_PROCESADOS=os.getenv('ARCHIVO_GUARDAR_DATOS_API_PROCESADOS')
    df_turismo_emisor.to_csv(ARCHIVO_GUARDAR_DATOS_API_PROCESADOS) #guardo el fichero
    print(f'Los datos de turismo emisor han sido procesados y guardados: {len(df_turismo_emisor)}')
    return df_turismo_emisor

def obtención_continentes_correctos(df_continentes, lista_paises_islas, ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY):
    """
    Corrige los continentes de ciertos países clasificados erróneamente como 'Caribe', 'Oriente Medio' o 'Islas Exóticas' 
    utilizando datos de geolocalización obtenidos desde la API de OpenCage.

    Parámetros:
    -----------
    df_continentes : pandas.DataFrame
        DataFrame original con los países y continentes escrapeados, donde algunos continentes pueden ser genéricos o incorrectos.

    lista_paises_islas : list of str
        Lista de países que necesitan revisión (por estar asociados a continentes ambiguos como 'Caribe').

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE : str
        Ruta al archivo `.pkl` donde se almacenan los resultados acumulados de llamadas a la API de OpenCage.

    API_KEY : str
        Clave de autenticación para acceder a la API de OpenCage.

    Retorna:
    --------
    tuple
        - df_continentes_agrupado_geolocalizacion_api : pandas.DataFrame  
            Información de geolocalización actualizada (continente, país, latitud, longitud) de todos los registros consultados.
        
        - df_continentes : pandas.DataFrame  
            DataFrame actualizado con los continentes corregidos en base a los datos de la API.

    Proceso:
    --------
    1. Si existen datos previos de la API (`.pkl`), se integran en `df_continentes`.
    2. Se identifican nuevamente los países con continentes genéricos para su revisión.
    3. Si aún hay países con continentes incorrectos:
        - Se consulta la API de OpenCage por cada uno.
        - Se reemplazan nombres de continentes del inglés al español.
        - Se actualiza `df_continentes` con los continentes reales obtenidos.
    4. Se guarda:
        - El resultado completo de llamadas a la API (actual y anteriores).
        - El DataFrame final corregido (`df_continentes`).

    Notas:
    ------
    - La función requiere `tr.obtener_pais_continente_lat_long()` y `ex.incorporar_información_df_original()`.
    - Es robusta a múltiples ejecuciones: guarda los resultados y evita repetir llamadas innecesarias a la API.
    - Asume que `df_continentes` tiene una columna `continente` y `pais`.

    Ejemplo de aplicación:
    ----------------------
    Ideal para limpiar etiquetas de continentes como 'Islas Exóticas', que no existen oficialmente, 
    y reemplazarlas por 'Oceanía', 'África', etc. según datos reales de geolocalización.
    """ 
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
    """
    Limpia el DataFrame de continentes escrapeados corrigiendo las asignaciones de países mal etiquetados 
    con continentes ficticios como 'Caribe', 'Oriente Medio' o 'Islas Exóticas', utilizando la API de OpenCage.

    Parámetros:
    -----------
    df_continentes : pandas.DataFrame
        DataFrame con los países y continentes obtenidos del scraping, que puede contener etiquetas no oficiales.

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE : str
        Ruta al archivo `.pkl` que almacena los resultados previos o nuevos de geolocalización.

    API_KEY : str
        Clave de autenticación para realizar consultas a la API de OpenCage.

    Retorna:
    --------
    tuple
        - df_continentes_agrupado_geolocalizacion_api : pandas.DataFrame  
            Resultado consolidado de llamadas a la API, con continentes corregidos y metadatos de ubicación.

        - df_continentes : pandas.DataFrame  
            DataFrame original actualizado con los continentes corregidos.

    Proceso:
    --------
    1. Filtra el DataFrame para obtener una lista única de países con continentes no oficiales.
    2. Llama a `obtenención_continentes_correctos()` para obtener los continentes reales vía API.
    3. Aplica los resultados al DataFrame original.
    4. Devuelve tanto el DataFrame corregido como el histórico de llamadas a la API.

    Notas:
    ------
    - Utiliza la función `tr.obtenención_continentes_correctos` como dependencia principal.
    - Esta función es útil para garantizar consistencia geográfica y evitar errores en análisis posteriores.
    - Permite mantener un histórico de geolocalización en disco para evitar llamadas repetidas a la API.
    """
    df_continentes_reducido = df_continentes[['continente', 'pais']].drop_duplicates() #elimino duplicados
    #obtengo los paises/islas que están en continentes ficticios:
    lista_paises_islas = df_continentes_reducido[df_continentes_reducido.continente.isin(['Caribe', 'Oriente Medio', 'Islas Exóticas'])].pais.tolist()
    df_continentes_agrupado_geolocalizacion_api, df_continentes= tr.obtención_continentes_correctos(df_continentes, lista_paises_islas, ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE, API_KEY)
    print('La limpieza de los continentes escrapeados ha finalizado')
    return df_continentes_agrupado_geolocalizacion_api,df_continentes

def correccion_mismonombre_diferenteurl(df_viajes_agrupados):
    """
    Detecta viajes que tienen el mismo nombre pero diferentes URLs, y marca aquellos casos donde es probable que
    se haya producido una actualización de URL (por ejemplo, cambios en el sitio web de TUI).

    Parámetros:
    -----------
    df_viajes_agrupados : pandas.DataFrame
        DataFrame con información de viajes, que debe incluir las columnas:
        - `nombre_viaje`
        - `url_viaje`
        - `en_ultimo_escrapeo`
        - `duracion_viaje`
        - `itinerario`

    Retorna:
    --------
    tuple
        - dict_cambios_url : dict
            Diccionario con los viajes duplicados por nombre que presentan URLs distintas.
        - df_cambios_url : pandas.DataFrame
            Versión en DataFrame del mismo diccionario, útil para inspección o exportación.

    Proceso:
    --------
    - Agrupa los viajes por `nombre_viaje`.
    - Para aquellos con más de una URL:
        - Verifica si tienen el mismo itinerario y duración (indicando que son realmente el mismo viaje).
        - Compara si uno de ellos estaba en el último escrapeo (`en_ultimo_escrapeo == True`) y otro no.
        - Registra el cambio de URL para su posible actualización en la base de datos.

    Notas:
    ------
    - Esta función no actualiza directamente la base de datos, solo detecta los casos sospechosos.
    - Puede usarse como paso previo a una limpieza de viajes inactivos o desduplicación lógica.
    - Los resultados permiten tomar decisiones informadas sobre cuál URL conservar como válida.
    - Útil en procesos de mantenimiento donde la web ha cambiado estructuras o URLs de productos.

    Ejemplo de uso:
    ---------------
    >>> dict_cambios, df_cambios = correccion_mismonombre_diferenteurl(df_viajes)
    """
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
    """
    Detecta viajes que tienen la misma URL pero nombres diferentes, indicando posibles actualizaciones de título 
    (por ejemplo, cambios de nombre en la web oficial sin modificar la URL del viaje).

    Parámetros:
    -----------
    df_viajes_agrupados : pandas.DataFrame
        DataFrame que debe contener, al menos, las columnas:
        - `url_viaje`
        - `nombre_viaje`
        - `en_ultimo_escrapeo`

    Retorna:
    --------
    tuple
        - dict_cambios_nombre_viaje : dict
            Diccionario con las URLs afectadas por cambios de nombre y su estado en el último escrapeo.
        
        - df_cambios_nombre_viaje : pandas.DataFrame
            DataFrame construido a partir del diccionario, útil para inspección visual o exportación.

    Proceso:
    --------
    - Agrupa los registros por `url_viaje`.
    - Identifica aquellas URLs que presentan más de un `nombre_viaje`.
    - Determina cuál es el nombre más reciente observando la columna `en_ultimo_escrapeo`.
    - Guarda la información en un diccionario estructurado.

    Notas:
    ------
    - Esta función no realiza ninguna modificación en los datos ni en la base de datos, solo detecta inconsistencias.
    - Es útil para aplicar lógicas de actualización de nombre de viaje en futuras cargas.
    - Puede servir para documentar cambios comerciales o renombramientos por parte de TUI en su portal.

    Ejemplo de uso:
    ---------------
    >>> dict_nombres, df_nombres = correccion_duplicados_mismaurl_diferente_nombre(df_viajes)
    """
    dict_cambios_nombre_viaje= {"url_viaje":[],'nombre_anterior':[], 'nombre_nuevo':[], 'nombre_anterior_en_ultimo_escrapeo':[],
                                 'nombre_nuevo_en_ultimo_escrapeo':[]}
    for url in df_viajes_agrupados.url_viaje.unique():
        df_filtrado = df_viajes_agrupados[df_viajes_agrupados.url_viaje == url]
        if len(df_filtrado.nombre_viaje.unique()) >1:
            #print(url)
            #print(len(df_filtrado.nombre_viaje.unique()))
            nombre1= df_filtrado.nombre_viaje.unique()[-2]
            nombre2=df_filtrado.nombre_viaje.unique()[-1]
            if len(df_filtrado.en_ultimo_escrapeo.unique())>1:
                en_ultimo_escrapeo_anterior = df_filtrado.en_ultimo_escrapeo.unique()[-2]
                en_ultimo_escrapeo_nuevo = df_filtrado.en_ultimo_escrapeo.unique()[-1]
            else:
                en_ultimo_escrapeo_anterior = df_filtrado.en_ultimo_escrapeo.unique()[-1]
                en_ultimo_escrapeo_nuevo = df_filtrado.en_ultimo_escrapeo.unique()[-1]
            #print(nombre1)
            #print(nombre2)
            dict_cambios_nombre_viaje['url_viaje'].append(url)
            dict_cambios_nombre_viaje['nombre_anterior'].append(nombre1)
            dict_cambios_nombre_viaje['nombre_nuevo'].append(nombre2)
            dict_cambios_nombre_viaje['nombre_anterior_en_ultimo_escrapeo'].append(en_ultimo_escrapeo_anterior)
            dict_cambios_nombre_viaje['nombre_nuevo_en_ultimo_escrapeo'].append(en_ultimo_escrapeo_nuevo)
    df_cambios_nombre_viaje = pd.DataFrame(dict_cambios_nombre_viaje)
    print(f'Los viajes duplicados con nombres diferentes han sido actualizados: {len(df_cambios_nombre_viaje)}')
    return dict_cambios_nombre_viaje, df_cambios_nombre_viaje

def limpieza_viajes_finales (df_viajes, df_opciones):
    """
    Realiza la limpieza, consolidación y normalización final del dataset de viajes, unificando los viajes base y sus opciones,
    corrigiendo duplicidades y dejando el conjunto preparado para su carga y análisis.

    Parámetros:
    -----------
    df_viajes : pandas.DataFrame
        DataFrame original de viajes extraídos directamente desde el scraping principal.

    df_opciones : pandas.DataFrame
        DataFrame con las opciones asociadas a los viajes, como variantes o versiones alternativas.

    Retorna:
    --------
    df_viajes_agrupados : pandas.DataFrame
        DataFrame final consolidado y limpio con todos los viajes, listo para ser cargado o analizado.

    Proceso:
    --------
    1. Elimina registros del scraping que contienen `'opciones'` en la URL (se reemplazan por datos más completos).
    2. Transforma el DataFrame de opciones a un formato homogéneo y lo concatena con los viajes base.
    3. Aplica correcciones a:
        - Viajes con **mismo nombre pero diferentes URLs** (`correccion_mismonombre_diferenteurl`).
        - Viajes con **misma URL pero diferentes nombres** (`correccion_duplicados_mismaurl_diferente_nombre`).
    4. Sustituye URLs y nombres antiguos por los nuevos en los casos detectados.
    5. Elimina registros incompletos (con `NaN` en `duracion_viaje` o `precio`).
    6. Limpia y transforma la columna `itinerario` para facilitar la extracción posterior de ciudades:
        - Elimina puntos.
        - Reemplaza conectores (`y`, `e`, paréntesis) por comas.
    7. Guarda el resultado final como un archivo `.pkl`, cuya ruta está definida por la variable de entorno 
       `ARCHIVO_GUARDAR_ESCRAPEO_VIAJES_PROCESADOS`.

    Notas:
    ------
    - Requiere funciones auxiliares del módulo `tr` (transformaciones) y `ex` (extracciones y reemplazos).
    - Esta función actúa como una etapa de preprocesado clave antes de insertar los datos en base de datos o analizarlos.
    - La transformación del itinerario es crucial para la identificación de ciudades en pasos posteriores.

    Ejemplo de uso:
    ---------------
    >>> df_final = limpieza_viajes_finales(df_viajes_raw, df_opciones_raw)
    """
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
    ex.incorporar_información_df_original(df_viajes_agrupados, df_cambios_nombre_viaje,
                                      'url_viaje', 'nombre_nuevo_en_ultimo_escrapeo', 'en_ultimo_escrapeo', 
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
    """
    Convierte un string de itinerario en una lista de ciudades, separando por comas y eliminando espacios innecesarios.

    Parámetros:
    -----------
    valor : str
        Cadena de texto que representa un itinerario, con las ciudades separadas por comas.

    Retorna:
    --------
    list of str
        Lista de nombres de ciudades, limpios de espacios adicionales.

    Proceso:
    --------
    - Divide el texto original por comas.
    - Aplica `.strip()` a cada elemento para eliminar espacios en blanco alrededor.
    - Retorna una lista con los nombres de ciudades en orden.

    Ejemplo:
    --------
    >>> conversion_itineario_en_lista("Madrid, París , Roma ")
    ['Madrid', 'París', 'Roma']
    """
    valor = valor.split(',')  # convertir string en lista, separando por comas
    valor = [v.strip() for v in valor]  # quitar espacios extra
    return valor

def asignacion_pais_itinerarios_con_un_pais(df_itinerarios):
    """
    Identifica los itinerarios que pertenecen exclusivamente a un único país 
    y asigna dicho país como el país correcto para todo el itinerario.

    Parámetros:
    -----------
    df_itinerarios : pandas.DataFrame
        DataFrame que debe contener las columnas:
        - `itinerario_modificado_para_dividir` (str): itinerario como cadena procesada.
        - `lista_itinerario` (list): lista de ciudades en el itinerario.
        - `pais` (str): país asociado a cada ciudad del itinerario.

    Retorna:
    --------
    df_pais_correcto_itinerario_un_pais : pandas.DataFrame
        DataFrame con tres columnas:
        - `itinerario_modificado_para_dividir`
        - `lista_itinerario`
        - `pais_correcto` (único país al que pertenece todo el itinerario)

    Proceso:
    --------
    - Agrupa los registros por `itinerario_modificado_para_dividir`.
    - Para cada itinerario, verifica si todas las ciudades pertenecen al mismo país (`len(pais.unique()) == 1`).
    - En caso afirmativo, extrae la lista de ciudades y el país correspondiente.
    - Almacena la información en un diccionario y lo convierte en DataFrame.

    Notas:
    ------
    - Esta función solo considera itinerarios sin ambigüedad geográfica.
    - Es útil para asignación automática de país a itinerarios completos sin requerir confirmación manual.
    - Se espera que la columna `lista_itinerario` esté previamente calculada con, por ejemplo, `conversion_itineario_en_lista`.

    Ejemplo:
    --------
    >>> df_resultado = asignacion_pais_itinerarios_con_un_pais(df_itinerarios)
    """
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
    """
    Descompone cada itinerario en registros individuales por ciudad, 
    manteniendo la relación con el país correcto asignado previamente.

    Parámetros:
    -----------
    df_pais_correcto_itinerario_a_dividir : pandas.DataFrame
        DataFrame que debe contener al menos las columnas:
        - `itinerario_modificado_para_dividir` (str): itinerario original.
        - `lista_itinerario` (list): lista de ciudades.
        - `pais_correcto` (str): país asociado al itinerario completo.

    Retorna:
    --------
    df_pais_correcto_itinerario_ciudad_dividida : pandas.DataFrame
        DataFrame con una fila por ciudad, conteniendo:
        - `itinerario_modificado_para_dividir`
        - `ciudad` (nombre limpio)
        - `pais_correcto`

    Proceso:
    --------
    - Aplica `explode()` a la columna `lista_itinerario` para separar las ciudades en filas individuales.
    - Renombra esa columna a `ciudad`.
    - Elimina espacios duplicados y bordes innecesarios en los nombres de ciudad.

    Notas:
    ------
    - Esta función es útil para construir relaciones ciudad–país derivadas de un itinerario confirmado.
    - Se asume que `lista_itinerario` contiene listas de strings.

    Ejemplo:
    --------
    >>> division_ciudades(df_resultado_itinerarios)
    """
    #divido las ciudades para asignar el pais a las ciudades:
    df_pais_correcto_itinerario_ciudad_dividida= df_pais_correcto_itinerario_a_dividir.explode('lista_itinerario') # Explode para deshacer las listas en filas
    df_pais_correcto_itinerario_ciudad_dividida = df_pais_correcto_itinerario_ciudad_dividida.rename(columns={'lista_itinerario': 'ciudad'}) # Renombro la columna a ciudad
    df_pais_correcto_itinerario_ciudad_dividida['ciudad'] = df_pais_correcto_itinerario_ciudad_dividida['ciudad'].str.replace(r'\s+', ' ', regex=True).str.strip() #eliminar espacios de delante y duplicados entre dos palabras
    return df_pais_correcto_itinerario_ciudad_dividida

def asignacion_pais_itinerarios_duplicados_en_paises (df_itinerarios_duplicados_en_paises,
                                                      API_KEY,ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE):
    """
    Asigna automáticamente el país correcto a las ciudades de itinerarios en los que aparecen países múltiples,
    utilizando datos previamente obtenidos o llamando a la API de geolocalización de OpenCage si es necesario.

    Parámetros:
    -----------
    df_itinerarios_duplicados_en_paises : pandas.DataFrame
        DataFrame con las columnas:
        - `itinerario_modificado_para_dividir` (str)
        - `ciudad` (str)
        - `pais_correcto` (str, puede contener valores nulos)

    API_KEY : str
        Clave de acceso a la API de OpenCage para realizar geolocalización.

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE : str
        Ruta al archivo `.pkl` donde se guarda y consulta el histórico de llamadas a la API para evitar repeticiones.

    Retorna:
    --------
    tuple
        - df_itinerarios_duplicados_en_paises : pandas.DataFrame  
          Con las columnas `itinerario_modificado_para_dividir`, `ciudad`, `pais_correcto`, actualizado con los países asignados.
        
        - df_agrupado_geolocalizacion_api : pandas.DataFrame  
          DataFrame consolidado con todas las llamadas a la API realizadas (histórico + nuevas).

    Proceso:
    --------
    1. Filtra las ciudades sin país asignado (`pais_correcto is null`).
    2. Revisa si ya existe información de esas ciudades en el histórico local de geolocalización (`.pkl`).
       - Si la encuentra, la incorpora.
       - Si no, consulta la API de OpenCage.
    3. Asocia el resultado (`pais_api`) como `pais_correcto`.
    4. Reemplaza los nombres de continentes del inglés al español (si se usan).
    5. Guarda el histórico actualizado de geolocalización en disco.

    Notas:
    ------
    - Optimiza las llamadas a la API reutilizando datos ya almacenados.
    - Útil en casos donde un itinerario pasa por varias ciudades que pertenecen a diferentes países y no puede inferirse un único país automáticamente.
    - Esta función no transforma los nombres de ciudad ni hace validaciones de exactitud semántica.

    Ejemplo:
    --------
    >>> df_corr, df_api = asignacion_pais_itinerarios_duplicados_en_paises(df_ambiguos, API_KEY, "ruta/api_geolocalizacion.pkl")
    """
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
    """
    Desglosa los itinerarios de los viajes en ciudades individuales y les asigna su país correspondiente, 
    utilizando reglas de inferencia y, en caso necesario, llamadas a la API de OpenCage. 
    Guarda el resultado en un archivo `.pkl`.

    Parámetros:
    -----------
    df_viajes_agrupados : pandas.DataFrame
        DataFrame con los viajes agrupados, que debe contener:
        - 'pais'
        - 'itinerario_modificado_para_dividir'

    df_agrupado_geolocalizacion_api : pandas.DataFrame
        Histórico de resultados de llamadas anteriores a la API de OpenCage.

    API_KEY : str
        Clave de acceso para realizar consultas a la API de OpenCage.

    ARCHIVO_GUARDAR_ESCRAPEO_API_OPENCAGE : str
        Ruta del archivo `.pkl` donde se guarda el histórico actualizado de geolocalización.

    Retorna:
    --------
    df_itinerario_ciudades_completo : pandas.DataFrame
        DataFrame final con:
        - 'itinerario_modificado_para_dividir'
        - 'ciudad'
        - 'pais_correcto'

    Proceso:
    --------
    1. Elimina duplicados por itinerario y convierte el string del itinerario en una lista de ciudades (`lista_itinerario`).
    2. Identifica itinerarios asociados a un único país y les asigna ese país directamente.
    3. Divide estos itinerarios en filas individuales por ciudad.
    4. Para los itinerarios con ciudades de varios países:
        - Intenta completar la asignación con información ya procesada.
        - Realiza llamadas a la API de OpenCage si es necesario.
        - Asigna el país a cada ciudad individualmente.
    5. Combina ambas fuentes de información (asignación directa + API).
    6. Corrige valores problemáticos (ej: "Estados Unidos de América" → "Estados Unidos").
    7. Guarda el DataFrame resultante en la ruta definida por la variable de entorno `ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS`.

    Notas:
    ------
    - Usa funciones auxiliares del módulo `tr` (transformaciones) y `ex` (extracción o fusión de información).
    - Esta función es clave para construir una tabla relacional de ciudades y países basada en los itinerarios.
    - El resultado puede ser usado para alimentar una tabla intermedia `ciudad_itinerario`.

    Ejemplo de uso:
    ---------------
    >>> df_ciudades = desglosar_ciudades_itinerarios(df_viajes_agrupados, df_api_cache, API_KEY, "api_cache.pkl")
    """
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
    """
    Desglosa los itinerarios de los viajes en ciudades individuales y les asigna su país correspondiente 
    utilizando tres fuentes: comparación directa, información geolocalizada previa y consultas a la API de OpenCage.
    Guarda el resultado final en un archivo `.pkl`.

    Parámetros:
    -----------
    df_viajes_agrupados : pandas.DataFrame
        DataFrame con los viajes, que debe contener:
        - 'pais'
        - 'itinerario_modificado_para_dividir'

    API_KEY : str
        Clave de acceso para realizar consultas a la API de geolocalización de OpenCage.

    ARCHIVO_GUARDAR_TOTAL_CIUDADES_API : str
        Ruta al archivo `.pkl` con resultados de llamadas anteriores a la API por ciudad (sin país asociado).

    ARCHIVO_EXTRACCION_API_DUPLICADOS : str
        Ruta al archivo `.pkl` con resultados de llamadas anteriores a la API con ciudad + país como clave (ciudades ambiguas).

    Retorna:
    --------
    df_itinerarios_ciudades_completo : pandas.DataFrame
        DataFrame final con:
        - 'itinerario_modificado_para_dividir'
        - 'ciudad'
        - 'pais_correcto'

    Proceso:
    --------
    1. Elimina duplicados de itinerarios y convierte cada uno en lista de ciudades (`lista_itinerario`).
    2. Explota esa lista y normaliza los nombres de las ciudades.
    3. Asigna país (`pais_api`) a partir de datos anteriores (ARCHIVO_GUARDAR_TOTAL_CIUDADES_API).
    4. Compara `pais_api` con el país original del viaje (`pais`):
        - Si coinciden, lo considera correcto.
        - Si no, intenta asignar con ARCHIVO_EXTRACCION_API_DUPLICADOS (ciudades con país ambiguo).
    5. Si aún quedan ciudades sin país asignado, realiza nuevas llamadas a la API de OpenCage usando la ciudad y su país original.
    6. Normaliza todos los valores devueltos por la API.
    7. Establece una columna `pais_correcto` final para cada ciudad.
    8. Reemplaza valores erróneos o inconsistentes (e.g. "Estados Unidos de América" → "Estados Unidos").
    9. Guarda el resultado en el archivo `.pkl` definido por `ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1`.

    Notas:
    ------
    - Usa funciones auxiliares de los módulos `tr` (transformación) y `ex` (fusión de datos).
    - Realiza geolocalización solo si no encuentra resultados previos, optimizando el uso de la API.
    - La función es especialmente útil para limpiar datos ambiguos y garantizar coherencia entre ciudad y país.

    Ejemplo:
    --------
    >>> df_final = desglosar_ciudades_itinerarios_2(df_viajes_agrupados, API_KEY, "ciudades_api.pkl", "ciudades_duplicadas_api.pkl")
    """
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
             'Estados Unidos De America':'Estados Unidos',
             'Spain': 'España',
             'Arabia Saudita': 'Arabia Saudí',
            'Fiyi': 'Fiji',
            'France': 'Francia',
            'Islas Turcas y Caicos': 'Turks and Caicos',
            'Islas Turcas Y Caicos': 'Turks and Caicos',
            'Papua-Nueva Guinea': 'Papua Nueva Guinea',
            'Zimbawe': 'Zimbabue',
            'Chipre Del Norte': 'Chipre',
            'Chipre del Norte / Chipre': 'Chipre',
            'Chipre Del Norte / Chipre': 'Chipre',
            'Botswana': 'Botsuana'})

    print(df_paises_api_todas_ciudades.columns)
    #incorporo la información al df_itinerarios_original
    df_itinerarios_ciudades_completo = df_itinerarios_ciudades[['itinerario_modificado_para_dividir', 'ciudad']]
    df_itinerarios_ciudades_completo['pais_correcto']= None
    ex.incorporar_información_df_original(df_itinerarios_ciudades_completo, df_itinerarios_ciudades_sin_lista,
                                           'ciudad','pais_final','pais_correcto')
    print(f'Se ha informado el pais correcto a cada ciudad de cada itinerario:{len(df_itinerarios_ciudades_completo)}')

    df_itinerarios_ciudades_completo['itinerario_modificado_para_dividir'] = df_itinerarios_ciudades_completo[
        'itinerario_modificado_para_dividir'].str.replace(r'\s+', ' ', regex=True).str.strip() #eliminar espacios de delante y duplicados entre dos palabras

    ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1=os.getenv('ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1')
    df_itinerarios_ciudades_completo.to_pickle(ARCHIVO_GUARDAR_ITINERARIOS_PROCESADOS_1)
    print(f'Se ha guardado la informacion de itinerarios y ciudades:{len(df_itinerarios_ciudades_completo)} ')
    return df_itinerarios_ciudades_completo
  

def transformacion_total():
    """
    Ejecuta el proceso completo de transformación y limpieza de los datos extraídos de scraping y de fuentes oficiales,
    preparando los datasets para su posterior carga en base de datos o análisis.

    Este pipeline automatiza la transformación de:
    - Datos de viajes y sus opciones desde la web de TUI.
    - Datos de turismo emisor desde la API de Dataestur.
    - Datos geográficos (países y continentes) obtenidos por scraping o APIs externas.

    Parámetros:
    -----------
    No recibe parámetros externos. Todas las rutas y claves se cargan automáticamente desde variables de entorno.

    Retorna:
    --------
    tuple :
        - df_turismo_emisor_procesado (pd.DataFrame):  
          Datos limpios de turismo emisor provenientes de la API oficial.
        - df_continentes_procesado (pd.DataFrame):  
          Información procesada de continentes corregidos y normalizados.
        - df_viajes_agrupados (pd.DataFrame):  
          Dataset completo de viajes, deduplicado, corregido y enriquecido con opciones.
        - df_itinerario_ciudades_completo (pd.DataFrame):  
          Resultado del desglose de itinerarios en ciudades y asignación del país correspondiente.

    Proceso:
    --------
    1. Carga rutas y credenciales desde variables de entorno (`.env`).
    2. Importa los ficheros `pickle` y `.csv` generados en la fase de extracción (scraping + APIs).
    3. Aplica limpieza al archivo de turismo emisor (`tr.limpieza_fichero_turismo_emisor`).
    4. Corrige los continentes ficticios o mal asignados mediante la API de OpenCage (`tr.limpieza_continentes_escrapeados`).
    5. Limpia y agrupa los datos de viajes y opciones (`tr.limpieza_viajes_finales`).
    6. Desglosa los itinerarios en ciudades y asigna países (`tr.desglosar_ciudades_itinerarios`).
    7. Devuelve los principales dataframes transformados para su posterior uso.

    Notas:
    ------
    - El flujo depende de múltiples funciones del módulo `tr` (transformaciones) y del uso correcto del archivo `.env`.
    - Esta función es el paso previo a la función `carga_total()`, que carga los resultados a la base de datos.
    - El proceso es reproducible siempre que se mantengan las rutas de acceso y los datos fuente.

    Ejemplo:
    --------
    >>> df_api, df_continentes, df_viajes, df_ciudades = transformacion_total()
    """
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
    """
    Ejecuta el flujo completo de transformación de los datos extraídos por scraping y API, utilizando 
    la versión más robusta de desglose y asignación de países a ciudades (`desglosar_ciudades_itinerarios_2`).

    Este pipeline transforma:
    - Datos de viajes y sus opciones desde la web de TUI.
    - Datos de turismo emisor desde la API de Dataestur.
    - Datos geográficos de países y continentes extraídos por scraping o geolocalización.
    - Itinerarios en listas de ciudades con país asignado usando validaciones y la API de OpenCage.

    Parámetros:
    -----------
    No recibe argumentos directamente. Todas las rutas y claves se cargan desde variables de entorno definidas en `.env`.

    Retorna:
    --------
    tuple :
        - df_turismo_emisor_procesado (pd.DataFrame):  
          Datos limpios de turismo emisor tras validación y estandarización.
        - df_continentes_procesado (pd.DataFrame):  
          Continentes corregidos y normalizados (evitando valores ficticios).
        - df_viajes_agrupados (pd.DataFrame):  
          Información final de viajes, unificada con opciones, sin duplicados ni errores.
        - df_itinerario_ciudades_completo (pd.DataFrame):  
          Tabla resultante con cada ciudad del itinerario y su país asignado correctamente.

    Proceso:
    --------
    1. Carga rutas y claves de entorno.
    2. Lee los ficheros extraídos: viajes, opciones, continentes y turismo emisor.
    3. Limpia el fichero de turismo emisor para estandarizar nombres y eliminar totales.
    4. Corrige los continentes con ayuda de la API de OpenCage (para casos como "Caribe", "Islas Exóticas"...).
    5. Agrupa y limpia los viajes, integrando también las opciones disponibles por TUI.
    6. Desglosa cada itinerario en ciudades y asigna el país correcto a cada ciudad:
        - Comparación directa.
        - Ficheros históricos de llamadas a la API.
        - Nuevas llamadas a la API si no hay coincidencias anteriores.
    7. Guarda los resultados transformados y devuelve los principales dataframes.

    Notas:
    ------
    - Esta versión usa `desglosar_ciudades_itinerarios_2()`, que permite una validación más completa.
    - Pensada como paso previo a la función `carga_total()`, que insertará los resultados en la base de datos.
    - La función espera que el entorno esté correctamente configurado y que los archivos `.pkl` y `.csv` de la fase de extracción existan.

    Ejemplo de uso:
    ---------------
    >>> df_api, df_continentes, df_viajes, df_ciudades = transformacion_total_1()
    """
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





