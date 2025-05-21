import pandas as pd
import numpy as np
import psycopg2
import os
from dotenv import load_dotenv
import sys #permite navegar por el sistema
sys.path.append("../") #solo aplica al soporte
import os
from dotenv import load_dotenv
import unicodedata
from src.etl import load as lo #con jupyter

load_dotenv()

#Llamamos a la variable de entorno
DB_NAME = os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST= os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

def extraer_datos_bbdd_y_convertir_en_df (lista_nombre_columnas, query_extracción,dbname=DB_NAME, user=DB_USER,
                           password=DB_PASSWORD,host=DB_HOST, port=DB_PORT):

    conn, cur=lo.crear_conexión(dbname, user, password, host, port) #creo la conexión

    cur.execute(query_extracción) #ejecuto la extracción
    resultados = cur.fetchall()
    df = pd.DataFrame(resultados, columns=lista_nombre_columnas) #creo el df
    lo.cerrar_conexion(conn, cur) #cierro la conexión

    return df

def reporte_1 (df_estudio):
    """
    Genera un reporte detallado sobre las columnas del DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con información sobre tipos de variables, conteo total, 
                      número de nulos, porcentaje de nulos, valores únicos y duplicados.
    """

    df_report = pd.DataFrame()

    df_report["tipo_variables"] = pd.DataFrame(df_estudio.dtypes)
    df_report["contador_total"] = pd.DataFrame(df_estudio.count ())
    df_report["numero_nulos"]=df_estudio.isnull().sum()
    df_report["porcentaje_nulos"] = round((df_estudio.isnull().sum()/df_estudio.shape[0])*100,2)
    df_report["valores_unicos"] = pd.DataFrame(df_estudio.nunique ())
    

    diccionario_duplicados = {}
    for indice in range (0, df_estudio.shape[1]):
        k= df_estudio.columns[indice]
        v= df_estudio.iloc[:,indice].duplicated().sum()
        diccionario_duplicados.update({k:v})
    
    serie_duplicados = pd.Series(diccionario_duplicados)

    df_report["duplicados"] = pd.DataFrame(serie_duplicados)
    

    return df_report


#Creación función para el análisis de las variables categóricas:
def analisis_descriptivos_categóricas (df_estudio):
    """Analiza las variables categóricas de un DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con estadísticas descriptivas de las columnas categóricas"""
    
    df_categóricas = df_estudio.select_dtypes(include = ["object"])
    print(f'Las columnas categóricas son {df_categóricas.columns}')
    print(f'Algunos ejemplos de filas son:')
    display(df_categóricas.sample(5))
    df_estudio_categóricas = df_estudio.describe(include = "object").T
    print(f'Las características de estas columnas son:')
    return df_estudio_categóricas



#Creación función para el análisis de las variables no categóricas:
def analisis_descriptivos_numéricas(df_estudio):
    """
    Analiza las variables no categóricas de un DataFrame.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.

    Retorno:
        pd.DataFrame: DataFrame con estadísticas descriptivas de las columnas numéricas.
    """

    df_no_categoricas = df_estudio.select_dtypes(exclude = "object")      
    print(f'Las columnas no categóricas son {df_no_categoricas.columns}')
    print(f'Algunos ejemplos de filas son:')
    display(df_no_categoricas.sample(5))
    df_estudio_numéricas = np.round(df_no_categoricas.describe().T,2)
    print(f'Las características de estas columnas son:')
    return df_estudio_numéricas



def analisis_individual_columnas(df_estudio, columna_analisis):
    """
    Realiza un análisis detallado de una columna específica.

    Parámetros:
        df_estudio (pd.DataFrame): DataFrame a analizar.
        columna_analisis (str): Nombre de la columna a analizar.

    Retorno:
        None. Muestra detalles sobre la columna seleccionada.
    """

    df_columna = pd.DataFrame(df_estudio[columna_analisis].value_counts())
    print(f'La categoría {columna_analisis} tiene {df_columna.shape[0]} elementos diferentes: \n')

    print(f'Los elementos de la categoría son:')
    display(df_estudio[columna_analisis].unique())

    df_columna["Porcentaje_recuento"] = np.round(df_columna['count']/df_estudio.shape[0]*100,2)
    print(f'Los 10 {columna_analisis} que MAS aparecen son:')
    display(df_columna.head(10))

    print(f'Los 10 {columna_analisis} que MENOS aparecen son:')
    display(df_columna.tail(10))

    df_columna_contador_columnas = pd.DataFrame(df_columna['count'].value_counts())
    df_columna_contador_columnas['% repetición'] =df_columna_contador_columnas['count']/df_columna.shape[0]*100
    print(f' Las distribución de las repeticiones de los {columna_analisis} son:')
    display(df_columna_contador_columnas)
