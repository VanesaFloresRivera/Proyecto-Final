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
import extract as ex
import transform as tr
import load as lo

print('COMIENZA LA FASE DE EXTRACCIÓN DE LOS DATOS')
#ex.escrapeo_total() #función para extraer datos mediante escrapeo de la página web

#ex.extraccion_datos_api() #función para extraer los datos de la api con el turismo emisor

#print('COMIENZA LA FASE DE TRANSFORMACIÓN')
#tr.transformacion_total_1() #funcion para transformar y limpiar los datos

print('COMIENZA LA FASE DE CARGA EN BBDD')
lo.carga_total()#funcion para cargar los datos en la BBDD
