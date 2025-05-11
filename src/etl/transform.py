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


#funcion para obtener pais, continente, latitud y longitud
def obtener_pais_continente_lat_long (municipio,API_KEY):
    url= f'https://api.opencagedata.com/geocode/v1/json?q= {municipio}&key={API_KEY}&language=es'
    response =rq.get(url)
    if response.status_code == 200:
        data = response.json()
        if data['results']:
            continente = data['results'][0]['components']['continent']
            pais = data['results'][0]['components']['country']
            lat =data['results'][0]['geometry']['lat']
            lon = data['results'][0]['geometry']['lng']
            return continente,pais, lat, lon 
        else:
            return 'Información desconocida', 'Información desconocida','Información desconocida','Información desconocida'
    else:
        print("Error de conexión")