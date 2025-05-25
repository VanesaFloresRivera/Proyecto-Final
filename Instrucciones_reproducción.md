
# 🌍 Proyecto TUIverso

Este proyecto analiza la alineación entre la **oferta turística de TUI** y la **demanda real de los turistas españoles**, utilizando técnicas de ETL, análisis exploratorio y visualización interactiva con Power BI.

---

## ⚙️ Requisitos previos

Antes de ejecutar el proyecto, asegúrate de tener:

- Python 3.9 o superior instalado.
- Power BI Desktop instalado.
- Git (opcional, para clonar el repositorio).

---

## 🧪 Instalación de dependencias

Instala todas las librerías necesarias ejecutando el siguiente comando en tu terminal desde la raíz del proyecto:

```bash
pip install -r requirements.txt
```

---

## 🔄 Ejecución del proceso ETL

Una vez instaladas las dependencias, dirígete a la carpeta `src/etl` y ejecuta el archivo `main.py`:

```bash
cd src/etl
python main.py
```

Este proceso realizará:

- Extracción de datos (web scraping y API).
- Limpieza y transformación.
- Carga en la base de datos o archivos preparados para Power BI.

---

## 📊 Visualización del Dashboard

Abre el archivo de Power BI llamado `TUIverso - Dashboard.pbix`, ubicado en la carpeta `dashboard`.

1. Abre el fichero con Power BI Desktop.
2. Pulsa en el botón **Actualizar** (🔄).
3. Power BI cargará los datos procesados y podrás visualizar todos los **insights**, KPIs y visualizaciones interactivas del proyecto.

---

## 🧭 Estructura del proyecto

```
├── dashboard/
│   └── TUIverso - Dashboard.pbix
├── data/
│   ├── data_raw/
│   ├── data_transform/
├── notebooks/
├── reports/
├── src/
│   ├── eda/
│   └── etl/
├── requirements.txt
└── TUIverso_README.md
```

---

## 📌 Autora

**Vanesa Flores Rivera**  
🔗 [GitHub](https://github.com/VanesaFloresRivera/Proyecto-Final)  
🔗 [LinkedIn](https://www.linkedin.com/in/vanesa-flores-rivera)
