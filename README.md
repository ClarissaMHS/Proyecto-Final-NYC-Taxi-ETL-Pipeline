## Proyecto-Final-NYC-Taxi-ETL-Pipeline
Un sistema de procesamiento de datos escalable construido con PySpark que implementa un pipeline ETL para analizar millones de viajes de taxis de Nueva York. Incluye limpieza de datos, feature engineering, métricas agregadas y capacidades de streaming para procesamiento en tiempo real.

## Descripción del Proyecto
Este proyecto implementa un pipeline completo de ETL (Extract, Transform, Load) para analizar datos históricos de viajes de taxis amarillos de Nueva York. El sistema procesa grandes volúmenes de datos utilizando PySpark y genera insights valiosos para la optimización de operaciones de transporte urbano.

## Instalación y Configuración
Prerrequisitos
Python 3.8+
Java 8+
Apache Spark
Git

# Instalación de Dependencias
pip install pyspark pandas numpy matplotlib seaborn jupyter

## Descarga de Datos
IMPORTANTE: Para ejecutar el proyecto, primero debes descargar los datos desde Kaggle:

Accede al dataset: NYC Yellow Taxi Trip Data en Kaggle
Descarga los archivos CSV:
-Ve a la página del dataset
-Haz clic en "Download"
-Necesitarás una cuenta de Kaggle (gratuita)
Coloca los archivos en la estructura del proyecto:
proyecto/
├── data/
│   ├── raw/                   ← COLOCA LOS ARCHIVOS CSV AQUÍ
│   │   ├── yellow_tripdata_2015-01.csv
│   │   ├── yellow_tripdata_2015-02.csv
│   │   └── ...
│   ├── processed/
│   └── business/
├── notebooks/
├── scripts/
└── README.md

## Estructura del Proyecto
proyecto-nyc-taxi/
├── data/
│   ├── raw/                      # Datos sin procesar (colocar archivos CSV aquí)
│   ├── processed/               # Datos transformados
│   └── business/                # Métricas de negocio
├── notebooks/
│   └── 01_eda_sample_data.ipynb # Análisis exploratorio
├── scripts/
│   ├── run_etl.py              # Pipeline ETL principal
│   ├── data_processor.py       # Procesamiento de datos
│   └── config.py               # Configuración
├── requirements.txt
└── README.md

## Ejecución del Proyecto
# 1. Preparar los Datos
Colocar los archivos CSV descargados de Kaggle en:
data/raw/

# 2. Ejecutar el Pipeline ETL
python scripts/run_etl.py

# 3. Análisis Exploratorio
jupyter notebook notebooks/01_eda_sample_data.ipynb

## Características Principales
# Pipeline ETL Completo
Extracción: Carga de datos desde múltiples archivos CSV
Transformación:
Limpieza y validación de datos
Feature engineering
Agregación de métricas
Carga: Almacenamiento en formatos optimizados

# Análisis Implementado
Patrones temporales de demanda
Métricas de rendimiento por zona
Análisis de ingresos y propinas
Optimización de rutas
Segmentación de clientes


# Capacidades Técnicas
Procesamiento distribuido con PySpark
Escalabilidad para millones de registros
Análisis en tiempo real (streaming)
Visualizaciones interactivas
Exportación de métricas de negocio

# Métricas de Negocio
El sistema genera las siguientes métricas clave:
Ingresos totales por periodo
Viajes más rentables
Horarios pico de demanda
Zonas de mayor actividad
Eficiencia operacional

# Tecnologías Utilizadas
PySpark: Procesamiento distribuido
Pandas: Análisis de datos
Matplotlib/Seaborn: Visualizaciones
Jupyter: Análisis interactivo
Python 3.8+: Lenguaje principal

# Enlaces 
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data

## Nota: Recuerda descargar los archivos CSV desde Kaggle y colocarlos en la carpeta data/raw/ antes de ejecutar el proyecto.

