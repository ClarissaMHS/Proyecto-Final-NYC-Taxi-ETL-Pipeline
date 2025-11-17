from pyspark.conf import SparkConf
from pyspark.sql.types import *

class Config:
    """Configuración optimizada para desarrollo local"""
    
    # Configuración de Spark para desarrollo local
    SPARK_CONF = SparkConf().setAppName("NYC_Taxi_ETL_Local") \
        .set("spark.sql.adaptive.enabled", "true") \
        .set("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .set("spark.sql.adaptive.skew.enabled", "true") \
        .set("spark.sql.parquet.compression.codec", "snappy") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.sql.hive.convertMetastoreParquet", "false") \
        .set("spark.driver.memory", "2g") \
        .set("spark.executor.memory", "2g") \
        .set("spark.sql.shuffle.partitions", "10")  # Reducido para datos pequeños
    
    # Rutas locales para desarrollo
    LOCAL_DATA_PATH = "data/"
    RAW_DATA_PATH = f"{LOCAL_DATA_PATH}raw/"
    PROCESSED_DATA_PATH = f"{LOCAL_DATA_PATH}processed/"
    INTERIM_DATA_PATH = f"{LOCAL_DATA_PATH}interim/"
    
    # Esquema de datos optimizado
    TAXI_SCHEMA = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("pickup_longitude", DoubleType(), True),
        StructField("pickup_latitude", DoubleType(), True),
        StructField("RateCodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("dropoff_longitude", DoubleType(), True),
        StructField("dropoff_latitude", DoubleType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True)
    ])
    
    # Parámetros de limpieza optimizados
    MIN_TRIP_DISTANCE = 0.1
    MAX_TRIP_DISTANCE = 50
    MIN_FARE_AMOUNT = 2.5
    MAX_FARE_AMOUNT = 200
    NYC_BOUNDS = {
        "min_lon": -74.25559,
        "max_lon": -73.70001,
        "min_lat": 40.49612, 
        "max_lat": 40.91553
    }
    
    # Configuración para muestras pequeñas
    SAMPLE_SIZE = 1000
    SAMPLE_FILE = "yellow_tripdata_2015-01_sample.csv"