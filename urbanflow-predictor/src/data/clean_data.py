from pyspark.sql.functions import *
from pyspark.sql.types import *

class DataCleaner:
    """Clase para limpieza de datos de NYC Taxi"""
    
    @staticmethod
    def remove_outliers(df, config):
        """Eliminar outliers basado en configuración"""
        return df.filter(
            (col("trip_distance") >= config.MIN_TRIP_DISTANCE) &
            (col("trip_distance") <= config.MAX_TRIP_DISTANCE) &
            (col("fare_amount") >= config.MIN_FARE_AMOUNT) &
            (col("fare_amount") <= config.MAX_FARE_AMOUNT) &
            (col("passenger_count") > 0) &
            (col("passenger_count") <= 6)
        )
    
    @staticmethod
    def validate_coordinates(df, bounds):
        """Validar coordenadas dentro de NYC"""
        return df.filter(
            (col("pickup_longitude").between(bounds["min_lon"], bounds["max_lon"])) &
            (col("pickup_latitude").between(bounds["min_lat"], bounds["max_lat"])) &
            (col("dropoff_longitude").between(bounds["min_lon"], bounds["max_lon"])) &
            (col("dropoff_latitude").between(bounds["min_lat"], bounds["max_lat"]))
        )
    
    @staticmethod
    def handle_missing_values(df):
        """Manejar valores faltantes"""
        # Eliminar filas con valores críticos faltantes
        critical_columns = [
            "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "pickup_longitude", "pickup_latitude", "fare_amount"
        ]
        
        df_cleaned = df.dropna(subset=critical_columns)
        
        # Imputar valores para columnas menos críticas
        df_filled = df_cleaned.fillna({
            "passenger_count": 1,
            "trip_distance": 0,
            "tip_amount": 0,
            "tolls_amount": 0,
            "extra": 0
        })
        
        return df_filled
    
    @staticmethod
    def validate_trip_times(df):
        """Validar que los tiempos de viaje sean lógicos"""
        return df.filter(
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
            (unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime") < 3600 * 4)  # Máximo 4 horas
        )