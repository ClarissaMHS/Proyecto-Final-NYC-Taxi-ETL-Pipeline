from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from config import Config

class NYCTaxiStreamingETL:
    """Pipeline ETL en streaming para datos de NYC Taxi (simulación)"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("NYC_Taxi_Streaming") \
            .config(conf=Config.SPARK_CONF) \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.config = Config()
        self.logger = self._setup_logging()
    
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    def simulate_streaming_data(self):
        """Simular datos en streaming para desarrollo local"""
        self.logger.info(" Simulando datos de streaming...")
        
        # Crear datos de prueba para streaming
        sample_data = [
            (1, '2023-01-01 08:30:00', '2023-01-01 08:45:00', 2, 2.5, -73.9857, 40.7484, 1, 'N', -73.9757, 40.7584, 1, 12.50, 0.5, 0.5, 2.50, 0.0, 0.3, 16.30),
            (2, '2023-01-01 09:15:00', '2023-01-01 09:35:00', 1, 3.2, -73.9800, 40.7500, 1, 'N', -73.9700, 40.7600, 2, 15.75, 1.0, 0.5, 0.0, 0.0, 0.3, 17.55),
            (1, '2023-01-01 10:00:00', '2023-01-01 10:20:00', 3, 4.1, -73.9900, 40.7400, 1, 'N', -73.9800, 40.7500, 1, 18.25, 0.5, 0.5, 3.65, 0.0, 0.3, 23.20)
        ]
        
        schema = Config.TAXI_SCHEMA
        
        streaming_df = self.spark.createDataFrame(sample_data, schema)
        
        return streaming_df
    
    def create_streaming_schema(self):
        """Esquema para datos de streaming"""
        return StructType([
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
    
    def process_streaming_data(self):
        """Procesar datos en tiempo real (simulación)"""
        self.logger.info(" Procesando datos de streaming...")
        
        # Simular DataFrame de streaming
        streaming_df = self.simulate_streaming_data()
        
        # Aplicar transformaciones en tiempo real
        processed_stream = streaming_df \
            .filter(col("trip_distance") > 0) \
            .filter(col("fare_amount") > 0) \
            .withColumn("processing_time", current_timestamp()) \
            .withColumn("trip_duration_minutes", 
                       round((unix_timestamp("tpep_dropoff_datetime") - 
                              unix_timestamp("tpep_pickup_datetime")) / 60, 2)) \
            .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
            .withColumn("avg_speed", 
                       when(col("trip_duration_minutes") > 0,
                            round(col("trip_distance") / (col("trip_duration_minutes") / 60), 2))
                       .otherwise(0))
        
        # Mostrar resultados
        self.logger.info(" Resultados del procesamiento streaming:")
        processed_stream.show()
        
        # Métricas en tiempo real
        streaming_metrics = processed_stream.agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration_minutes").alias("avg_duration"),
            sum("total_amount").alias("total_revenue")
        )
        
        self.logger.info(" Métricas en tiempo real:")
        streaming_metrics.show()
        
        return processed_stream
    
    def run_streaming_demo(self):
        """Ejecutar demostración de streaming"""
        try:
            self.logger.info(" Iniciando demostración de streaming...")
            
            # Procesar datos de streaming
            processed_data = self.process_streaming_data()
            
            self.logger.info(" Demostración de streaming completada")
            
            # Guardar resultados de demostración
            processed_data.write \
                .mode("overwrite") \
                .parquet(f"{self.config.PROCESSED_DATA_PATH}streaming_demo")
                
            self.logger.info(" Resultados de streaming guardados")
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f" Error en streaming demo: {str(e)}")
            raise

if __name__ == "__main__":
    streaming_etl = NYCTaxiStreamingETL()
    streaming_etl.run_streaming_demo()