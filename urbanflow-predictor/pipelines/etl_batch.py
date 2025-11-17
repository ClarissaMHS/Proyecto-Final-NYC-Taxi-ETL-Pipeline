from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os
from config import Config

class NYCTaxiETL:
    """Pipeline ETL optimizado para datos de muestra"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.config = Config()
        self.logger = self._setup_logging()
    
    def _create_spark_session(self):
        """Crear sesión de Spark optimizada para desarrollo local"""
        return SparkSession.builder \
            .config(conf=Config.SPARK_CONF) \
            .getOrCreate()
    
    def _setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        return logging.getLogger(__name__)
    
    def extract_data(self):
        """Extraer datos del archivo de muestra local"""
        self.logger.info(" Extrayendo datos de muestra...")
        
        sample_path = f"{self.config.RAW_DATA_PATH}{self.config.SAMPLE_FILE}"
        
        if not os.path.exists(sample_path):
            self.logger.error(f" Archivo de muestra no encontrado: {sample_path}")
            raise FileNotFoundError(f"Ejecuta primero sample_data_generator.py")
        
        # Leer datos con esquema definido
        df = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .schema(self.config.TAXI_SCHEMA) \
            .load(sample_path)
        
        self.logger.info(f" Datos extraídos: {df.count()} registros")
        return df
    
    def clean_data(self, df):
        """Limpieza y validación de datos optimizada"""
        self.logger.info(" Iniciando limpieza de datos...")
        
        initial_count = df.count()
        
        # Filtrar outliers y datos inválidos
        cleaned_df = df.filter(
            (col("trip_distance") >= self.config.MIN_TRIP_DISTANCE) &
            (col("trip_distance") <= self.config.MAX_TRIP_DISTANCE) &
            (col("fare_amount") >= self.config.MIN_FARE_AMOUNT) &
            (col("fare_amount") <= self.config.MAX_FARE_AMOUNT) &
            (col("pickup_longitude").between(
                self.config.NYC_BOUNDS["min_lon"], 
                self.config.NYC_BOUNDS["max_lon"])) &
            (col("pickup_latitude").between(
                self.config.NYC_BOUNDS["min_lat"],
                self.config.NYC_BOUNDS["max_lat"])) &
            (col("passenger_count") > 0) &
            (col("passenger_count") <= 6) &
            (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
            (col("total_amount") > 0)
        )
        
        # Limpiar valores nulos en columnas críticas
        cleaned_df = cleaned_df.dropna(
            subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", 
                   "pickup_longitude", "pickup_latitude", "fare_amount"]
        )
        
        final_count = cleaned_df.count()
        removed_count = initial_count - final_count
        
        self.logger.info(f" Datos después de limpieza: {final_count} registros")
        self.logger.info(f"  Registros removidos: {removed_count} ({removed_count/initial_count*100:.1f}%)")
        
        return cleaned_df
    
    def transform_data(self, df):
        """Transformaciones y feature engineering optimizado"""
        self.logger.info(" Aplicando transformaciones...")
        
        transformed_df = df \
            .withColumn("trip_duration_minutes", 
                       round((unix_timestamp("tpep_dropoff_datetime") - 
                              unix_timestamp("tpep_pickup_datetime")) / 60, 2)) \
            .withColumn("avg_speed_mph", 
                       when(col("trip_duration_minutes") > 0,
                            round(col("trip_distance") / (col("trip_duration_minutes") / 60), 2))
                       .otherwise(0)) \
            .withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
            .withColumn("pickup_day", dayofweek("tpep_pickup_datetime")) \
            .withColumn("pickup_month", month("tpep_pickup_datetime")) \
            .withColumn("is_weekend", 
                       when(dayofweek("tpep_pickup_datetime").isin(1, 7), 1).otherwise(0)) \
            .withColumn("time_of_day",
                       when((col("pickup_hour") >= 6) & (col("pickup_hour") < 12), "morning")
                       .when((col("pickup_hour") >= 12) & (col("pickup_hour") < 18), "afternoon")
                       .when((col("pickup_hour") >= 18) & (col("pickup_hour") < 24), "evening")
                       .otherwise("night")) \
            .withColumn("fare_per_mile",
                       when(col("trip_distance") > 0,
                            round(col("fare_amount") / col("trip_distance"), 2))
                       .otherwise(0)) \
            .withColumn("tip_percentage",
                       when(col("fare_amount") > 0, 
                            round((col("tip_amount") / col("fare_amount")) * 100, 2))
                       .otherwise(0))
        
        # Calcular distancia Haversine (simplificado para performance)
        transformed_df = self._calculate_simple_distance(transformed_df)
        
        self.logger.info(" Transformaciones aplicadas exitosamente")
        return transformed_df
    
    def _calculate_simple_distance(self, df):
        """Calcular distancia simplificada para mejor performance"""
        from pyspark.sql.functions import sqrt, pow
        
        return df.withColumn(
            "straight_line_distance_km",
            round(sqrt(
                pow((col("dropoff_latitude") - col("pickup_latitude")) * 111, 2) +
                pow((col("dropoff_longitude") - col("pickup_longitude")) * 111 * 
                    cos(radians(col("pickup_latitude"))), 2)
            ), 2)
        )
    
    def calculate_metrics(self, df):
        """Calcular métricas agregadas optimizadas"""
        self.logger.info(" Calculando métricas agregadas...")
        
        # Cachear DataFrame para reutilización
        df.cache()
        
        # Métricas por hora del día
        hourly_metrics = df.groupBy("pickup_hour", "time_of_day").agg(
            count("*").alias("trip_count"),
            avg("trip_distance").alias("avg_distance"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_duration_minutes").alias("avg_duration"),
            avg("passenger_count").alias("avg_passengers"),
            sum("total_amount").alias("total_revenue"),
            avg("tip_percentage").alias("avg_tip_percentage")
        ).orderBy("pickup_hour")
        
        # Métricas por día de la semana
        daily_metrics = df.groupBy("pickup_day", "is_weekend").agg(
            count("*").alias("trip_count"),
            avg("total_amount").alias("avg_total_amount"),
            avg("tip_amount").alias("avg_tip_amount"),
            sum("total_amount").alias("daily_revenue"),
            avg("trip_distance").alias("avg_distance")
        ).orderBy("pickup_day")
        
        # Distribución geográfica simplificada
        geo_metrics = df.groupBy(
            round(col("pickup_latitude"), 2).alias("pickup_lat_zone"),
            round(col("pickup_longitude"), 2).alias("pickup_lon_zone")
        ).agg(
            count("*").alias("pickup_count"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_distance")
        ).filter(col("pickup_count") > 1)  # Filtro más laxo para datos pequeños
        
        # Métricas de negocio
        business_metrics = df.agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_revenue_per_trip"),
            avg("trip_duration_minutes").alias("avg_trip_duration"),
            avg("passenger_count").alias("avg_passengers_per_trip"),
            sum("tip_amount").alias("total_tips"),
            avg(when(col("tip_amount") > 0, 1).otherwise(0)).alias("tipping_rate")
        )
        
        return {
            "hourly_metrics": hourly_metrics,
            "daily_metrics": daily_metrics,
            "geo_metrics": geo_metrics,
            "business_metrics": business_metrics
        }
    
    def optimize_dataframe(self, df):
        """Aplicar optimizaciones al DataFrame"""
        self.logger.info(" Aplicando optimizaciones...")
        
        # Reparticionar para mejor paralelismo en datos pequeños
        optimized_df = df.repartition(4)
        
        return optimized_df
    
    def load_data(self, df, metrics, output_format="parquet"):
        """Cargar datos procesados localmente"""
        self.logger.info(" Cargando datos procesados...")
        
        # Asegurar que el directorio existe
        os.makedirs(self.config.PROCESSED_DATA_PATH, exist_ok=True)
        
        # Datos transformados completos
        df.write \
            .mode("overwrite") \
            .format(output_format) \
            .save(f"{self.config.PROCESSED_DATA_PATH}transformed_trips")
        
        # Métricas agregadas
        metrics["hourly_metrics"].write \
            .mode("overwrite") \
            .format(output_format) \
            .save(f"{self.config.PROCESSED_DATA_PATH}hourly_metrics")
            
        metrics["daily_metrics"].write \
            .mode("overwrite") \
            .format(output_format) \
            .save(f"{self.config.PROCESSED_DATA_PATH}daily_metrics")
            
        metrics["geo_metrics"].write \
            .mode("overwrite") \
            .format(output_format) \
            .save(f"{self.config.PROCESSED_DATA_PATH}geo_metrics")
        
        # Métricas de negocio como JSON para fácil lectura
        business_pandas = metrics["business_metrics"].toPandas()
        business_pandas.to_json(f"{self.config.PROCESSED_DATA_PATH}business_metrics.json", 
                               orient="records", indent=2)
        
        self.logger.info(" Datos cargados exitosamente")
    
    def run_pipeline(self):
        """Ejecutar pipeline ETL completo optimizado"""
        try:
            self.logger.info(" Iniciando pipeline ETL optimizado...")
            
            # E: Extract
            raw_df = self.extract_data()
            
            # T: Transform
            cleaned_df = self.clean_data(raw_df)
            transformed_df = self.transform_data(cleaned_df)
            optimized_df = self.optimize_dataframe(transformed_df)
            
            # Calcular métricas
            metrics = self.calculate_metrics(optimized_df)
            
            # L: Load
            self.load_data(optimized_df, metrics)
            
            self.logger.info("🎉 Pipeline ETL completado exitosamente")
            
            # Mostrar estadísticas finales
            self._show_statistics(optimized_df, metrics)
            
            return optimized_df, metrics
            
        except Exception as e:
            self.logger.error(f" Error en el pipeline: {str(e)}")
            raise
        finally:
            # Limpiar cache
            if 'optimized_df' in locals():
                optimized_df.unpersist()
    
    def _show_statistics(self, df, metrics):
        """Mostrar estadísticas del procesamiento"""
        total_trips = df.count()
        business_stats = metrics["business_metrics"].collect()[0]
        
        print(f"""
         ===== ESTADÍSTICAS DEL PROCESAMIENTO =====
         Total de viajes procesados: {total_trips:,}
         Ingreso total: ${business_stats['total_revenue']:,.2f}
         Tarifa promedio: ${business_stats['avg_revenue_per_trip']:.2f}
         Duración promedio: {business_stats['avg_trip_duration']:.2f} minutos
         Pasajeros promedio: {business_stats['avg_passengers_per_trip']:.1f}
         Tasa de propinas: {business_stats['tipping_rate']*100:.1f}%
        
         Métricas calculadas:
           Distribución horaria ({metrics['hourly_metrics'].count()} categorías)
           Métricas diarias ({metrics['daily_metrics'].count()} categorías)  
            Análisis geográfico ({metrics['geo_metrics'].count()} zonas)
        ==========================================
        """)

if __name__ == "__main__":
    etl = NYCTaxiETL()
    etl.run_pipeline()