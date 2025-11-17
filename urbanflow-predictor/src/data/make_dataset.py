import pandas as pd
import os
from pyspark.sql import SparkSession

class DataCollector:
    """Clase para recolectar y preparar datos"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
    
    def load_sample_data(self, file_path):
        """Cargar datos de muestra"""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        df = self.spark.read \
            .format("csv") \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .load(file_path)
        
        return df
    
    def validate_data_schema(self, df, expected_schema):
        """Validar que el DataFrame tiene el esquema esperado"""
        actual_schema = df.schema
        
        for field in expected_schema:
            if field.name not in [f.name for f in actual_schema]:
                raise ValueError(f"Campo faltante: {field.name}")
        
        return True
    
    def get_data_summary(self, df):
        """Obtener resumen de los datos"""
        summary = {
            "total_records": df.count(),
            "columns": len(df.columns),
            "column_names": df.columns,
            "schema": str(df.schema)
        }
        
        return summary