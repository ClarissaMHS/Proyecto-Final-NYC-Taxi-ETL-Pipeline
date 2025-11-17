import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql.functions import *

class DataVisualizer:
    """Clase para visualización de datos"""
    
    @staticmethod
    def plot_temporal_trends(spark_df, output_path=None):
        """Graficar tendencias temporales"""
        # Convertir a Pandas para visualización
        pd_df = spark_df.toPandas()
        
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))
        
        # Viajes por hora
        hourly = pd_df.groupby('pickup_hour').size()
        axes[0,0].plot(hourly.index, hourly.values, marker='o')
        axes[0,0].set_title('Viajes por Hora')
        axes[0,0].set_xlabel('Hora')
        axes[0,0].set_ylabel('Número de Viajes')
        
        # Tarifa promedio por hora
        fare_hourly = pd_df.groupby('pickup_hour')['fare_amount'].mean()
        axes[0,1].plot(fare_hourly.index, fare_hourly.values, marker='s', color='green')
        axes[0,1].set_title('Tarifa Promedio por Hora')
        axes[0,1].set_xlabel('Hora')
        axes[0,1].set_ylabel('Tarifa Promedio (USD)')
        
        # Duración por día de la semana
        duration_daily = pd_df.groupby('pickup_day')['trip_duration_minutes'].mean()
        axes[1,0].bar(duration_daily.index, duration_daily.values, color='orange', alpha=0.7)
        axes[1,0].set_title('Duración Promedio por Día')
        axes[1,0].set_xlabel('Día de la Semana')
        axes[1,0].set_ylabel('Duración (minutos)')
        
        # Distribución de distancias
        axes[1,1].hist(pd_df['trip_distance'], bins=30, alpha=0.7, color='purple')
        axes[1,1].set_title('Distribución de Distancias')
        axes[1,1].set_xlabel('Distancia (millas)')
        axes[1,1].set_ylabel('Frecuencia')
        
        plt.tight_layout()
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        plt.show()
    
    @staticmethod
    def plot_geographic_heatmap(spark_df, output_path=None):
        """Graficar mapa de calor geográfico"""
        pd_df = spark_df.toPandas()
        
        plt.figure(figsize=(12, 8))
        
        scatter = plt.scatter(pd_df['pickup_longitude'], 
                             pd_df['pickup_latitude'], 
                             c=pd_df['fare_amount'], 
                             cmap='viridis', 
                             alpha=0.6,
                             s=20)
        
        plt.colorbar(scatter, label='Tarifa (USD)')
        plt.title('Mapa de Calor - Tarifas por Ubicación')
        plt.xlabel('Longitud')
        plt.ylabel('Latitud')
        plt.grid(True, alpha=0.3)
        
        if output_path:
            plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        plt.show()