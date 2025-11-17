import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

def generate_sample_data(num_records=1000, output_path="data/raw/yellow_tripdata_2015-01_sample.csv"):
    """Genera datos de muestra realistas para pruebas rápidas"""
    
    np.random.seed(42)
    
    # Fechas de enero 2015
    start_date = datetime(2015, 1, 1)
    end_date = datetime(2015, 1, 31)
    
    data = []
    
    for i in range(num_records):
        # Fechas aleatorias dentro de enero 2015
        pickup_time = start_date + timedelta(
            days=np.random.randint(0, 31),
            hours=np.random.randint(0, 24),
            minutes=np.random.randint(0, 60)
        )
        dropoff_time = pickup_time + timedelta(minutes=np.random.randint(5, 120))
        
        # Coordenadas dentro de NYC
        pickup_lon = np.random.uniform(-74.02, -73.90)
        pickup_lat = np.random.uniform(40.70, 40.80)
        
        # Distancia de viaje razonable
        trip_distance = np.random.uniform(0.5, 20.0)
        
        # Calcular tarifa basada en distancia
        fare_amount = max(2.5, trip_distance * 2.5 + np.random.uniform(0, 10))
        
        record = {
            'VendorID': np.random.choice([1, 2]),
            'tpep_pickup_datetime': pickup_time,
            'tpep_dropoff_datetime': dropoff_time,
            'passenger_count': np.random.randint(1, 7),
            'trip_distance': round(trip_distance, 2),
            'pickup_longitude': round(pickup_lon, 6),
            'pickup_latitude': round(pickup_lat, 6),
            'RateCodeID': np.random.choice([1, 2, 3, 4, 5, 6], p=[0.95, 0.02, 0.01, 0.01, 0.005, 0.005]),
            'store_and_fwd_flag': np.random.choice(['Y', 'N'], p=[0.05, 0.95]),
            'dropoff_longitude': round(pickup_lon + np.random.uniform(-0.02, 0.02), 6),
            'dropoff_latitude': round(pickup_lat + np.random.uniform(-0.02, 0.02), 6),
            'payment_type': np.random.choice([1, 2, 3, 4, 5, 6], p=[0.7, 0.25, 0.02, 0.01, 0.01, 0.01]),
            'fare_amount': round(fare_amount, 2),
            'extra': round(np.random.choice([0, 0.5, 1.0], p=[0.7, 0.15, 0.15]), 2),
            'mta_tax': 0.5,
            'tip_amount': round(fare_amount * np.random.uniform(0, 0.2), 2),
            'tolls_amount': round(np.random.choice([0, 5.0, 10.0], p=[0.8, 0.15, 0.05]), 2),
            'improvement_surcharge': 0.3,
            'total_amount': 0  # Se calculará después
        }
        
        # Calcular total_amount
        record['total_amount'] = round(
            record['fare_amount'] + record['extra'] + record['mta_tax'] + 
            record['tip_amount'] + record['tolls_amount'] + record['improvement_surcharge'], 2
        )
        
        data.append(record)
    
    # Crear DataFrame
    df = pd.DataFrame(data)
    
    # Asegurar que el directorio existe
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Guardar como CSV
    df.to_csv(output_path, index=False)
    print(f" Datos de muestra generados: {output_path}")
    print(f" Registros generados: {len(df)}")
    
    return df

if __name__ == "__main__":
    # Generar solo 1000 registros para pruebas rápidas
    generate_sample_data(1000)