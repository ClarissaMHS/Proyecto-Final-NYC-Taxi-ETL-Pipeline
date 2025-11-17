#!/usr/bin/env python3
"""
Script principal de ejecución del pipeline ETL
Ubicación: raíz del proyecto urbanflow-predictor/
"""

import time
import sys
import os

# Agregar el directorio pipelines al path
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipelines'))

from sample_data_generator import generate_sample_data
from etl_batch import NYCTaxiETL

def main():
    print(" INICIANDO PIPELINE ETL COMPLETO - NYC TAXI ANALYTICS")
    print("=" * 60)
    
    try:
        # Paso 1: Generar datos de muestra
        print(" Paso 1: Generando datos de muestra...")
        start_time = time.time()
        generate_sample_data(1000)  # Solo 1000 registros para velocidad
        sample_time = time.time() - start_time
        print(f" Datos generados en {sample_time:.2f} segundos")
        
        # Paso 2: Ejecutar pipeline ETL
        print("\n Paso 2: Ejecutando pipeline ETL...")
        start_time = time.time()
        etl = NYCTaxiETL()
        df, metrics = etl.run_pipeline()
        etl_time = time.time() - start_time
        
        print(f" Pipeline completado en {etl_time:.2f} segundos")
        print(f" Total tiempo ejecución: {sample_time + etl_time:.2f} segundos")
        
        # Mostrar información final
        print("\n PIPELINE COMPLETADO EXITOSAMENTE!")
        print(" Datos procesados en: data/processed/")
        print(" Métricas guardadas en formato Parquet y JSON")
        print("\n Próximos pasos:")
        print("   - Ejecutar: jupyter notebooks/01_eda_sample_data.ipynb")
        print("   - Revisar métricas en: data/processed/business_metrics.json")
        
    except Exception as e:
        print(f" Error en la ejecución: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()