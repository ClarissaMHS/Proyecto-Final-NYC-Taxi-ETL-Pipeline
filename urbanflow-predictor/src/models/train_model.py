from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import *

class DemandPredictor:
    """Clase para entrenar modelo de predicción de demanda"""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.model = None
        self.pipeline = None
    
    def prepare_features(self, df):
        """Preparar features para el modelo"""
        feature_columns = [
            'pickup_hour', 'pickup_day', 'is_weekend', 'is_rush_hour',
            'pickup_lat_zone', 'pickup_lon_zone', 'manhattan_zone'
        ]
        
        # Agrupar por características temporales y geográficas
        demand_df = df.groupBy(feature_columns).agg(
            count("*").alias("trip_count")
        )
        
        return demand_df
    
    def train_model(self, df):
        """Entrenar modelo de predicción de demanda"""
        # Preparar datos
        demand_df = self.prepare_features(df)
        
        # Features para el modelo
        feature_cols = [col for col in demand_df.columns if col != 'trip_count']
        
        # Ensamblar features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features"
        )
        
        # Modelo de Random Forest
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="trip_count",
            numTrees=10,  # Reducido para datos pequeños
            maxDepth=5
        )
        
        # Pipeline
        self.pipeline = Pipeline(stages=[assembler, rf])
        
        # Entrenar modelo
        self.model = self.pipeline.fit(demand_df)
        
        return self.model
    
    def predict_demand(self, feature_data):
        """Predecir demanda"""
        if self.model is None:
            raise ValueError("Modelo no entrenado. Ejecuta train_model primero.")
        
        return self.model.transform(feature_data)