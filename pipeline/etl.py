import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List

class DataPipeline:
    """
    ETL pipeline for processing customer data from multiple sources
    """
    
    def __init__(self, source_config: Dict):
        self.source_config = source_config
        self.data = None
        
    def extract(self, source: str) -> pd.DataFrame:
        """Extract data from specified source"""
        # Simulate data extraction
        if source == "database":
            return self._extract_from_database()
        elif source == "api":
            return self._extract_from_api()
        else:
            raise ValueError(f"Unknown source: {source}")
    
    def _extract_from_database(self) -> pd.DataFrame:
        # Simulate database extraction
        return pd.DataFrame({
            'customer_id': range(1000),
            'name': [f'Customer {i}' for i in range(1000)],
            'email': [f'customer{i}@example.com' for i in range(1000)],
            'created_at': [datetime.now() - timedelta(days=i) for i in range(1000)]
        })
    
    def _extract_from_api(self) -> pd.DataFrame:
        # Simulate API extraction
        return pd.DataFrame({
            'transaction_id': range(5000),
            'customer_id': np.random.randint(0, 1000, 5000),
            'amount': np.random.uniform(10, 1000, 5000),
            'timestamp': [datetime.now() - timedelta(hours=i) for i in range(5000)]
        })
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean data"""
        # Remove duplicates
        df = df.drop_duplicates()
        
        # Handle missing values
        df = df.fillna(0)
        
        # Add derived columns
        if 'created_at' in df.columns:
            df['days_since_creation'] = (datetime.now() - df['created_at']).dt.days
        
        return df
    
    def load(self, df: pd.DataFrame, destination: str):
        """Load data to destination"""
        # Simulate data loading
        print(f"Loading {len(df)} records to {destination}")
        # In real implementation, would write to database/warehouse
        
    def run(self):
        """Execute full ETL pipeline"""
        # Extract from multiple sources
        customer_data = self.extract("database")
        transaction_data = self.extract("api")
        
        # Transform
        customer_data = self.transform(customer_data)
        transaction_data = self.transform(transaction_data)
        
        # Join datasets
        merged_data = pd.merge(
            transaction_data, 
            customer_data, 
            on='customer_id', 
            how='left'
        )
        
        # Load to data warehouse
        self.load(merged_data, "data_warehouse")
        
        return merged_data