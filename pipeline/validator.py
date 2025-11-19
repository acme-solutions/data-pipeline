import pandas as pd
import numpy as np
from great_expectations.core import ExpectationSuite

class DataValidator:
    def __init__(self):
        self.validation_suite = ExpectationSuite(expectation_suite_name="data_validation")
    
    def validate_customer_data(self, df: pd.DataFrame) -> dict:
        results = {}
        
        # Check for nulls in critical columns
        null_check = df[['customer_id', 'email']].isnull().sum()
        results['null_check'] = null_check.to_dict()
        
        # Validate email format
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        valid_emails = df['email'].str.match(email_pattern)
        results['invalid_emails'] = (~valid_emails).sum()
        
        # Check for duplicates
        duplicates = df.duplicated(subset=['customer_id']).sum()
        results['duplicates'] = duplicates
        
        return results
    
    def validate_transaction_data(self, df: pd.DataFrame) -> dict:
        results = {}
        
        # Check for negative amounts
        negative_amounts = (df['amount'] < 0).sum()
        results['negative_amounts'] = negative_amounts
        
        # Check timestamp validity
        invalid_timestamps = df['timestamp'].isnull().sum()
        results['invalid_timestamps'] = invalid_timestamps
        
        return results
