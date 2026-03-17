"""
CleanX Utility Functions
Helper functions for logging, file handling, and data operations
"""

import logging
import logging.config
import json
from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

from src.config import LOGGING_CONFIG

def setup_logging(module_name):
    """
    Setup logging configuration for a module
    
    Args:
        module_name (str): Name of the module
    
    Returns:
        logger: Configured logger instance
    """
    logging.config.dictConfig(LOGGING_CONFIG)
    return logging.getLogger(module_name)

def save_interim_data(df, filename, step_name):
    """
    Save interim data during pipeline processing
    
    Args:
        df (pd.DataFrame): DataFrame to save
        filename (str): Original filename
        step_name (str): Current pipeline step
    
    Returns:
        str: Path to saved file
    """
    from src.config import INTERIM_DATA_DIR
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    interim_filename = f"{timestamp}_{step_name}_{filename}"
    interim_path = INTERIM_DATA_DIR / interim_filename
    
    df.to_csv(interim_path, index=False)
    return str(interim_path)

def load_csv(filepath, **kwargs):
    """
    Safely load a CSV file with error handling
    
    Args:
        filepath (str): Path to CSV file
        **kwargs: Additional arguments for pd.read_csv
    
    Returns:
        pd.DataFrame: Loaded dataframe
    """
    try:
        df = pd.read_csv(filepath, **kwargs)
        return df
    except Exception as e:
        raise Exception(f"Error loading CSV file {filepath}: {str(e)}")

def save_processed_data(df, filename):
    """
    Save final processed data
    
    Args:
        df (pd.DataFrame): Cleaned dataframe
        filename (str): Output filename
    
    Returns:
        str: Path to saved file
    """
    from src.config import PROCESSED_DATA_DIR
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_filename = f"cleaned_{timestamp}_{filename}"
    output_path = PROCESSED_DATA_DIR / output_filename
    
    df.to_csv(output_path, index=False)
    return str(output_path)

def generate_report(stats, steps_executed):
    """
    Generate a comprehensive pipeline report
    
    Args:
        stats (dict): Pipeline statistics
        steps_executed (list): Steps that were executed
    
    Returns:
        dict: Formatted report
    """
    report = {
        'timestamp': datetime.now().isoformat(),
        'steps_executed': steps_executed,
        'statistics': stats,
        'summary': {
            'total_rows': stats.get('initial_rows', 0),
            'final_rows': stats.get('final_rows', 0),
            'rows_removed': stats.get('initial_rows', 0) - stats.get('final_rows', 0),
            'total_columns': stats.get('initial_columns', 0),
            'final_columns': stats.get('final_columns', 0),
            'columns_removed': stats.get('initial_columns', 0) - stats.get('final_columns', 0)
        }
    }
    
    return report

def detect_column_type(series):
    """
    Intelligently detect column data type
    
    Args:
        series (pd.Series): Column to analyze
    
    Returns:
        str: Detected type ('numerical', 'categorical', 'datetime', 'text')
    """
    # Drop NA for type detection
    sample = series.dropna()
    
    if len(sample) == 0:
        return 'unknown'
    
    # Check if datetime
    try:
        pd.to_datetime(sample, errors='raise')
        return 'datetime'
    except:
        pass
    
    # Check if numerical
    try:
        pd.to_numeric(sample, errors='raise')
        return 'numerical'
    except:
        pass
    
    # Check if categorical (low cardinality)
    if sample.nunique() / len(sample) < 0.05:  # Less than 5% unique values
        return 'categorical'
    
    # Default to text
    return 'text'

def validate_dataframe(df):
    """
    Validate dataframe for processing
    
    Args:
        df (pd.DataFrame): Dataframe to validate
    
    Returns:
        tuple: (is_valid, error_message)
    """
    if df is None:
        return False, "DataFrame is None"
    
    if df.empty:
        return False, "DataFrame is empty"
    
    if len(df.columns) == 0:
        return False, "DataFrame has no columns"
    
    return True, "Valid"

def get_data_quality_report(df):
    """
    Generate data quality report for initial dataset
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        dict: Data quality metrics
    """
    report = {
        'rows': len(df),
        'columns': len(df.columns),
        'missing_values': df.isnull().sum().sum(),
        'missing_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
        'duplicates': df.duplicated().sum(),
        'duplicate_percentage': (df.duplicated().sum() / len(df)) * 100,
        'column_types': df.dtypes.astype(str).to_dict(),
        'memory_usage': df.memory_usage(deep=True).sum() / (1024 * 1024)  # MB
    }
    
    return report