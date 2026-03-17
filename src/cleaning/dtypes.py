"""
CleanX - Data Type Conversion Module
Step 6: Infer and convert data types
"""

import pandas as pd
import numpy as np
from src.utils import setup_logging, detect_column_type

logger = setup_logging(__name__)

def convert_data_types(df):
    """
    Infer and convert data types for all columns
    
    Strategy:
    1. Try numeric conversion first
    2. Try datetime conversion
    3. Convert low-cardinality to categorical
    4. Validate no significant data loss
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with optimized data types
    """
    logger.info("Starting data type conversion")
    
    conversion_stats = {
        'converted_to_numeric': [],
        'converted_to_datetime': [],
        'converted_to_categorical': []
    }
    
    for col in df.columns:
        original_dtype = df[col].dtype
        
        # Skip if already numeric or datetime
        if pd.api.types.is_numeric_dtype(df[col]) or pd.api.types.is_datetime64_any_dtype(df[col]):
            continue
        
        # Try numeric conversion
        df, converted = try_numeric_conversion(df, col)
        if converted:
            conversion_stats['converted_to_numeric'].append(col)
            logger.debug(f"Converted '{col}' to numeric")
            continue
        
        # Try datetime conversion
        df, converted = try_datetime_conversion(df, col)
        if converted:
            conversion_stats['converted_to_datetime'].append(col)
            logger.debug(f"Converted '{col}' to datetime")
            continue
        
        # Try categorical conversion for low cardinality
        df, converted = try_categorical_conversion(df, col)
        if converted:
            conversion_stats['converted_to_categorical'].append(col)
            logger.debug(f"Converted '{col}' to categorical")
    
    # Log summary
    logger.info(f"Conversion summary:")
    logger.info(f"  - Numeric: {len(conversion_stats['converted_to_numeric'])} columns")
    logger.info(f"  - Datetime: {len(conversion_stats['converted_to_datetime'])} columns")
    logger.info(f"  - Categorical: {len(conversion_stats['converted_to_categorical'])} columns")
    
    return df

def try_numeric_conversion(df, col):
    """Attempt to convert column to numeric"""
    try:
        # Try conversion
        converted = pd.to_numeric(df[col], errors='coerce')
        
        # Check for significant data loss (>5% becomes null)
        original_non_null = df[col].notna().sum()
        converted_non_null = converted.notna().sum()
        data_loss = (original_non_null - converted_non_null) / original_non_null
        
        if data_loss <= 0.05:  # Less than 5% data loss
            df[col] = converted
            return df, True
        else:
            logger.debug(f"Numeric conversion for '{col}' would lose {data_loss:.2%} data - skipping")
            return df, False
            
    except Exception as e:
        logger.debug(f"Numeric conversion failed for '{col}': {str(e)}")
        return df, False

def try_datetime_conversion(df, col):
    """Attempt to convert column to datetime"""
    try:
        # Try conversion
        converted = pd.to_datetime(df[col], errors='coerce')
        
        # Check for significant data loss
        original_non_null = df[col].notna().sum()
        converted_non_null = converted.notna().sum()
        data_loss = (original_non_null - converted_non_null) / original_non_null
        
        if data_loss <= 0.05:  # Less than 5% data loss
            df[col] = converted
            return df, True
        else:
            logger.debug(f"Datetime conversion for '{col}' would lose {data_loss:.2%} data - skipping")
            return df, False
            
    except Exception as e:
        logger.debug(f"Datetime conversion failed for '{col}': {str(e)}")
        return df, False

def try_categorical_conversion(df, col):
    """Attempt to convert low-cardinality columns to categorical"""
    try:
        # Calculate unique ratio
        unique_ratio = df[col].nunique() / len(df)
        
        # Convert if less than 5% unique values
        if unique_ratio < 0.05:
            df[col] = df[col].astype('category')
            return df, True
        
        return df, False
        
    except Exception as e:
        logger.debug(f"Categorical conversion failed for '{col}': {str(e)}")
        return df, False