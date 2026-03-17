"""
CleanX - Column Name Cleaning Module
Step 1: Clean and standardize column names
"""

import re
import pandas as pd
from src.utils import setup_logging

logger = setup_logging(__name__)

def clean_column_names(df):
    """
    Clean and standardize column names
    
    Steps:
    1. Strip leading/trailing spaces
    2. Convert to lowercase
    3. Replace spaces & special characters with underscores
    4. Collapse multiple underscores
    5. Remove leading/trailing underscores
    6. Ensure uniqueness
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with cleaned column names
    """
    logger.info("Starting column name cleaning")
    
    original_columns = df.columns.tolist()
    cleaned_columns = []
    
    for col in original_columns:
        # Convert to string and strip
        cleaned = str(col).strip()
        
        # Convert to lowercase
        cleaned = cleaned.lower()
        
        # Replace spaces and special characters with underscore
        # Keep only alphanumeric and underscores
        cleaned = re.sub(r'[^a-z0-9]', '_', cleaned)
        
        # Collapse multiple underscores
        cleaned = re.sub(r'_+', '_', cleaned)
        
        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')
        
        # Handle empty column names
        if cleaned == '':
            cleaned = 'column'
        
        cleaned_columns.append(cleaned)
    
    # Ensure uniqueness
    final_columns = []
    seen = {}
    
    for col in cleaned_columns:
        if col not in seen:
            seen[col] = 0
            final_columns.append(col)
        else:
            seen[col] += 1
            new_col = f"{col}_{seen[col]}"
            final_columns.append(new_col)
    
    # Rename columns
    df.columns = final_columns
    
    # Log changes
    changes = []
    for old, new in zip(original_columns, final_columns):
        if old != new:
            changes.append(f"'{old}' -> '{new}'")
    
    if changes:
        logger.info(f"Renamed {len(changes)} columns")
        for change in changes[:10]:  # Log first 10 changes
            logger.debug(change)
    else:
        logger.info("No column name changes needed")
    
    return df