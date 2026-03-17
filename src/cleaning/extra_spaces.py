"""
CleanX - Extra Spaces Cleaning Module
Step 4: Clean extra spaces in string columns
"""

import pandas as pd
import re
from src.utils import setup_logging

logger = setup_logging(__name__)

def clean_extra_spaces(df):
    """
    Clean extra spaces in string columns
    
    Operations:
    - Strip leading spaces
    - Strip trailing spaces
    - Replace multiple internal spaces with single space
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with cleaned string columns
    """
    logger.info("Starting extra spaces cleaning")
    
    # Identify string columns
    string_columns = df.select_dtypes(include=['object', 'string']).columns
    logger.info(f"Found {len(string_columns)} string columns to clean")
    
    if len(string_columns) == 0:
        logger.info("No string columns found")
        return df
    
    cleaned_count = 0
    
    for col in string_columns:
        # Skip if all values are null
        if df[col].isnull().all():
            continue
        
        # Store original values for comparison
        original_values = df[col].copy()
        
        # Apply cleaning to non-null values
        mask = df[col].notna()
        
        # Strip leading/trailing spaces and collapse internal spaces
        df.loc[mask, col] = df.loc[mask, col].apply(
            lambda x: re.sub(r'\s+', ' ', str(x).strip()) if isinstance(x, str) else x
        )
        
        # Count changes
        changes = (original_values != df[col]).sum()
        cleaned_count += changes
        
        if changes > 0:
            logger.debug(f"Cleaned {changes} values in column '{col}'")
    
    logger.info(f"Total values cleaned: {cleaned_count}")
    
    return df