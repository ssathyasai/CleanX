"""
CleanX - Date Standardization Module
Step 7: Standardize date columns to consistent format
"""

import pandas as pd
from src.utils import setup_logging
from src.config import DATE_FORMATS

logger = setup_logging(__name__)

def standardize_dates(df):
    """
    Identify and standardize date columns
    
    Steps:
    1. Identify potential date columns
    2. Parse using pandas to_datetime
    3. Coerce invalid values to NaT
    4. Validate conversion success
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with standardized dates
    """
    logger.info("Starting date standardization")
    
    date_columns = []
    conversion_stats = {}
    
    for col in df.columns:
        # Skip if already datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            date_columns.append(col)
            logger.debug(f"Column '{col}' already datetime")
            continue
        
        # Check if column might be dates (object or string type)
        if df[col].dtype in ['object', 'string']:
            # Sample some values to check
            sample = df[col].dropna().head(100)
            
            if len(sample) == 0:
                continue
            
            # Try to detect if column contains dates
            try:
                # Check if values look like dates
                test_parse = pd.to_datetime(sample, errors='coerce')
                success_rate = test_parse.notna().sum() / len(sample)
                
                if success_rate > 0.8:  # 80% successful parsing
                    date_columns.append(col)
                    
            except:
                continue
    
    logger.info(f"Found {len(date_columns)} date columns")
    
    for col in date_columns:
        df, stats = standardize_date_column(df, col)
        conversion_stats[col] = stats
    
    # Log summary
    for col, stats in conversion_stats.items():
        logger.info(f"Column '{col}': {stats['success_rate']:.1%} success rate")
    
    return df

def standardize_date_column(df, col):
    """
    Standardize a single date column
    
    Args:
        df (pd.DataFrame): Dataframe
        col (str): Column name
    
    Returns:
        tuple: (updated_df, conversion_stats)
    """
    original_non_null = df[col].notna().sum()
    
    # Try conversion with automatic format detection
    converted = pd.to_datetime(df[col], errors='coerce')
    
    # Calculate success rate
    converted_non_null = converted.notna().sum()
    success_rate = converted_non_null / original_non_null if original_non_null > 0 else 0
    
    logger.debug(f"Converting '{col}': {success_rate:.1%} success rate")
    
    if success_rate >= 0.95:
        # Excellent conversion - replace column
        df[col] = converted
        logger.debug(f"High quality conversion for '{col}' - replacing")
        
    elif success_rate >= 0.80:
        # Good conversion - replace but keep original as backup
        backup_col = f"{col}_original"
        df[backup_col] = df[col]
        df[col] = converted
        logger.debug(f"Good conversion for '{col}' - backed up original to '{backup_col}'")
        
    elif success_rate >= 0.50:
        # Moderate conversion - create new column
        new_col = f"{col}_standardized"
        df[new_col] = converted
        logger.debug(f"Moderate conversion for '{col}' - created '{new_col}'")
        
    else:
        # Poor conversion - keep as is
        logger.debug(f"Poor conversion for '{col}' - keeping original")
    
    stats = {
        'original_non_null': original_non_null,
        'converted_non_null': converted_non_null,
        'success_rate': success_rate
    }
    
    return df, stats