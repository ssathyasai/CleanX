"""
CleanX - Duplicate Handling Module
Step 3: Detect and handle duplicate rows
"""

import pandas as pd
import numpy as np
from src.utils import setup_logging

logger = setup_logging(__name__)

def handle_duplicates(df):
    """
    Handle duplicate rows based on percentage thresholds
    
    Strategy:
    - <15% duplicates: Exact deduplication (keep first)
    - 15-25% duplicates: Exact deduplication with warning
    - >25% duplicates: Conditional deduplication (subset-based)
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with duplicates handled
    """
    logger.info("Starting duplicate handling")
    
    initial_rows = len(df)
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = (duplicate_count / initial_rows) * 100
    
    logger.info(f"Found {duplicate_count} duplicate rows ({duplicate_percentage:.2f}%)")
    
    if duplicate_count == 0:
        logger.info("No duplicates found")
        return df
    
    # Decision based on duplicate percentage
    if duplicate_percentage < 15:
        # Low duplicates - exact deduplication
        logger.info("Low duplicate percentage (<15%) - performing exact deduplication")
        df_cleaned = df.drop_duplicates(keep='first')
        
    elif duplicate_percentage <= 25:
        # Medium duplicates - exact deduplication with inspection
        logger.info("Medium duplicate percentage (15-25%) - performing exact deduplication with inspection")
        df_cleaned = df.drop_duplicates(keep='first')
        logger.warning(f"Removed {duplicate_count} duplicate rows - please verify")
        
    else:
        # High duplicates - conditional deduplication
        logger.info("High duplicate percentage (>25%) - performing conditional deduplication")
        
        # Try to find a subset of columns that uniquely identifies rows
        # Start with all columns and progressively remove columns
        all_cols = df.columns.tolist()
        best_subset = all_cols.copy()
        
        for i in range(len(all_cols) - 1, 0, -1):
            subset = all_cols[:i]
            if df.duplicated(subset=subset).sum() == 0:
                best_subset = subset
                break
        
        if best_subset != all_cols:
            logger.info(f"Using subset of {len(best_subset)} columns for deduplication")
            df_cleaned = df.drop_duplicates(subset=best_subset, keep='first')
        else:
            # If no subset works, use time-based if available
            date_cols = df.select_dtypes(include=['datetime64']).columns.tolist()
            
            if date_cols:
                logger.info(f"Using time-based deduplication with {date_cols[0]}")
                df_sorted = df.sort_values(date_cols[0])
                df_cleaned = df_sorted.drop_duplicates(keep='last')
            else:
                # Fallback to exact deduplication with warning
                logger.warning("Could not find optimal subset - using exact deduplication")
                df_cleaned = df.drop_duplicates(keep='first')
    
    final_rows = len(df_cleaned)
    rows_removed = initial_rows - final_rows
    
    logger.info(f"Removed {rows_removed} duplicate rows")
    logger.info(f"Final row count: {final_rows}")
    
    return df_cleaned