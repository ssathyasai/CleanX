"""
CleanX - Outlier Detection and Handling Module
Step 8: Detect and handle outliers in numerical columns
"""

import pandas as pd
import numpy as np
from src.utils import setup_logging
from src.config import OUTLIER_THRESHOLDS

logger = setup_logging(__name__)

def detect_outliers(df):
    """
    Detect and handle outliers in numerical columns
    
    Methods:
    - IQR-based detection
    - Z-score based detection
    
    Handling strategies based on outlier percentage:
    - <5%: Remove outliers
    - 5-10%: Cap/floor outliers
    - >10%: Keep or transform (log transformation)
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with outliers handled
    """
    logger.info("Starting outlier detection")
    
    # Select numerical columns
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    logger.info(f"Analyzing {len(numerical_cols)} numerical columns")
    
    outlier_stats = {}
    
    for col in numerical_cols:
        # Skip columns with all nulls
        if df[col].isnull().all():
            continue
        
        # Detect outliers using IQR
        outliers_iqr = detect_outliers_iqr(df[col])
        
        # Detect outliers using Z-score
        outliers_zscore = detect_outliers_zscore(df[col])
        
        # Combine methods (consider outlier if detected by either method)
        outliers = outliers_iqr | outliers_zscore
        
        outlier_count = outliers.sum()
        outlier_percentage = (outlier_count / len(df)) * 100
        
        outlier_stats[col] = {
            'count': outlier_count,
            'percentage': outlier_percentage,
            'method': 'IQR + Z-score'
        }
        
        logger.info(f"Column '{col}': {outlier_count} outliers ({outlier_percentage:.2f}%)")
        
        # Handle outliers based on percentage
        df = handle_outliers(df, col, outliers, outlier_percentage)
    
    # Log summary
    total_outliers = sum(stats['count'] for stats in outlier_stats.values())
    logger.info(f"Total outliers detected: {total_outliers}")
    
    return df

def detect_outliers_iqr(series):
    """
    Detect outliers using IQR method
    
    Args:
        series (pd.Series): Numerical series
    
    Returns:
        pd.Series: Boolean mask of outliers
    """
    Q1 = series.quantile(0.25)
    Q3 = series.quantile(0.75)
    IQR = Q3 - Q1
    
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    return (series < lower_bound) | (series > upper_bound)

def detect_outliers_zscore(series, threshold=3):
    """
    Detect outliers using Z-score method
    
    Args:
        series (pd.Series): Numerical series
        threshold (float): Z-score threshold
    
    Returns:
        pd.Series: Boolean mask of outliers
    """
    z_scores = np.abs((series - series.mean()) / series.std())
    return z_scores > threshold

def handle_outliers(df, col, outlier_mask, outlier_percentage):
    """
    Handle outliers based on their percentage
    
    Args:
        df (pd.DataFrame): Dataframe
        col (str): Column name
        outlier_mask (pd.Series): Boolean mask of outliers
        outlier_percentage (float): Percentage of outliers
    
    Returns:
        pd.DataFrame: Dataframe with outliers handled
    """
    if outlier_percentage == 0:
        return df
    
    thresholds = OUTLIER_THRESHOLDS
    
    if outlier_percentage < thresholds['low'] * 100:
        # Low outliers - remove them
        logger.info(f"Low outliers in '{col}' ({outlier_percentage:.2f}%) - removing")
        df = df[~outlier_mask]
        
    elif outlier_percentage <= thresholds['medium'] * 100:
        # Medium outliers - cap/floor them
        logger.info(f"Medium outliers in '{col}' ({outlier_percentage:.2f}%) - capping/floring")
        
        # Calculate bounds
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        
        # Cap/floor outliers
        df.loc[df[col] < lower_bound, col] = lower_bound
        df.loc[df[col] > upper_bound, col] = upper_bound
        
    else:
        # High outliers - consider transformation
        logger.info(f"High outliers in '{col}' ({outlier_percentage:.2f}%) - considering transformation")
        
        # Check if log transformation helps
        if (df[col] > 0).all():  # All positive values
            try:
                log_transformed = np.log1p(df[col])
                log_outliers = detect_outliers_iqr(log_transformed).sum()
                log_outlier_pct = (log_outliers / len(df)) * 100
                
                if log_outlier_pct < outlier_percentage:
                    logger.info(f"Log transformation reduces outliers to {log_outlier_pct:.2f}% - applying")
                    df[col] = log_transformed
                else:
                    logger.info("Keeping original values")
            except:
                logger.info("Log transformation not applicable - keeping original")
    
    return df