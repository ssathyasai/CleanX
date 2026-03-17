"""
CleanX - Missing Values Handling Module
Step 5: Handle missing values based on column type and missing percentage
"""

import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from src.utils import setup_logging, detect_column_type
from src.config import MISSING_VALUE_THRESHOLDS

logger = setup_logging(__name__)

# MAKE SURE THIS FUNCTION NAME IS EXACTLY: handle_missing_values (not handle_missing)
def handle_missing_values(df):
    """
    Handle missing values in the dataframe
    
    Args:
        df (pd.DataFrame): Input dataframe
    
    Returns:
        pd.DataFrame: Dataframe with missing values handled
    """
    logger.info("Starting missing values handling")
    
    initial_shape = df.shape
    total_missing = df.isnull().sum().sum()
    missing_percentage = (total_missing / (df.shape[0] * df.shape[1])) * 100
    
    logger.info(f"Total missing values: {total_missing} ({missing_percentage:.2f}%)")
    
    columns_dropped = []
    
    for col in df.columns:
        missing_count = df[col].isnull().sum()
        if missing_count == 0:
            continue
            
        missing_pct = missing_count / len(df)
        
        # Detect column type
        col_type = detect_column_type(df[col])
        logger.info(f"Column '{col}' - Type: {col_type}, Missing: {missing_pct:.2%}")
        
        if col_type == 'numerical':
            df = handle_numerical_missing(df, col, missing_pct, columns_dropped)
        elif col_type in ['categorical', 'datetime']:
            df = handle_categorical_missing(df, col, missing_pct, columns_dropped)
        else:
            # Text columns - treat as categorical
            df = handle_categorical_missing(df, col, missing_pct, columns_dropped)
    
    final_shape = df.shape
    logger.info(f"Missing values handled. Shape: {initial_shape} -> {final_shape}")
    logger.info(f"Dropped columns: {columns_dropped}")
    
    return df

def handle_numerical_missing(df, col, missing_pct, columns_dropped):
    """Handle missing values in numerical columns"""
    thresholds = MISSING_VALUE_THRESHOLDS['numerical']
    
    if missing_pct <= thresholds['low']:
        # Low missing - median imputation
        logger.info(f"Low missing in '{col}' - using median imputation")
        df[col].fillna(df[col].median(), inplace=True)
        
    elif missing_pct <= thresholds['medium']:
        # Medium missing - KNN imputation
        logger.info(f"Medium missing in '{col}' - using KNN imputation")
        df = knn_impute(df, [col])
        
    elif missing_pct <= thresholds['high']:
        # High missing - advanced imputation
        logger.info(f"High missing in '{col}' - using advanced imputation")
        df = advanced_numerical_impute(df, [col])
        
    else:
        # Very high missing - drop column
        logger.info(f"Very high missing in '{col}' ({missing_pct:.2%}) - dropping column")
        df.drop(columns=[col], inplace=True)
        columns_dropped.append(col)
    
    return df

def handle_categorical_missing(df, col, missing_pct, columns_dropped):
    """Handle missing values in categorical columns"""
    thresholds = MISSING_VALUE_THRESHOLDS['categorical']
    
    if missing_pct <= thresholds['low']:
        # Low missing - mode imputation
        logger.info(f"Low missing in '{col}' - using mode imputation")
        mode_value = df[col].mode()
        if len(mode_value) > 0:
            df[col].fillna(mode_value[0], inplace=True)
        else:
            df[col].fillna('Unknown', inplace=True)
        
    elif missing_pct <= thresholds['medium']:
        # Medium missing - mode + Unknown
        logger.info(f"Medium missing in '{col}' - using mode + Unknown")
        mode_value = df[col].mode()
        if len(mode_value) > 0:
            df[col].fillna(mode_value[0], inplace=True)
        else:
            df[col].fillna('Unknown', inplace=True)
        
        # Add "Unknown" if not already a category
        if 'Unknown' not in df[col].values:
            # For categorical dtype, we need to handle differently
            if hasattr(df[col], 'cat'):
                df[col] = df[col].cat.add_categories(['Unknown'])
        
    elif missing_pct <= thresholds['high']:
        # High missing - add "Missing" category
        logger.info(f"High missing in '{col}' - adding 'Missing' category")
        df[col].fillna('Missing', inplace=True)
        
    else:
        # Very high missing - drop column
        logger.info(f"Very high missing in '{col}' ({missing_pct:.2%}) - dropping column")
        df.drop(columns=[col], inplace=True)
        columns_dropped.append(col)
    
    return df

def knn_impute(df, columns):
    """Perform KNN imputation on specified columns"""
    try:
        # Select only numerical columns for KNN
        numerical_cols = df[columns].select_dtypes(include=[np.number]).columns
        
        if len(numerical_cols) == 0:
            logger.warning("No numerical columns for KNN imputation - using median")
            for col in columns:
                df[col].fillna(df[col].median(), inplace=True)
            return df
        
        # Prepare data for KNN
        imputer = KNNImputer(n_neighbors=5)
        
        # Fit and transform
        imputed_array = imputer.fit_transform(df[numerical_cols])
        
        # Update dataframe
        df[numerical_cols] = imputed_array
        
    except Exception as e:
        logger.error(f"KNN imputation failed: {str(e)} - using median")
        for col in columns:
            if col in df.columns:
                df[col].fillna(df[col].median(), inplace=True)
    
    return df

def advanced_numerical_impute(df, columns):
    """Advanced imputation for high missing percentage"""
    try:
        from sklearn.experimental import enable_iterative_imputer
        from sklearn.impute import IterativeImputer
        
        # Select numerical columns
        numerical_cols = df[columns].select_dtypes(include=[np.number]).columns
        
        if len(numerical_cols) == 0:
            logger.warning("No numerical columns for advanced imputation - using median")
            for col in columns:
                df[col].fillna(df[col].median(), inplace=True)
            return df
        
        # Use iterative imputer
        imputer = IterativeImputer(max_iter=10, random_state=42)
        imputed_array = imputer.fit_transform(df[numerical_cols])
        
        # Update dataframe
        df[numerical_cols] = imputed_array
        
    except Exception as e:
        logger.error(f"Advanced imputation failed: {str(e)} - using KNN")
        df = knn_impute(df, columns)
    
    return df