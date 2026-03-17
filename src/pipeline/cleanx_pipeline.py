"""
CleanX - Main Pipeline Orchestration Module
Coordinates all cleaning steps in the correct order
"""

import pandas as pd
from datetime import datetime
from src.utils import setup_logging, load_csv, save_interim_data, generate_report, get_data_quality_report
from src.cleaning.column_names import clean_column_names
from src.cleaning.duplicates import handle_duplicates
from src.cleaning.missing_values import handle_missing_values
from src.cleaning.extra_spaces import clean_extra_spaces
from src.cleaning.dtypes import convert_data_types
from src.cleaning.date_standardize import standardize_dates
from src.cleaning.outliers import detect_outliers

logger = setup_logging(__name__)

class CleanXPipeline:
    """
    Main pipeline class that orchestrates the entire data cleaning process
    """
    
    def __init__(self, remove_columns=None, steps=None):
        """
        Initialize the pipeline with configuration
        
        Args:
            remove_columns (list): List of columns to remove
            steps (list): List of steps to execute (None = all steps)
        """
        self.remove_columns = remove_columns or []
        self.steps = steps or [
            'clean_names',
            'remove_columns',
            'handle_duplicates',
            'clean_spaces',
            'handle_missing',
            'type_conversion',
            'standardize_dates',
            'detect_outliers'
        ]
        
        self.step_functions = {
            'clean_names': clean_column_names,
            'handle_duplicates': handle_duplicates,
            'clean_spaces': clean_extra_spaces,
            'handle_missing': handle_missing_values,
            'type_conversion': convert_data_types,
            'standardize_dates': standardize_dates,
            'detect_outliers': detect_outliers
        }
        
        self.stats = {
            'initial_rows': 0,
            'initial_columns': 0,
            'final_rows': 0,
            'final_columns': 0,
            'steps_executed': [],
            'step_details': {}
        }
        
    def run(self, filepath):
        """
        Execute the complete cleaning pipeline
        
        Args:
            filepath (str): Path to input CSV file
        
        Returns:
            tuple: (cleaned_df, report)
        """
        logger.info(f"Starting CleanX pipeline for {filepath}")
        start_time = datetime.now()
        
        # Load data
        df = load_csv(filepath)
        self.stats['initial_rows'] = len(df)
        self.stats['initial_columns'] = len(df.columns)
        
        logger.info(f"Initial dataset: {len(df)} rows, {len(df.columns)} columns")
        
        # Generate initial quality report
        initial_quality = get_data_quality_report(df)
        logger.info(f"Initial quality: {initial_quality['missing_values']} missing, "
                   f"{initial_quality['duplicates']} duplicates")
        
        # Step 1: Clean column names
        if 'clean_names' in self.steps:
            logger.info("Step 1: Cleaning column names")
            df = clean_column_names(df)
            self.stats['steps_executed'].append('clean_names')
            self.save_interim(df, '01_clean_names')
        
        # Step 2: Remove specified columns
        if 'remove_columns' in self.steps and self.remove_columns:
            logger.info(f"Step 2: Removing columns: {self.remove_columns}")
            existing_cols = [col for col in self.remove_columns if col in df.columns]
            if existing_cols:
                df = df.drop(columns=existing_cols)
                logger.info(f"Removed {len(existing_cols)} columns")
            self.stats['steps_executed'].append('remove_columns')
            self.save_interim(df, '02_remove_columns')
        
        # Step 3: Handle duplicates
        if 'handle_duplicates' in self.steps:
            logger.info("Step 3: Handling duplicates")
            before_rows = len(df)
            df = handle_duplicates(df)
            after_rows = len(df)
            self.stats['step_details']['duplicates_removed'] = before_rows - after_rows
            self.stats['steps_executed'].append('handle_duplicates')
            self.save_interim(df, '03_handle_duplicates')
        
        # Step 4: Clean extra spaces
        if 'clean_spaces' in self.steps:
            logger.info("Step 4: Cleaning extra spaces")
            df = clean_extra_spaces(df)
            self.stats['steps_executed'].append('clean_spaces')
            self.save_interim(df, '04_clean_spaces')
        
        # Step 5: Handle missing values
        if 'handle_missing' in self.steps:
            logger.info("Step 5: Handling missing values")
            before_missing = df.isnull().sum().sum()
            df = handle_missing_values(df)
            after_missing = df.isnull().sum().sum()
            self.stats['step_details']['missing_handled'] = before_missing - after_missing
            self.stats['steps_executed'].append('handle_missing')
            self.save_interim(df, '05_handle_missing')
        
        # Step 6: Convert data types
        if 'type_conversion' in self.steps:
            logger.info("Step 6: Converting data types")
            df = convert_data_types(df)
            self.stats['steps_executed'].append('type_conversion')
            self.save_interim(df, '06_type_conversion')
        
        # Step 7: Standardize dates
        if 'standardize_dates' in self.steps:
            logger.info("Step 7: Standardizing dates")
            df = standardize_dates(df)
            self.stats['steps_executed'].append('standardize_dates')
            self.save_interim(df, '07_standardize_dates')
        
        # Step 8: Detect outliers
        if 'detect_outliers' in self.steps:
            logger.info("Step 8: Detecting outliers")
            df = detect_outliers(df)
            self.stats['steps_executed'].append('detect_outliers')
            self.save_interim(df, '08_detect_outliers')
        
        # Update final stats
        self.stats['final_rows'] = len(df)
        self.stats['final_columns'] = len(df.columns)
        
        # Calculate processing time
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()
        self.stats['processing_time_seconds'] = processing_time
        
        # Generate final quality report
        final_quality = get_data_quality_report(df)
        
        # Create comprehensive report
        report = generate_report(
            {**self.stats, 'initial_quality': initial_quality, 'final_quality': final_quality},
            self.stats['steps_executed']
        )
        
        logger.info(f"Pipeline completed in {processing_time:.2f} seconds")
        logger.info(f"Final dataset: {len(df)} rows, {len(df.columns)} columns")
        
        return df, report
    
    def save_interim(self, df, step_name):
        """Save interim data if configured"""
        # This is optional - can be enabled via config
        pass