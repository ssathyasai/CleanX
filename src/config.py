"""
CleanX Configuration Module
Contains all configuration parameters and settings
"""

# WITH THIS (more robust for Render):
import os
from pathlib import Path

# Use environment variable for data directory on Render
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = Path(os.environ.get('RENDER_DATA_DIR', BASE_DIR / 'data'))
RAW_DATA_DIR = DATA_DIR / 'raw'
INTERIM_DATA_DIR = DATA_DIR / 'interim'
PROCESSED_DATA_DIR = DATA_DIR / 'processed'
LOGS_DIR = Path(os.environ.get('RENDER_LOG_DIR', BASE_DIR / 'logs'))

# Create directories if they don't exist
for dir_path in [RAW_DATA_DIR, INTERIM_DATA_DIR, PROCESSED_DATA_DIR, LOGS_DIR]:
    dir_path.mkdir(parents=True, exist_ok=True)

# Pipeline configuration
PIPELINE_STEPS = [
    'clean_names',
    'remove_columns',
    'handle_duplicates',
    'clean_spaces',
    'handle_missing',
    'type_conversion',
    'standardize_dates',
    'detect_outliers'
]

# Missing value thresholds
MISSING_VALUE_THRESHOLDS = {
    'numerical': {
        'low': 0.15,      # <=15% -> median
        'medium': 0.30,    # 15-30% -> KNN
        'high': 0.50       # 30-50% -> advanced
        # >50% -> drop column
    },
    'categorical': {
        'low': 0.15,       # <=15% -> mode
        'medium': 0.30,    # 15-30% -> mode + Unknown
        'high': 0.50       # 30-50% -> add "Missing" category
        # >50% -> drop column
    }
}

# Outlier thresholds
OUTLIER_THRESHOLDS = {
    'low': 0.05,      # <5% -> remove
    'medium': 0.10    # 5-10% -> cap/floor
    # >10% -> keep or transform
}

# Duplicate thresholds
DUPLICATE_THRESHOLDS = {
    'low': 0.15,      # <15% -> exact deduplication
    'medium': 0.25    # 15-25% -> exact with inspection
    # >25% -> conditional deduplication
}

# Date formats for parsing
DATE_FORMATS = [
    '%Y-%m-%d',
    '%d/%m/%Y',
    '%m/%d/%Y',
    '%Y%m%d',
    '%d-%m-%Y',
    '%m-%d-%Y',
    '%Y/%m/%d',
    '%d.%m.%Y',
    '%m.%d.%Y'
]

# Logging configuration
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'file': {
            'class': 'logging.FileHandler',
            'filename': LOGS_DIR / 'cleanx.log',
            'formatter': 'standard'
        },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'standard'
        }
    },
    'root': {
        'handlers': ['file', 'console'],
        'level': 'INFO'
    }
}