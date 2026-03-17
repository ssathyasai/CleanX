"""
CleanX - Industrial-Grade Automated Data Cleaning System
Main Flask Application Entry Point
"""

import os
import logging
import json
import webbrowser
import threading
import numpy as np
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd

from src.pipeline.cleanx_pipeline import CleanXPipeline
from src.utils import setup_logging

# Custom JSON encoder to handle NumPy types
class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for NumPy data types"""
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')
        elif hasattr(obj, 'dtype'):  # Handle other numpy types
            return obj.item()
        return super(NumpyEncoder, self).default(obj)

# Initialize Flask app
app = Flask(__name__)
app.json_encoder = NumpyEncoder  # Use custom encoder for all JSON responses
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB limit
app.config['SECRET_KEY'] = 'cleanx-secret-key-2026'

# Ensure directories exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs('data/raw', exist_ok=True)
os.makedirs('data/interim', exist_ok=True)
os.makedirs('data/processed', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# Setup logging
logger = setup_logging('app')

# Store pipeline state (in production, use session/database)
pipeline_state = {}

def allowed_file(filename):
    """Check if file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() == 'csv'

def convert_numpy_types(obj):
    """Recursively convert numpy types to Python native types"""
    if isinstance(obj, dict):
        return {key: convert_numpy_types(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_numpy_types(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_numpy_types(item) for item in obj)
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif pd.isna(obj):  # Handle NaN, None
        return None
    else:
        return obj

def open_browser():
    """Open browser after a short delay"""
    import time
    time.sleep(1.5)  # Wait for server to start
    webbrowser.open_new('http://127.0.0.1:5000/')

@app.route('/')
def index():
    """Render main page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle CSV file upload"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file uploaded'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Only CSV files are allowed'}), 400
        
        # Secure filename and save
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"{timestamp}_{filename}"
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], unique_filename)
        file.save(filepath)
        
        # Read CSV to get info
        logger.info(f"Reading uploaded file: {filename}")
        df = pd.read_csv(filepath, nrows=1000)  # Sample for preview
        full_df = pd.read_csv(filepath)  # Full for stats
        
        # Convert numpy types in sample data
        sample_data = convert_numpy_types(df.head(10).to_dict('records'))
        column_names = convert_numpy_types(full_df.columns.tolist())
        
        # Store file info in state
        pipeline_state['current_file'] = {
            'path': filepath,
            'filename': filename,
            'unique_filename': unique_filename,
            'rows': int(len(full_df)),  # Convert to Python int
            'columns': int(len(full_df.columns)),  # Convert to Python int
            'column_names': column_names,
            'sample_data': sample_data
        }
        
        logger.info(f"File uploaded successfully: {filename} - {len(full_df)} rows, {len(full_df.columns)} columns")
        
        return jsonify({
            'success': True,
            'filename': filename,
            'rows': int(len(full_df)),
            'columns': int(len(full_df.columns)),
            'column_names': column_names
        })
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_columns')
def get_columns():
    """Get columns for current file"""
    try:
        if 'current_file' not in pipeline_state:
            return jsonify({'error': 'No file uploaded'}), 400
        
        return jsonify({
            'columns': pipeline_state['current_file']['column_names']
        })
    except Exception as e:
        logger.error(f"Get columns error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/run_pipeline', methods=['POST'])
def run_pipeline():
    """Execute the cleaning pipeline"""
    try:
        data = request.json
        selected_steps = data.get('steps', [])
        columns_to_remove = data.get('columns_to_remove', [])
        
        if 'current_file' not in pipeline_state:
            return jsonify({'error': 'No file uploaded'}), 400
        
        filepath = pipeline_state['current_file']['path']
        filename = pipeline_state['current_file']['filename']
        
        logger.info(f"Starting pipeline for {filename} with steps: {selected_steps}")
        logger.info(f"Columns to remove: {columns_to_remove}")
        
        # Initialize pipeline with selected steps
        pipeline = CleanXPipeline(
            remove_columns=columns_to_remove,
            steps=selected_steps
        )
        
        # Run pipeline
        cleaned_df, report = pipeline.run(filepath)
        
        # Convert numpy types in report
        report = convert_numpy_types(report)
        
        # Save processed file
        output_filename = f"cleaned_{filename}"
        output_path = os.path.join('data/processed', output_filename)
        cleaned_df.to_csv(output_path, index=False)
        
        # Store output info
        pipeline_state['output_file'] = output_path
        pipeline_state['report'] = report
        
        logger.info(f"Pipeline completed successfully. Output saved to {output_filename}")
        
        return jsonify({
            'success': True,
            'report': report,
            'output_file': output_filename
        })
        
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download processed file"""
    try:
        filepath = os.path.join('data/processed', filename)
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
        logger.info(f"Downloading file: {filename}")
        
        return send_file(
            filepath,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        logger.error(f"Download error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/reset')
def reset_pipeline():
    """Reset pipeline state"""
    try:
        pipeline_state.clear()
        logger.info("Pipeline state reset")
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Reset error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '1.0.0'
    })

# Error handlers
@app.errorhandler(404)
def not_found_error(error):
    return jsonify({'error': 'Not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal server error: {str(error)}")
    return jsonify({'error': 'Internal server error'}), 500

@app.errorhandler(413)
def too_large_error(error):
    return jsonify({'error': 'File too large. Maximum size is 50MB'}), 413

# For local development
if __name__ == '__main__':
    port = 5000
    debug = True
    
    # Print beautiful startup message
    print("\n" + "=" * 60)
    print(" 🧹  CleanX - Industrial-Grade Data Cleaning System".ljust(59) + " ")
    print("=" * 60)
    print(" 📂 Upload folder:    uploads/".ljust(59) + " ")
    print(" 📊 Data folders:     data/raw, data/interim, data/processed".ljust(59) + " ")
    print(" 📝 Logs folder:      logs/".ljust(59) + " ")
    print(" 🌐 Server:           http://127.0.0.1:5000".ljust(59) + " ")
    print(" 🔧 Debug mode:       ON".ljust(59) + " ")
    print("=" * 60)
    print(" 🚀 Starting server...".ljust(59) + " ")
    print(" ⏱️  Opening browser in 2 seconds...".ljust(59) + " ")
    print("=" * 60)
    print(" Press Ctrl+C to stop the server".ljust(59) + " ")
    print("=" * 60 + "\n")
    
    # Open browser automatically
    threading.Timer(2.0, open_browser).start()
    
    # Run the app
    app.run(
        host='127.0.0.1',
        port=port,
        debug=debug,
        use_reloader=False  # Prevents browser from opening twice on reload
    )