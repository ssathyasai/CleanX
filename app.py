"""
CleanX - Industrial-Grade Automated Data Cleaning System
Main Flask Application Entry Point
"""

import os
import logging
from datetime import datetime
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd

from src.pipeline.cleanx_pipeline import CleanXPipeline
from src.config import config
from src.utils import setup_logging

# Initialize Flask app
app = Flask(__name__)
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
        df = pd.read_csv(filepath, nrows=1000)  # Sample for preview
        full_df = pd.read_csv(filepath)  # Full for stats
        
        # Store file info in state
        pipeline_state['current_file'] = {
            'path': filepath,
            'filename': filename,
            'unique_filename': unique_filename,
            'rows': len(full_df),
            'columns': len(full_df.columns),
            'column_names': full_df.columns.tolist(),
            'sample_data': df.head(10).to_dict('records')
        }
        
        return jsonify({
            'success': True,
            'filename': filename,
            'rows': len(full_df),
            'columns': len(full_df.columns),
            'column_names': full_df.columns.tolist()
        })
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/get_columns')
def get_columns():
    """Get columns for current file"""
    if 'current_file' not in pipeline_state:
        return jsonify({'error': 'No file uploaded'}), 400
    
    return jsonify({
        'columns': pipeline_state['current_file']['column_names']
    })

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
        
        # Initialize pipeline with selected steps
        pipeline = CleanXPipeline(
            remove_columns=columns_to_remove,
            steps=selected_steps
        )
        
        # Run pipeline
        cleaned_df, report = pipeline.run(filepath)
        
        # Save processed file
        output_filename = f"cleaned_{filename}"
        output_path = os.path.join('data/processed', output_filename)
        cleaned_df.to_csv(output_path, index=False)
        
        # Store output info
        pipeline_state['output_file'] = output_path
        pipeline_state['report'] = report
        
        return jsonify({
            'success': True,
            'report': report,
            'output_file': output_filename
        })
        
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/download/<filename>')
def download_file(filename):
    """Download processed file"""
    try:
        filepath = os.path.join('data/processed', filename)
        if not os.path.exists(filepath):
            return jsonify({'error': 'File not found'}), 404
        
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
    pipeline_state.clear()
    return jsonify({'success': True})

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)