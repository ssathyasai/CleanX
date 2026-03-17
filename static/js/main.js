// CleanX Main JavaScript

let currentFile = null;

// Initialize event listeners when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    initializeUploadArea();
});

function initializeUploadArea() {
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('fileInput');

    // Click to upload
    uploadArea.addEventListener('click', () => {
        fileInput.click();
    });

    // File selection
    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
            uploadFile(e.target.files[0]);
        }
    });

    // Drag and drop
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.classList.add('drag-over');
    });

    uploadArea.addEventListener('dragleave', () => {
        uploadArea.classList.remove('drag-over');
    });

    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.classList.remove('drag-over');
        
        if (e.dataTransfer.files.length > 0) {
            uploadFile(e.dataTransfer.files[0]);
        }
    });
}

async function uploadFile(file) {
    // Validate file type
    if (!file.name.endsWith('.csv')) {
        alert('Please upload a CSV file');
        return;
    }

    // Validate file size (50MB)
    if (file.size > 50 * 1024 * 1024) {
        alert('File size exceeds 50MB limit');
        return;
    }

    const formData = new FormData();
    formData.append('file', file);

    showLoading();

    try {
        const response = await fetch('/upload', {
            method: 'POST',
            body: formData
        });

        const result = await response.json();

        if (result.success) {
            currentFile = result;
            showPipelineInterface(result);
        } else {
            alert('Upload failed: ' + result.error);
        }
    } catch (error) {
        console.error('Upload error:', error);
        alert('Upload failed. Please try again.');
    } finally {
        hideLoading();
    }
}

function showPipelineInterface(fileInfo) {
    // Hide features section, show pipeline section
    document.getElementById('features-section').style.display = 'none';
    document.getElementById('pipeline-section').style.display = 'block';
    
    // Update file info
    document.getElementById('filename').textContent = fileInfo.filename;
    document.getElementById('fileStats').textContent = 
        `${fileInfo.rows.toLocaleString()} rows × ${fileInfo.columns} columns`;
    
    // Populate columns grid
    const columnsGrid = document.getElementById('columnsGrid');
    columnsGrid.innerHTML = '';
    
    fileInfo.column_names.forEach(column => {
        const columnDiv = document.createElement('div');
        columnDiv.className = 'column-item';
        columnDiv.innerHTML = `
            <input type="checkbox" id="col_${column}" value="${column}" class="column-checkbox">
            <label for="col_${column}" class="column-label">${column}</label>
        `;
        columnsGrid.appendChild(columnDiv);
    });
}

function resetToDefaults() {
    // Check all step checkboxes
    document.querySelectorAll('.step-checkbox').forEach(cb => {
        cb.checked = true;
    });
    
    // Uncheck all column checkboxes
    document.querySelectorAll('.column-checkbox').forEach(cb => {
        cb.checked = false;
    });
}

async function runPipeline() {
    // Get selected steps
    const selectedSteps = [];
    document.querySelectorAll('.step-checkbox:checked').forEach(cb => {
        selectedSteps.push(cb.value);
    });
    
    // Get columns to remove
    const columnsToRemove = [];
    document.querySelectorAll('.column-checkbox:checked').forEach(cb => {
        columnsToRemove.push(cb.value);
    });
    
    if (selectedSteps.length === 0) {
        alert('Please select at least one cleaning step');
        return;
    }
    
    showLoading();
    
    try {
        const response = await fetch('/run_pipeline', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                steps: selectedSteps,
                columns_to_remove: columnsToRemove
            })
        });
        
        const result = await response.json();
        
        if (result.success) {
            showResults(result);
        } else {
            alert('Pipeline failed: ' + result.error);
        }
    } catch (error) {
        console.error('Pipeline error:', error);
        alert('Pipeline execution failed. Please try again.');
    } finally {
        hideLoading();
    }
}

function showResults(result) {
    // Hide pipeline section, show results section
    document.getElementById('pipeline-section').style.display = 'none';
    document.getElementById('results-section').style.display = 'block';
    
    // Format and display results
    const resultsContent = document.getElementById('resultsContent');
    let reportHtml = '<strong>Pipeline Execution Report:</strong>\n\n';
    
    if (result.report) {
        reportHtml += JSON.stringify(result.report, null, 2);
    } else {
        reportHtml += 'Pipeline completed successfully.';
    }
    
    resultsContent.textContent = reportHtml;
    
    // Store output filename for download
    window.outputFilename = result.output_file;
}

function downloadFile() {
    if (window.outputFilename) {
        window.location.href = `/download/${window.outputFilename}`;
    }
}

function showLoading() {
    document.getElementById('loadingOverlay').style.display = 'flex';
}

function hideLoading() {
    document.getElementById('loadingOverlay').style.display = 'none';
}