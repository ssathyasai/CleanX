// =========================================
// CLEANX - SUPER FANTASTIC JAVASCRIPT
// Crystal Clear Column Names Edition
// =========================================

let currentFile = null;

// Initialize everything when DOM is loaded
document.addEventListener('DOMContentLoaded', function() {
    console.log('✨ CleanX initialized');
    
    // Hide loading overlay
    const loadingOverlay = document.getElementById('loadingOverlay');
    if (loadingOverlay) loadingOverlay.style.display = 'none';
    
    // Initialize components
    initializeUploadArea();
    initializeSteps();
    initializeTooltips();
});

// =========================================
// UPLOAD AREA INITIALIZATION
// =========================================

function initializeUploadArea() {
    const uploadArea = document.getElementById('uploadArea');
    const fileInput = document.getElementById('fileInput');
    
    if (!uploadArea || !fileInput) return;
    
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
    
    // Drag and drop effects
    uploadArea.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadArea.style.transform = 'scale(1.02)';
        uploadArea.style.borderColor = '#3b82f6';
        uploadArea.style.background = 'rgba(59, 130, 246, 0.1)';
    });
    
    uploadArea.addEventListener('dragleave', () => {
        uploadArea.style.transform = 'scale(1)';
        uploadArea.style.borderColor = 'rgba(99, 102, 241, 0.3)';
        uploadArea.style.background = 'rgba(31, 41, 55, 0.4)';
    });
    
    uploadArea.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadArea.style.transform = 'scale(1)';
        uploadArea.style.borderColor = 'rgba(99, 102, 241, 0.3)';
        uploadArea.style.background = 'rgba(31, 41, 55, 0.4)';
        
        if (e.dataTransfer.files.length > 0) {
            uploadFile(e.dataTransfer.files[0]);
        }
    });
}

// =========================================
// STEPS INITIALIZATION
// =========================================

function initializeSteps() {
    const stepsGrid = document.getElementById('stepsGrid');
    if (!stepsGrid) return;
    
    const steps = [
        { value: 'clean_names', label: 'Clean Column Names', icon: 'fa-tag' },
        { value: 'remove_columns', label: 'Remove Columns', icon: 'fa-trash' },
        { value: 'handle_duplicates', label: 'Handle Duplicates', icon: 'fa-copy' },
        { value: 'clean_spaces', label: 'Clean Extra Spaces', icon: 'fa-space-shuttle' },
        { value: 'handle_missing', label: 'Handle Missing Values', icon: 'fa-question-circle' },
        { value: 'type_conversion', label: 'Data Type Conversion', icon: 'fa-code' },
        { value: 'standardize_dates', label: 'Standardize Dates', icon: 'fa-calendar' },
        { value: 'detect_outliers', label: 'Detect Outliers', icon: 'fa-chart-pie' }
    ];
    
    stepsGrid.innerHTML = steps.map((step, index) => {
        // Remove Columns (index 1) checked by default like screenshot
        const checked = index === 1 ? 'checked' : '';
        return `
            <label class="step-item-modern">
                <input type="checkbox" class="step-checkbox" value="${step.value}" ${checked}>
                <i class="fas ${step.icon}" style="color: #3b82f6;"></i>
                <span class="step-label-modern">${step.label}</span>
            </label>
        `;
    }).join('');
}

// =========================================
// TOOLTIPS INITIALIZATION
// =========================================

function initializeTooltips() {
    // Add any tooltip initializations here
}

// =========================================
// FILE UPLOAD FUNCTION
// =========================================

async function uploadFile(file) {
    // Validate file type
    if (!file.name.endsWith('.csv')) {
        showNotification('Please upload a CSV file', 'error');
        return;
    }
    
    // Validate file size
    if (file.size > 50 * 1024 * 1024) {
        showNotification('File size exceeds 50MB limit', 'error');
        return;
    }
    
    const formData = new FormData();
    formData.append('file', file);
    
    showLoading('Uploading file...');
    
    try {
        const response = await fetch('/upload', {
            method: 'POST',
            body: formData
        });
        
        const result = await response.json();
        
        if (result.success) {
            currentFile = result;
            showPipelineInterface(result);
            showNotification('File uploaded successfully!', 'success');
        } else {
            showNotification('Upload failed: ' + result.error, 'error');
        }
    } catch (error) {
        console.error('Upload error:', error);
        showNotification('Upload failed. Please try again.', 'error');
    } finally {
        hideLoading();
    }
}

// =========================================
// SHOW PIPELINE INTERFACE
// =========================================

function showPipelineInterface(fileInfo) {
    // Hide features section, show pipeline section
    const featuresSection = document.getElementById('featuresSection');
    const pipelineSection = document.getElementById('pipelineSection');
    
    if (featuresSection) featuresSection.style.display = 'none';
    if (pipelineSection) pipelineSection.style.display = 'block';
    
    // Update file info
    const filenameEl = document.getElementById('filename');
    const fileStatsEl = document.getElementById('fileStats');
    
    if (filenameEl) filenameEl.textContent = fileInfo.filename;
    if (fileStatsEl) {
        fileStatsEl.textContent = `${fileInfo.rows.toLocaleString()} rows × ${fileInfo.columns} columns`;
    }
    
    // Populate columns grid - CRYSTAL CLEAR COLUMNS
    const columnsGrid = document.getElementById('columnsGrid');
    if (!columnsGrid) return;
    
    columnsGrid.innerHTML = '';
    
    fileInfo.column_names.forEach(column => {
        const columnDiv = document.createElement('div');
        columnDiv.className = 'column-item';
        columnDiv.innerHTML = `
            <input type="checkbox" id="col_${column.replace(/[^a-zA-Z0-9]/g, '_')}" value="${column}" class="column-checkbox">
            <label for="col_${column.replace(/[^a-zA-Z0-9]/g, '_')}" class="column-label">${column}</label>
        `;
        columnsGrid.appendChild(columnDiv);
    });
    
    // Add animation class
    pipelineSection.classList.add('pulse-animation');
    setTimeout(() => {
        pipelineSection.classList.remove('pulse-animation');
    }, 1000);
}

// =========================================
// RESET TO DEFAULTS
// =========================================

function resetToDefaults() {
    // Set Remove Columns (index 1) checked, others unchecked
    document.querySelectorAll('.step-checkbox').forEach((cb, index) => {
        cb.checked = (index === 1);
    });
    
    // Uncheck all column checkboxes
    document.querySelectorAll('.column-checkbox').forEach(cb => {
        cb.checked = false;
    });
    
    showNotification('Settings reset to defaults', 'info');
}

// =========================================
// RUN PIPELINE
// =========================================

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
        showNotification('Please select at least one cleaning step', 'warning');
        return;
    }
    
    showLoading('Running cleaning pipeline...');
    
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
            showNotification('Pipeline completed successfully!', 'success');
        } else {
            showNotification('Pipeline failed: ' + result.error, 'error');
        }
    } catch (error) {
        console.error('Pipeline error:', error);
        showNotification('Pipeline execution failed. Please try again.', 'error');
    } finally {
        hideLoading();
    }
}

// =========================================
// SHOW RESULTS
// =========================================

function showResults(result) {
    const pipelineSection = document.getElementById('pipelineSection');
    const resultsSection = document.getElementById('resultsSection');
    const resultsContent = document.getElementById('resultsContent');
    
    if (pipelineSection) pipelineSection.style.display = 'none';
    if (resultsSection) resultsSection.style.display = 'block';
    
    if (resultsContent) {
        resultsContent.textContent = JSON.stringify(result.report, null, 2);
    }
    
    window.outputFilename = result.output_file;
}

// =========================================
// DOWNLOAD FILE
// =========================================

function downloadFile() {
    if (window.outputFilename) {
        window.location.href = `/download/${window.outputFilename}`;
        showNotification('Download started!', 'success');
    }
}

// =========================================
// LOADING OVERLAY FUNCTIONS
// =========================================

function showLoading(message = 'Processing Your Data...') {
    const loadingOverlay = document.getElementById('loadingOverlay');
    const loadingSubtitle = loadingOverlay?.querySelector('.loading-subtitle');
    
    if (loadingOverlay) {
        if (loadingSubtitle) loadingSubtitle.textContent = message;
        loadingOverlay.style.display = 'flex';
    }
}

function hideLoading() {
    const loadingOverlay = document.getElementById('loadingOverlay');
    if (loadingOverlay) {
        loadingOverlay.style.display = 'none';
    }
}

// =========================================
// NOTIFICATION SYSTEM
// =========================================

function showNotification(message, type = 'info') {
    const notification = document.createElement('div');
    notification.className = `notification notification-${type}`;
    notification.innerHTML = `
        <i class="fas ${getNotificationIcon(type)}"></i>
        <span>${message}</span>
    `;
    
    // Style the notification
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: rgba(31, 41, 55, 0.95);
        backdrop-filter: blur(10px);
        border: 1px solid ${getNotificationColor(type)};
        border-radius: 12px;
        padding: 1rem 1.5rem;
        color: white;
        font-size: 0.95rem;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 0.75rem;
        z-index: 10000;
        animation: slideIn 0.3s ease;
        box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
    `;
    
    document.body.appendChild(notification);
    
    // Add animation keyframes if not exists
    if (!document.querySelector('#notification-keyframes')) {
        const style = document.createElement('style');
        style.id = 'notification-keyframes';
        style.textContent = `
            @keyframes slideIn {
                from { transform: translateX(100%); opacity: 0; }
                to { transform: translateX(0); opacity: 1; }
            }
        `;
        document.head.appendChild(style);
    }
    
    // Remove after 5 seconds
    setTimeout(() => {
        notification.style.animation = 'slideOut 0.3s ease';
        notification.style.transform = 'translateX(100%)';
        setTimeout(() => notification.remove(), 300);
    }, 5000);
}

function getNotificationIcon(type) {
    switch(type) {
        case 'success': return 'fa-check-circle';
        case 'error': return 'fa-exclamation-circle';
        case 'warning': return 'fa-exclamation-triangle';
        default: return 'fa-info-circle';
    }
}

function getNotificationColor(type) {
    switch(type) {
        case 'success': return '#10b981';
        case 'error': return '#ef4444';
        case 'warning': return '#f59e0b';
        default: return '#3b82f6';
    }
}

// Add CSS for notification animation
const style = document.createElement('style');
style.textContent = `
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(100%); opacity: 0; }
    }
`;
document.head.appendChild(style);