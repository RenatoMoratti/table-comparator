// Modern Table Comparator JavaScript

// Enhanced page load animations and interactions
document.addEventListener('DOMContentLoaded', function() {
    console.log('🚀 Table Comparator UI loaded with modern enhancements');
    
    // Initialize page animations
    initPageAnimations();
    
    // Initialize enhanced tooltips
    initEnhancedTooltips();
    
    // Initialize modern form interactions
    initFormEnhancements();
});

// Page Animation System
function initPageAnimations() {
    // Staggered animation for cards
    const cards = document.querySelectorAll('.card');
    cards.forEach((card, index) => {
        card.style.opacity = '0';
        card.style.transform = 'translateY(30px)';
        
        setTimeout(() => {
            card.style.transition = 'all 0.6s cubic-bezier(0.4, 0, 0.2, 1)';
            card.style.opacity = '1';
            card.style.transform = 'translateY(0)';
        }, index * 150);
    });
    
    // Animate hero section
    const hero = document.querySelector('.display-5');
    if (hero) {
        hero.style.opacity = '0';
        hero.style.transform = 'translateY(20px)';
        
        setTimeout(() => {
            hero.style.transition = 'all 0.8s cubic-bezier(0.4, 0, 0.2, 1)';
            hero.style.opacity = '1';
            hero.style.transform = 'translateY(0)';
        }, 200);
    }
}

// Enhanced Tooltips
function initEnhancedTooltips() {
    // Create modern tooltips for icons and buttons
    const tooltipElements = document.querySelectorAll('[data-bs-toggle="tooltip"], .fas, .far');
    
    tooltipElements.forEach(element => {
        // Add custom tooltip behavior
        element.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.1)';
            this.style.transition = 'transform 0.2s ease';
        });
        
        element.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
        });
    });
}

// Form Enhancement System
function initFormEnhancements() {
    // Enhanced focus states for form controls
    const formControls = document.querySelectorAll('.form-control, .form-select');
    
    formControls.forEach(control => {
        control.addEventListener('focus', function() {
            this.parentElement.style.transform = 'translateY(-2px)';
            this.parentElement.style.transition = 'transform 0.2s ease';
        });
        
        control.addEventListener('blur', function() {
            this.parentElement.style.transform = 'translateY(0)';
        });
        
        // Add floating label effect
        control.addEventListener('input', function() {
            if (this.value) {
                this.classList.add('has-value');
            } else {
                this.classList.remove('has-value');
            }
        });
    });
    
    // Enhanced button interactions
    const buttons = document.querySelectorAll('.btn');
    buttons.forEach(button => {
        button.addEventListener('mousedown', function() {
            this.style.transform = 'scale(0.95)';
        });
        
        button.addEventListener('mouseup', function() {
            this.style.transform = 'scale(1)';
        });
        
        button.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
        });
    });
}

// Modern Loading States
function showModernLoading(element, text = 'Processing...', icon = 'fa-spinner') {
    if (element) {
        const originalContent = element.innerHTML;
        element.dataset.originalContent = originalContent;
        
        element.innerHTML = `<i class="fas ${icon} fa-spin me-2"></i>${text}`;
        element.disabled = true;
        element.style.transition = 'all 0.3s ease';
        element.style.transform = 'scale(0.98)';
        
        // Add loading glow effect
        if (icon === 'fa-spinner') {
            element.style.boxShadow = '0 0 20px rgba(102, 126, 234, 0.4)';
        }
    }
}

function hideModernLoading(element) {
    if (element && element.dataset.originalContent) {
        element.innerHTML = element.dataset.originalContent;
        element.disabled = false;
        element.style.transform = 'scale(1)';
        element.style.boxShadow = '';
        delete element.dataset.originalContent;
    }
}

// Enhanced Status Messages
function showStatusMessage(message, type = 'info', duration = 5000) {
    const alertClass = type === 'error' ? 'alert-danger' : `alert-${type}`;
    const icon = type === 'error' ? 'fa-exclamation-triangle' : 
                type === 'success' ? 'fa-check-circle' : 
                type === 'warning' ? 'fa-exclamation-triangle' : 'fa-info-circle';
    
    const alertHTML = `
        <div class="alert ${alertClass} alert-dismissible fade show slide-up" role="alert">
            <i class="fas ${icon} me-2"></i>
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        </div>
    `;
    
    // Insert at the top of main content
    const mainContent = document.querySelector('.container-fluid');
    if (mainContent) {
        mainContent.insertAdjacentHTML('afterbegin', alertHTML);
        
        // Auto-dismiss after duration
        if (duration > 0) {
            setTimeout(() => {
                const alert = mainContent.querySelector('.alert');
                if (alert) {
                    alert.style.opacity = '0';
                    alert.style.transform = 'translateY(-20px)';
                    setTimeout(() => alert.remove(), 300);
                }
            }, duration);
        }
    }
}

// Smooth Scroll to Element
function smoothScrollTo(element) {
    if (element) {
        element.scrollIntoView({
            behavior: 'smooth',
            block: 'start'
        });
    }
}

// Copy to Clipboard with Modern Feedback
function copyToClipboard(text, successMessage = 'Copied to clipboard!') {
    navigator.clipboard.writeText(text).then(() => {
        showStatusMessage(successMessage, 'success', 3000);
    }).catch(() => {
        showStatusMessage('Failed to copy to clipboard', 'error', 3000);
    });
}

// Template management with enhanced UX
function saveAsTemplate() {
    const templateName = prompt('💾 Enter a name for this configuration template:');
    if (templateName) {
        const formData = new FormData(document.getElementById('comparisonForm'));
        const templateData = Object.fromEntries(formData);
        
        // Save to localStorage
        let templates = JSON.parse(localStorage.getItem('comparisonTemplates') || '{}');
        templates[templateName] = templateData;
        localStorage.setItem('comparisonTemplates', JSON.stringify(templates));
        
        showStatusMessage(`Template "${templateName}" saved successfully! 🎉`, 'success');
    }
}

function loadTemplate() {
    const templates = JSON.parse(localStorage.getItem('comparisonTemplates') || '{}');
    const templateNames = Object.keys(templates);
    
    if (templateNames.length === 0) {
        showStatusMessage('No saved templates found. Create one first! 📝', 'warning');
        return;
    }
    
    const templateName = prompt(`📋 Choose a template to load:\n\n${templateNames.join('\n')}`);
    if (templateName && templates[templateName]) {
        const templateData = templates[templateName];
        
        // Fill form with template data
        Object.keys(templateData).forEach(key => {
            const field = document.querySelector(`[name="${key}"]`);
            if (field) {
                field.value = templateData[key];
                field.dispatchEvent(new Event('input')); // Trigger any listeners
            }
        });
        
        showStatusMessage(`Template "${templateName}" loaded successfully! ✨`, 'success');
    }
}

// Enhanced comparison status functions
function getSamplingMethodText(method) {
    const methods = {
        'LAST_N': 'most recent',
        'RANDOM': 'random sample of',
        'TOP_N': 'first'
    };
    return methods[method] || 'first';
}

function getDetailedStatusMessage(data) {
    if (data.tables_identical) {
        return `
            <div class="d-flex align-items-center mb-3">
                <i class="fas fa-check-circle text-success me-2 fs-4"></i>
                <h6 class="mb-0 text-success">Perfect Match! 🎉</h6>
            </div>
            <div class="row g-3 text-sm">
                <div class="col-md-3">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-database me-2 text-success"></i>
                        <span>Schema Match</span>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-list-ol me-2 text-success"></i>
                        <span>${data.dev_row_count.toLocaleString()} Rows</span>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-equals me-2 text-success"></i>
                        <span>Data Identical</span>
                    </div>
                </div>
                <div class="col-md-3">
                    <div class="d-flex align-items-center">
                        <i class="fas fa-chart-line me-2 text-success"></i>
                        <span>Complete Analysis</span>
                    </div>
                </div>
            </div>
        `;
    } else {
        let issues = [];
        
        // Schema check
        if (data.schema_differences && data.schema_differences.length > 0) {
            issues.push({
                icon: 'fa-exclamation-triangle',
                text: `Schema differences (${data.schema_differences.length})`,
                type: 'danger'
            });
        } else {
            issues.push({
                icon: 'fa-check',
                text: 'Schema identical',
                type: 'success'
            });
        }
        
        // Row count check
        if (data.dev_row_count !== data.prod_row_count) {
            issues.push({
                icon: 'fa-exclamation-triangle',
                text: `Row count mismatch: DEV(${data.dev_row_count.toLocaleString()}) vs PROD(${data.prod_row_count.toLocaleString()})`,
                type: 'danger'
            });
        } else {
            issues.push({
                icon: 'fa-check',
                text: `Row counts match (${data.dev_row_count.toLocaleString()})`,
                type: 'success'
            });
        }
        
        // Data differences check
        if (data.summary.total_differing > 0 || data.summary.total_missing_dev > 0 || data.summary.total_missing_prod > 0) {
            let dataIssues = [];
            if (data.summary.total_differing > 0) dataIssues.push(`${data.summary.total_differing} differing`);
            if (data.summary.total_missing_dev > 0) dataIssues.push(`${data.summary.total_missing_dev} missing from DEV`);
            if (data.summary.total_missing_prod > 0) dataIssues.push(`${data.summary.total_missing_prod} missing from PROD`);
            
            issues.push({
                icon: 'fa-exclamation-triangle',
                text: `Data differences: ${dataIssues.join(', ')}`,
                type: 'danger'
            });
        } else {
            issues.push({
                icon: 'fa-check',
                text: 'Compared data identical',
                type: 'success'
            });
        }
        
        // Coverage check
        if (data.was_limited) {
            issues.push({
                icon: 'fa-info-circle',
                text: `Limited analysis: ${getSamplingMethodText(data.sampling_method)} ${data.max_rows_setting.toLocaleString()} rows`,
                type: 'warning'
            });
        } else {
            issues.push({
                icon: 'fa-check',
                text: 'Complete table analysis',
                type: 'success'
            });
        }
        
        const issuesList = issues.map(issue => 
            `<div class="d-flex align-items-center mb-2">
                <i class="fas ${issue.icon} me-2 text-${issue.type}"></i>
                <span class="text-${issue.type}">${issue.text}</span>
            </div>`
        ).join('');
        
        return `
            <div class="mb-3">
                <h6 class="text-muted mb-3">
                    <i class="fas fa-clipboard-check me-2"></i>
                    Analysis Results
                </h6>
                ${issuesList}
            </div>
        `;
    }
}

function getComparisonStatusClass(data) {
    return data.tables_identical ? 'card-header bg-success text-white' : 'card-header bg-warning text-dark';
}

// Intersection Observer for scroll animations
function initScrollAnimations() {
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('animate-in');
            }
        });
    }, { threshold: 0.1 });
    
    document.querySelectorAll('.card, .table-pair-item').forEach(el => {
        observer.observe(el);
    });
}