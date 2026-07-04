// Global state
let currentFilter = 'pending';

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    fetchStats();
    fetchJobs();
    setupSSE();
    setupForms();

    // Check which engine is active
    fetch('/api/stats')
        .then(res => res.json())
        .then(() => {
            // Default is SQLite, if we had options we could show it, but SQLite is standard here.
            document.getElementById('backend-type').textContent = 'SQLite Engine';
        });
});

// Setup Server-Sent Events for real-time dashboard updates
func = function setupSSE() {
    const eventSource = new EventSource('/api/events');

    eventSource.onmessage = (event) => {
        if (event.data === 'refresh') {
            triggerRefreshVisual();
            fetchStats();
            fetchJobs();
        }
    };

    eventSource.onerror = (error) => {
        console.error('SSE Error:', error);
        // Fallback to polling every 3 seconds if SSE fails
        setTimeout(() => {
            fetchStats();
            fetchJobs();
        }, 3000);
    };
}
// Fix naming
setupSSE = func;

function triggerRefreshVisual() {
    const indicator = document.getElementById('refresh-indicator');
    indicator.style.opacity = '0.5';
    setTimeout(() => {
        indicator.style.opacity = '1';
    }, 300);
}

// Fetch stats summary
async function fetchStats() {
    try {
        const response = await fetch('/api/stats');
        const stats = await response.json();

        document.getElementById('stat-pending').textContent = stats.pending;
        document.getElementById('stat-processing').textContent = stats.processing;
        document.getElementById('stat-completed').textContent = stats.completed;
        document.getElementById('stat-failed').textContent = stats.failed;
        document.getElementById('stat-dead-letter').textContent = stats.dead_letter;

        // Toggle visibility of DLQ buttons based on dead_letter count
        const dlqCard = document.querySelector('.dlq-actions-card');
        if (stats.dead_letter > 0) {
            dlqCard.style.opacity = '1';
            dlqCard.style.pointerEvents = 'auto';
        } else {
            dlqCard.style.opacity = '0.6';
        }
    } catch (error) {
        console.error('Error fetching stats:', error);
    }
}

// Set active tab and fetch jobs
function filterJobs(state) {
    currentFilter = state;
    
    // Update active UI classes
    document.querySelectorAll('.stat-card').forEach(card => {
        card.classList.remove('active');
    });
    
    let activeCard = document.querySelector(`.stat-card.${state.replace('_', '-')}`);
    if (activeCard) {
        activeCard.classList.add('active');
    }

    // Update title
    const titles = {
        'pending': 'Pending',
        'processing': 'Processing',
        'completed': 'Completed',
        'failed': 'Failed (Retrying)',
        'dead_letter': 'Dead Letter'
    };
    document.getElementById('current-filter-title').textContent = titles[state] || state;

    fetchJobs();
}

// Fetch jobs list for current filter
async function fetchJobs() {
    try {
        const response = await fetch(`/api/jobs?state=${currentFilter}&limit=20`);
        const jobs = await response.json();
        renderJobs(jobs);
    } catch (error) {
        console.error('Error fetching jobs:', error);
    }
}

// Render jobs to table
function renderJobs(jobs) {
    const tbody = document.getElementById('jobs-list');
    tbody.innerHTML = '';

    if (!jobs || jobs.length === 0) {
        tbody.innerHTML = `<tr><td colspan="6" class="empty-state">No jobs found in this state.</td></tr>`;
        return;
    }

    jobs.forEach(job => {
        const tr = document.createElement('tr');
        
        // Format Payload
        let payloadStr = '';
        try {
            const p = JSON.parse(atob(job.payload)); // If base64
            payloadStr = JSON.stringify(p);
        } catch (e) {
            // Assume it's already a UTF-8 string or JSON parsed by JSON decoder
            if (typeof job.payload === 'string') {
                payloadStr = job.payload;
            } else {
                // If it's a binary array or similar, try decoding
                const binary = String.fromCharCode.apply(null, new Uint8Array(job.payload));
                try {
                    payloadStr = JSON.stringify(JSON.parse(binary));
                } catch(err) {
                    payloadStr = binary;
                }
            }
        }
        
        // Clean up visual payload presentation
        try {
            const parsed = JSON.parse(payloadStr);
            payloadStr = parsed.data || payloadStr;
        } catch(e) {}

        // Format Date/Time
        let scheduleLease = '-';
        if (job.state === 'processing') {
            const reservedUntil = new Date(job.reserved_until);
            const remaining = Math.max(0, Math.round((reservedUntil - new Date()) / 1000));
            scheduleLease = `🔒 Lease expires in ${remaining}s`;
        } else if (job.state === 'pending' || job.state === 'failed') {
            const runAt = new Date(job.run_at);
            const now = new Date();
            if (runAt > now) {
                const diff = Math.round((runAt - now) / 1000);
                scheduleLease = `⏳ Scheduled in ${diff}s`;
            } else {
                scheduleLease = '⚡ Run immediately';
            }
        }

        // Actions buttons depending on state
        let actionsHtml = '-';
        if (job.state === 'dead_letter') {
            actionsHtml = `
                <button class="btn-action-icon" onclick="redriveDLQ('${job.id}')" title="Redrive Job">
                    <i class="fa-solid fa-rotate-right"></i>
                </button>
                <button class="btn-action-icon btn-action-delete" onclick="deleteDLQ('${job.id}')" title="Delete Job">
                    <i class="fa-solid fa-trash"></i>
                </button>
            `;
        }

        tr.innerHTML = `
            <td class="job-id-cell" title="${job.id}">${job.id.substring(0, 8)}...</td>
            <td>
                <span class="job-type-badge">${job.type}</span>
                <div style="font-size: 0.75rem; color: var(--text-secondary); margin-top: 0.25rem;">
                    Payload: <code>${payloadStr}</code>
                </div>
            </td>
            <td>${job.retries} / ${job.max_retries}</td>
            <td>${scheduleLease}</td>
            <td>
                <div class="error-text" title="${job.last_error || ''}">
                    ${job.last_error ? `<i class="fa-solid fa-triangle-exclamation"></i> ${job.last_error}` : '-'}
                </div>
            </td>
            <td>${actionsHtml}</td>
        `;
        tbody.appendChild(tr);
    });
}

// Setup Form Submission handlers
function setupForms() {
    const form = document.getElementById('enqueue-form');
    form.addEventListener('submit', async (e) => {
        e.preventDefault();

        const type = document.getElementById('job-type').value;
        const payload = document.getElementById('job-payload').value;
        const delaySec = parseInt(document.getElementById('job-delay').value) || 0;
        const maxRetries = parseInt(document.getElementById('job-max-retries').value) || 3;
        const forceFail = document.getElementById('job-force-fail').checked;

        try {
            const response = await fetch('/api/jobs', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ type, payload, delay_sec: delaySec, max_retries: maxRetries, force_fail: forceFail })
            });

            if (response.ok) {
                showToast('Job successfully enqueued!', 'success');
                form.reset();
                // Set default values back
                document.getElementById('job-delay').value = 0;
                document.getElementById('job-max-retries').value = 3;
                fetchStats();
                fetchJobs();
            } else {
                const text = await response.text();
                showToast(`Enqueue failed: ${text}`, 'error');
            }
        } catch (error) {
            showToast('Network error enqueuing job', 'error');
        }
    });
}

// Redrive DLQ jobs (all or specific job ID)
async function redriveDLQ(jobId = null) {
    const body = {};
    if (jobId) {
        body.job_ids = [jobId];
    }

    try {
        const response = await fetch('/api/jobs/redrive', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        if (response.ok) {
            const res = await response.json();
            showToast(`Redriven ${res.redriven_count} job(s)`, 'success');
            fetchStats();
            fetchJobs();
        } else {
            showToast('Failed to redrive jobs', 'error');
        }
    } catch (error) {
        showToast('Network error on redrive', 'error');
    }
}

// Purge DLQ jobs (all or specific job ID)
async function deleteDLQ(jobId = null) {
    const body = {};
    if (jobId) {
        body.job_ids = [jobId];
    } else {
        if (!confirm('Are you sure you want to delete ALL dead lettered jobs? This cannot be undone.')) {
            return;
        }
    }

    try {
        const response = await fetch('/api/jobs/delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body)
        });

        if (response.ok) {
            showToast(jobId ? 'Job deleted' : 'DLQ purged', 'success');
            fetchStats();
            fetchJobs();
        } else {
            showToast('Failed to delete jobs', 'error');
        }
    } catch (error) {
        showToast('Network error on delete', 'error');
    }
}

// Helper to show toasts
function showToast(message, type = 'info') {
    const toast = document.getElementById('toast');
    toast.className = `toast ${type} show`;
    toast.innerHTML = `
        <i class="fa-solid ${type === 'success' ? 'fa-circle-check' : type === 'error' ? 'fa-circle-xmark' : 'fa-info-circle'}"></i>
        <span>${message}</span>
    `;

    setTimeout(() => {
        toast.classList.remove('show');
    }, 3000);
}
