document.addEventListener('DOMContentLoaded', () => {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    
    const welcomeState = document.getElementById('welcome-state');
    const analysisState = document.getElementById('analysis-state');
    const terminalLogs = document.getElementById('terminal-logs');
    const resultsContainer = document.getElementById('results-container');
    const aiInsightsContent = document.getElementById('ai-insights-content');
    const profileStats = document.getElementById('profile-stats');
    const planList = document.getElementById('plan-list');
    const questionsList = document.getElementById('questions-list');
    const chartContainer = document.getElementById('chart-container');
    const jobStatusTitle = document.getElementById('job-status-title');
    const downloadBtn = document.getElementById('download-btn');
    const downloadJsonBtn = document.getElementById('download-json-btn');
    const downloadHtmlBtn = document.getElementById('download-html-btn');
    const downloadPdfBtn = document.getElementById('download-pdf-btn');
    const sqlDbUrlInput = document.getElementById('sql-db-url');
    const sqlTableNameInput = document.getElementById('sql-table-name');
    const sqlUploadBtn = document.getElementById('sql-upload-btn');
    
    const cleaningModal = document.getElementById('cleaning-modal');
    const plannerModal = document.getElementById('planner-modal');
    const cleaningPlanContainer = document.getElementById('cleaning-plan-container');
    const modalQuestionsList = document.getElementById('modal-questions-list');
    const modalPlanList = document.getElementById('modal-plan-list');
    const approveCleaningBtn = document.getElementById('approve-cleaning-btn');
    const approvePlanBtn = document.getElementById('approve-plan-btn');

    let currentCleaningPlan = null;
    let currentAnalysisPlan = null;
    let currentAnalyticalQuestions = null;

    let currentJobId = null;
    let pollingInterval = null;
    let pollFailureCount = 0;
    const maxPollFailures = 5;
    const apiKey = localStorage.getItem('API_KEY');
    const requireApiKey = window.APP_CONFIG && window.APP_CONFIG.require_api_key;
    const backendBase = (window.APP_CONFIG && window.APP_CONFIG.backend_url) || '';

    function buildHeaders(extraHeaders = {}) {
        const headers = { ...extraHeaders };
        if (apiKey) headers['X-API-Key'] = apiKey;
        return headers;
    }

    function buildUrl(path) {
        if (!backendBase) return path;
        const base = backendBase.replace(/\/+$/, '');
        const normalized = path.startsWith('/') ? path : `/${path}`;
        return `${base}${normalized}`;
    }

    async function readError(res) {
        try {
            const data = await res.json();
            return data.detail || data.error || 'Unknown error';
        } catch (e) {
            return 'Unknown error';
        }
    }

    function stopPolling() {
        if (pollingInterval) {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
    }

    function authorizationHint(statusCode) {
        if (statusCode === 401) {
            return ' Unauthorized request. Set localStorage API_KEY to match API_KEY.';
        }
        return '';
    }

    function setStatusWithLoader(message) {
        jobStatusTitle.textContent = message;
        const loader = document.createElement('span');
        loader.className = 'loader';
        jobStatusTitle.appendChild(document.createTextNode(' '));
        jobStatusTitle.appendChild(loader);
    }

    function setStatusWithIcon(message, iconClass, color) {
        jobStatusTitle.textContent = '';
        const icon = document.createElement('i');
        icon.className = iconClass;
        if (color) icon.style.color = color;
        jobStatusTitle.appendChild(icon);
        jobStatusTitle.appendChild(document.createTextNode(` ${message}`));
    }

    // File Upload Handlers
    dropZone.addEventListener('click', () => fileInput.click());
    
    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });
    
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
    
    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]);
    });

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length) handleFile(e.target.files[0]);
    });

    sqlUploadBtn.addEventListener('click', () => {
        const dbUrl = (sqlDbUrlInput.value || '').trim();
        const tableName = (sqlTableNameInput.value || '').trim();
        if (!dbUrl || !tableName) {
            alert('Enter both database URL and table name.');
            return;
        }
        handleSqlUpload(dbUrl, tableName);
    });

    async function handleFile(file) {
        if (!file.name.toLowerCase().endsWith('.csv')) {
            alert('Please upload a CSV file.');
            return;
        }

        // Switch UI state
        stopPolling();
        pollFailureCount = 0;
        welcomeState.classList.add('hidden');
        analysisState.classList.remove('hidden');
        resultsContainer.classList.add('hidden');
        terminalLogs.textContent = '';
        downloadBtn.classList.add('hidden');
        setStatusWithLoader('Uploading Dataset...');

        const formData = new FormData();
        formData.append('file', file);

        try {
            logTerminal(`[System]: Initiating upload for ${file.name}...`);
            const res = await fetch(buildUrl('/upload_dataset'), {
                method: 'POST',
                headers: buildHeaders(),
                body: formData
            });
            if (res.ok) {
                const data = await res.json();
                currentJobId = data.job_id;
                logTerminal(`[System]: Upload successful. Job ID: ${currentJobId}`);
                startAnalysis(currentJobId);
            } else {
                const errorMsg = await readError(res);
                logTerminal(`[Error]: Upload failed - ${errorMsg}${authorizationHint(res.status)}`, true);
            }
        } catch (error) {
            console.error('Upload Error:', error);
            logTerminal(`[Error]: Upload failed - Network Error`, true);
        }
    }

    async function handleSqlUpload(databaseUrl, tableName) {
        stopPolling();
        pollFailureCount = 0;
        welcomeState.classList.add('hidden');
        analysisState.classList.remove('hidden');
        resultsContainer.classList.add('hidden');
        terminalLogs.textContent = '';
        downloadBtn.classList.add('hidden');
        setStatusWithLoader('Loading SQL Table...');

        try {
            logTerminal(`[System]: Loading SQL table ${tableName}...`);
            const res = await fetch(buildUrl('/upload_sql_table'), {
                method: 'POST',
                headers: buildHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({
                    database_url: databaseUrl,
                    table_name: tableName,
                    limit: 30000
                })
            });
            if (res.ok) {
                const data = await res.json();
                currentJobId = data.job_id;
                logTerminal(`[System]: SQL table upload successful. Job ID: ${currentJobId}`);
                startAnalysis(currentJobId);
            } else {
                const errorMsg = await readError(res);
                logTerminal(`[Error]: SQL upload failed - ${errorMsg}${authorizationHint(res.status)}`, true);
            }
        } catch (error) {
            console.error('SQL Upload Error:', error);
            logTerminal('[Error]: SQL upload failed - Network Error', true);
        }
    }

    async function startAnalysis(jobId) {
        try {
            logTerminal('[System]: Requesting analysis start...');
            setStatusWithLoader('Analysis in Progress...');
            const res = await fetch(buildUrl('/start_analysis'), {
                method: 'POST',
                headers: buildHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ job_id: jobId })
            });
            if (res.ok) {
                stopPolling();
                // Start polling status
                pollingInterval = setInterval(() => pollStatus(jobId), 1500);
            } else {
                const errorMsg = await readError(res);
                logTerminal(`[Error]: Start analysis failed - ${errorMsg}${authorizationHint(res.status)}`, true);
            }
        } catch (error) {
            console.error('Start Error:', error);
            logTerminal(`[Error]: Start analysis failed - Network Error`, true);
        }
    }

    async function pollStatus(jobId) {
        try {
            const res = await fetch(buildUrl(`/analysis_status/${jobId}`), {
                headers: buildHeaders()
            });
            if (!res.ok) {
                pollFailureCount += 1;
                if (pollFailureCount >= maxPollFailures) {
                    stopPolling();
                    setStatusWithIcon('Analysis status unavailable', 'fa-solid fa-triangle-exclamation text-error', '#ef4444');
                    const apiHint = requireApiKey ? ' If API key auth is enabled, ensure localStorage API_KEY is set.' : '';
                    logTerminal(`[Error]: Status polling failed repeatedly. Please retry.${apiHint}`, true);
                }
                return;
            }
            const data = await res.json();
            pollFailureCount = 0;
            
            // Sync logs
            if (data.progress_logs) {
                terminalLogs.textContent = data.progress_logs;
                terminalLogs.scrollTop = terminalLogs.scrollHeight;
            }

            if (data.status === 'pending_cleaning') {
                stopPolling();
                setStatusWithIcon('Review Cleaning Proposals', 'fa-solid fa-broom text-yellow', '#f59e0b');
                logTerminal('[System]: Analysis paused. Waiting for human approval of data cleaning proposals.');
                fetchAndShowCleaningModal(jobId);
            } else if (data.status === 'pending_approval') {
                stopPolling();
                setStatusWithIcon('Review Analysis Plan', 'fa-solid fa-sitemap text-indigo', '#818cf8');
                logTerminal('[System]: Analysis paused. Waiting for human approval of execution plan.');
                fetchAndShowPlannerModal(jobId);
            } else if (data.status === 'completed') {
                stopPolling();
                logTerminal('[System]: Workflow execution successful.');
                setStatusWithIcon('Analysis Complete', 'fa-solid fa-check-circle text-success', '#10b981');
                downloadBtn.classList.remove('hidden');
                
                downloadBtn.onclick = () => downloadReport(jobId, 'pdf');
                downloadJsonBtn.onclick = () => downloadReport(jobId, 'json');
                downloadHtmlBtn.onclick = () => downloadReport(jobId, 'html');
                downloadPdfBtn.onclick = () => downloadReport(jobId, 'pdf');

                fetchAndRenderReport(jobId);
                
            } else if (data.status === 'error') {
                stopPolling();
                setStatusWithIcon('Analysis Failed', 'fa-solid fa-times-circle text-error', '#ef4444');
                logTerminal(`[CRITICAL ERROR]: ${data.error_message}`, true);
            }
            
        } catch (error) {
            console.error('Poll Error:', error);
            pollFailureCount += 1;
            if (pollFailureCount >= maxPollFailures) {
                stopPolling();
                setStatusWithIcon('Analysis status unavailable', 'fa-solid fa-triangle-exclamation text-error', '#ef4444');
                logTerminal('[Error]: Network polling failed repeatedly. Please retry.', true);
            }
        }
    }

    async function fetchAndRenderReport(jobId) {
        try {
            logTerminal('[System]: Fetching report data...');
            const res = await fetch(buildUrl(`/download_report/${jobId}?format=json`), {
                headers: buildHeaders()
            });
            if (!res.ok) throw new Error("Report not generated");
            const report = await res.json();
            
            renderReport(report);
            resultsContainer.classList.remove('hidden');
            logTerminal('[System]: UI Results Rendered.');

        } catch (error) {
            console.error('Fetch Report Error:', error);
            logTerminal(`[System]: Failed to fetch UI JSON report - ${error.message}`, true);
        }
    }

    function renderReport(report) {
        // 1. Render Insights
        if(report.insights) {
             const rendered = marked.parse(report.insights);
             aiInsightsContent.innerHTML = DOMPurify.sanitize(rendered);
        }
        
        // 2. Render Profile Stats
        const profile = report.profile;
        if(profile) {
            profileStats.textContent = '';

            const stats = [
                { label: 'Total Rows', value: profile.num_rows },
                { label: 'Total Columns', value: profile.num_cols }
            ];

            stats.forEach((stat) => {
                const item = document.createElement('div');
                item.className = 'stat-item';

                const value = document.createElement('div');
                value.className = 'stat-value';
                value.textContent = String(stat.value ?? 'N/A');

                const label = document.createElement('div');
                label.className = 'stat-label';
                label.textContent = stat.label;

                item.appendChild(value);
                item.appendChild(label);
                profileStats.appendChild(item);
            });
        }

        // 3. Render Plan
        const plan = report.plan;
        if(plan && Array.isArray(plan)) {
            planList.textContent = '';
            plan.forEach((step, index) => {
                const li = document.createElement('li');
                li.textContent = step && step.task ? String(step.task) : `Task ${index + 1}`;
                planList.appendChild(li);
            });
        }

        // 4. Render Ranked Analytical Questions
        const questions = report.analytical_questions;
        if (questions && Array.isArray(questions)) {
            questionsList.textContent = '';
            questions.forEach((q, index) => {
                const li = document.createElement('li');
                const rank = q && q.rank ? `#${q.rank}` : `#${index + 1}`;
                const question = q && q.question ? String(q.question) : 'Question unavailable';
                li.textContent = `${rank} ${question}`;
                questionsList.appendChild(li);
            });
        }

        // 5. Render Visualizations (multiple chart specs)
        chartContainer.textContent = '';
        const chartSpecs = report.visualizations && Array.isArray(report.visualizations.chart_specs)
            ? report.visualizations.chart_specs
            : [];

        if (chartSpecs.length > 0) {
            chartSpecs.forEach((spec, idx) => {
                const chartDiv = document.createElement('div');
                chartDiv.id = `chart-${idx}`;
                chartDiv.style.minHeight = '320px';
                chartDiv.style.marginBottom = '1rem';
                chartContainer.appendChild(chartDiv);

                const layout = {
                    ...(spec.layout || {}),
                    title: spec.title || 'Chart',
                    paper_bgcolor: 'rgba(0,0,0,0)',
                    plot_bgcolor: 'rgba(0,0,0,0)',
                    font: { color: '#94a3b8' }
                };
                Plotly.newPlot(chartDiv, spec.data || [], layout, { responsive: true });
            });
        } else {
            chartContainer.innerHTML = '<div class="empty-state">No visualizations applicable for this dataset structure.</div>';
        }
    }

    function logTerminal(msg, isError = false) {
        const span = document.createElement('div');
        span.textContent = msg;
        if (isError) span.style.color = '#ef4444';
        terminalLogs.appendChild(span);
        terminalLogs.scrollTop = terminalLogs.scrollHeight;
    }

    async function fetchAndShowCleaningModal(jobId) {
        try {
            const res = await fetch(buildUrl(`/job_cleaning/${jobId}`), { headers: buildHeaders() });
            const data = await res.json();
            if (res.ok) {
                currentCleaningPlan = data.cleaning_plan || [];
                cleaningPlanContainer.textContent = '';
                if (currentCleaningPlan.length === 0) {
                    cleaningPlanContainer.textContent = 'No specific cleaning required.';
                } else {
                    const pre = document.createElement('pre');
                    pre.className = 'json-view';
                    pre.textContent = JSON.stringify(currentCleaningPlan, null, 2);
                    cleaningPlanContainer.appendChild(pre);
                }
                cleaningModal.classList.remove('hidden');
            } else {
                logTerminal('[Error]: Failed to fetch cleaning plan', true);
            }
        } catch (e) {
            logTerminal('[Error]: Network error fetching cleaning plan', true);
        }
    }

    async function fetchAndShowPlannerModal(jobId) {
        try {
            const res = await fetch(buildUrl(`/job_plan/${jobId}`), { headers: buildHeaders() });
            const data = await res.json();
            if (res.ok) {
                currentAnalyticalQuestions = data.analytical_questions || [];
                currentAnalysisPlan = data.analysis_plan || [];
                
                modalQuestionsList.textContent = '';
                currentAnalyticalQuestions.forEach(q => {
                    const li = document.createElement('li');
                    li.textContent = q.question || JSON.stringify(q);
                    modalQuestionsList.appendChild(li);
                });

                modalPlanList.textContent = '';
                currentAnalysisPlan.forEach(p => {
                    const li = document.createElement('li');
                    li.textContent = p.task || JSON.stringify(p);
                    modalPlanList.appendChild(li);
                });

                plannerModal.classList.remove('hidden');
            } else {
                logTerminal('[Error]: Failed to fetch analysis plan', true);
            }
        } catch (e) {
            logTerminal('[Error]: Network error fetching analysis plan', true);
        }
    }

    approveCleaningBtn.addEventListener('click', async () => {
        cleaningModal.classList.add('hidden');
        logTerminal('[System]: Sending approved cleaning plan to engine...');
        setStatusWithLoader('Executing Cleaning...');
        try {
            const res = await fetch(buildUrl('/approve_cleaning'), {
                method: 'POST',
                headers: buildHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ job_id: currentJobId, cleaning_plan: currentCleaningPlan })
            });
            if (res.ok) {
                logTerminal('[System]: Cleaning approved. Resuming background workflows...');
                pollingInterval = setInterval(() => pollStatus(currentJobId), 1500);
            } else {
                logTerminal('[Error]: Failed to approve cleaning', true);
            }
        } catch (e) {
            logTerminal('[Error]: Network error during approval', true);
        }
    });

    approvePlanBtn.addEventListener('click', async () => {
        plannerModal.classList.add('hidden');
        logTerminal('[System]: Sending approved execution plan to engine...');
        setStatusWithLoader('Executing Analysis...');
        try {
            const res = await fetch(buildUrl('/approve_plan'), {
                method: 'POST',
                headers: buildHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ job_id: currentJobId, analysis_plan: currentAnalysisPlan })
            });
            if (res.ok) {
                logTerminal('[System]: Execution plan approved. Engine starting execution phase...');
                pollingInterval = setInterval(() => pollStatus(currentJobId), 1500);
            } else {
                logTerminal('[Error]: Failed to approve plan', true);
            }
        } catch (e) {
            logTerminal('[Error]: Network error during approval', true);
        }
    });

    window.addEventListener('beforeunload', () => {
        stopPolling();
    });

    async function downloadReport(jobId, format) {
        try {
            const res = await fetch(buildUrl(`/download_report/${jobId}?format=${format}`), {
                headers: buildHeaders()
            });
            if (!res.ok) {
                const errorMsg = await readError(res);
                logTerminal(`[Error]: Download failed - ${errorMsg}${authorizationHint(res.status)}`, true);
                return;
            }
            const blob = await res.blob();
            const fileExt = format === 'pdf' ? 'pdf' : (format === 'html' ? 'html' : 'json');
            const filename = `report_${jobId}.${fileExt}`;

            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = filename;
            document.body.appendChild(link);
            link.click();
            link.remove();
            URL.revokeObjectURL(url);
        } catch (error) {
            logTerminal('[Error]: Download failed - Network Error', true);
        }
    }

    // --- Chat with Data Logic ---
    const chatBtn = document.getElementById('chat-btn');
    const chatInput = document.getElementById('chat-input');
    const chatHistory = document.getElementById('chat-history');

    async function sendChatMessage() {
        if (!currentJobId) return;
        const query = chatInput.value.trim();
        if (!query) return;

        // Add user message
        const userMsg = document.createElement('div');
        userMsg.style = "align-self: flex-end; background: #3b82f6; color: white; padding: 8px 12px; border-radius: 8px; max-width: 80%; word-wrap: break-word;";
        userMsg.innerText = query;
        chatHistory.appendChild(userMsg);
        
        chatInput.value = '';
        chatInput.disabled = true;
        chatBtn.disabled = true;
        chatBtn.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i>';
        
        chatHistory.scrollTop = chatHistory.scrollHeight;

        try {
            const res = await fetch(buildUrl(`/chat/${currentJobId}`), {
                method: 'POST',
                headers: buildHeaders({ 'Content-Type': 'application/json' }),
                body: JSON.stringify({ query: query })
            });
            
            const data = await res.json();
            
            const aiMsg = document.createElement('div');
            aiMsg.style = "align-self: flex-start; background: #334155; color: #f8fafc; padding: 8px 12px; border-radius: 8px; max-width: 80%; word-wrap: break-word; border:1px solid #475569;";
            if (res.ok) {
                aiMsg.innerText = data.response;
            } else {
                aiMsg.innerText = `Error: ${data.detail || 'Could not process query.'}`;
            }
            chatHistory.appendChild(aiMsg);
        } catch (error) {
            const aiMsg = document.createElement('div');
            aiMsg.style = "align-self: flex-start; background: #7f1d1d; color: #f8fafc; padding: 8px 12px; border-radius: 8px; border:1px solid #991b1b;";
            aiMsg.innerText = "Network error. Please try again.";
            chatHistory.appendChild(aiMsg);
        } finally {
            chatInput.disabled = false;
            chatBtn.disabled = false;
            chatBtn.innerHTML = 'Send <i class="fa-solid fa-paper-plane"></i>';
            chatHistory.scrollTop = chatHistory.scrollHeight;
            chatInput.focus();
        }
    }

    if (chatBtn && chatInput) {
        chatBtn.addEventListener('click', sendChatMessage);
        chatInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') sendChatMessage();
        });
    }

});
