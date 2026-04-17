# Flask Frontend for Autonomous AI Data Analyst Agent

Modern, responsive web UI built with **Flask** + **Jinja2** templates. The browser calls the FastAPI backend directly.

## Features

- **Flask-based UI**: Server-rendered templates with dynamic configuration
- **Direct API Calls**: Browser talks to FastAPI (CORS-enabled)
- **API Key Support**: Optional X-API-Key header forwarding
- **Real-time Polling**: WebSocket-ready status updates via REST polling
- **Interactive Visualizations**: Plotly charts rendered in browser
- **Markdown Rendering**: LLM insights with marked.js + DOMPurify sanitization
- **Dark Mode UI**: Glass-morphism design with smooth animations

## Project Structure

```
frontend/
├── app.py                 # Flask application (UI only)
├── templates/
│   └── index.html         # Jinja2 template (Flask-aware)
├── static/
│   ├── style.css          # Glass-morphism CSS
│   ├── app.js             # Client-side logic (vanilla JS)
│   └── ...
├── requirements.txt       # (Inherited from root)
└── .env                   # Environment variables
```

## Installation

### Prerequisites
- Python 3.9+
- FastAPI backend running (see parent README)

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Copy `.env.example` from root and set Flask-specific variables:

```bash
cp ../.env.example .env
```

Edit `.env`:

```env
# Flask Frontend Configuration
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
FLASK_DEBUG=false

# Backend Configuration
BACKEND_URL=http://localhost:8000
REQUIRE_API_KEY=false
API_KEY=your-optional-key

# App
APP_VERSION=0.1.0
```

### 3. Run Flask Development Server

```bash
python frontend/app.py
```

Or using Flask CLI:

```bash
export FLASK_APP=frontend.app
flask run --host 0.0.0.0 --port 5000
```

### 4. Open in Browser

```
http://localhost:5000
```

The FastAPI backend should still be running on port 8000 (or configured `BACKEND_URL`).

## Environment Variables

### Required

- **BACKEND_URL**: URL of FastAPI backend (default: `http://localhost:8000`)

### Optional

- **FLASK_HOST**: Host to bind to (default: `0.0.0.0`)
- **FLASK_PORT**: Port to run on (default: `5000`)
- **FLASK_DEBUG**: Enable debug mode (default: `false`)
- **REQUIRE_API_KEY**: Require X-API-Key header (default: `false`)
- **API_KEY**: Shared API key (if `REQUIRE_API_KEY=true`)
- **APP_VERSION**: Application version string

## API Endpoints (Called Directly by Browser)

The frontend JavaScript calls the FastAPI backend at `BACKEND_URL`.

### Upload Endpoints
 - `POST /upload_dataset` - Upload CSV file
 - `POST /upload_sql_table` - Ingest from SQL database

### Analysis Endpoints
 - `POST /start_analysis` - Start workflow
 - `GET /analysis_status/{job_id}` - Poll job status
 - `GET /download_report/{job_id}?format=json|html|pdf` - Download report

### History
 - `GET /analysis_history` - Retrieve past insights

### Frontend-Specific
 - `GET /` - Serve the UI
 - `GET /health` - Health check
 - `GET /api/config` - Retrieve frontend config

## Authentication

### When REQUIRE_API_KEY=false (Default)

Direct access to all endpoints. No authentication needed.

### When REQUIRE_API_KEY=true

Include `X-API-Key` header in all requests:

```bash
curl -H "X-API-Key: your-key" http://localhost:5000/health
```

**Browser-based requests**: Store API key in browser localStorage:

```javascript
localStorage.setItem('API_KEY', 'your-key');
```

The frontend JavaScript automatically forwards this in the `X-API-Key` header.


## Deployment

### Development

```bash
python frontend/app.py
```

### Production (with Gunicorn)

```bash
pip install gunicorn
gunicorn -w 4 -b 0.0.0.0:5000 frontend.app:app
```

## Comparison: Flask vs Original Vanilla HTML Frontend

| Feature | Flask | Original |
|---------|-------|----------|
| **Server-side** | Yes (Jinja2) | No (Static) |
| **Dynamic Config** | Yes (env vars passed to template) | Manual updates |
| **Sessions** | Supported | localStorage only |
| **CSRF Protection** | Built-in (optional) | Manual |
| **Route Handling** | Centralized Flask routes | Direct fetch() calls |
| **Scalability** | Multi-worker (Gunicorn) | Single app.main |
| **Development** | Debug mode with auto-reload | Manual browser refresh |

## Architecture Diagram

```
User Browser
    ↓
[Flask Frontend : 5000]
    ├─ Templates: Jinja2 (index.html)
    ├─ Static: CSS, JS
    └─ Routes: Python UI handlers
    ↓
[FastAPI Backend : 8000]
    ├─ Health checks
    ├─ File uploads
    ├─ Analysis workflows
    └─ Report generation
    ↓
[PostgreSQL Database]
[File Storage: data/uploads, data/reports]
```

## Troubleshooting

### "Connection refused" to backend

- Ensure FastAPI backend is running on configured `BACKEND_URL`
- Check `BACKEND_URL` environment variable
- Verify firewall rules

### "Unauthorized" (401) errors

- If `REQUIRE_API_KEY=true`, ensure:
  - API key is set in `.env`
  - Browser has `localStorage.API_KEY` set to same value
  - X-API-Key header is being sent

### "ImportError: No module named 'flask'"

```bash
pip install -r requirements.txt
```

### CORS errors in browser

- Ensure `CORS_ORIGINS` includes `http://localhost:5000` (default now does)
- Restart FastAPI after env changes

## Development

### Debug Mode

```env
FLASK_DEBUG=true
```

Enables:
- Auto-reload on file changes
- Detailed error pages
- Interactive debugger

### Adding New Routes

Edit `frontend/app.py`:

```python
@app.route("/new-endpoint", methods=["GET"])
def new_endpoint():
  return jsonify({"message": "Hello from Flask!"})
```

### Modifying Templates

Edit `frontend/templates/index.html` using Jinja2 syntax:

```html
<h1>{{ app_version }}</h1>
```

## Performance Notes

- For high-traffic deployments, use Gunicorn + Nginx reverse proxy
- Consider caching headers for static assets
- Browser caching of reports via ETag headers (future enhancement)

## Security

- ✅ API key authentication forwarding
- ✅ CORS handled by backend
- ✅ No sensitive data in logs
- ⚠️ Enable HTTPS in production (via reverse proxy)
- ⚠️ Set secure session cookies (future enhancement)

## Notes

- Frontend is stateless (all state stored in backend database)
- JavaScript remains vanilla (no framework dependencies)
- Responsive design tested on Chrome, Firefox, Safari, Edge
- Mobile support via responsive CSS grid layouts
