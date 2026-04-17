"""
Flask Frontend Application for Autonomous AI Data Analyst Agent.
Serves the UI only; the browser calls the FastAPI backend directly.
"""
import os
import logging
from flask import Flask, render_template, jsonify

app = Flask(__name__, template_folder="templates", static_folder="static")

# Configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://127.0.0.1:8000")
REQUIRE_API_KEY = os.getenv("REQUIRE_API_KEY", "false").lower() == "true"
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================================
# FRONTEND ROUTES
# ============================================================================

@app.route("/")
def index():
    """Serve main dashboard page."""
    return render_template(
        "index.html",
        app_version=APP_VERSION,
        require_api_key=REQUIRE_API_KEY,
        backend_url=BACKEND_URL
    )


@app.route("/health")
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "ok",
        "version": APP_VERSION,
        "frontend": "flask"
    })


# ============================================================================
# UTILITY ROUTES
# ============================================================================

@app.route("/api/config", methods=["GET"])
def get_config():
    """Get frontend configuration."""
    return jsonify({
        "backend_url": BACKEND_URL,
        "require_api_key": REQUIRE_API_KEY,
        "app_version": APP_VERSION
    })


if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    port = int(os.getenv("FLASK_PORT", "5000"))
    host = os.getenv("FLASK_HOST", "0.0.0.0")

    logger.info(f"Starting Flask frontend on {host}:{port}")
    logger.info(f"Backend URL: {BACKEND_URL}")

    app.run(host=host, port=port, debug=debug)
