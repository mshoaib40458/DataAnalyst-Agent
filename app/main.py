from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, Header, Depends, Request, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import os
import io
import uuid
import shutil

# Guarantee .env is forcefully loaded into the Uvicorn parent process
if os.path.exists(".env"):
    with open(".env") as f:
        for line in f:
            if line.strip() and not line.startswith("#") and "=" in line:
                key, val = line.strip().split("=", 1)
                os.environ.setdefault(key.strip(), val.strip().strip("'\""))

import re
import logging
import secrets
import time
from contextlib import contextmanager

# Safe monkey-patch for langchain-core backwards compatibility issues
try:
    import langchain
    if not hasattr(langchain, "debug"):
        langchain.debug = False
    if not hasattr(langchain, "verbose"):
        langchain.verbose = False
    if not hasattr(langchain, "llm_cache"):
        langchain.llm_cache = None
except ImportError:
    pass
from typing import Generator
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text, create_engine

from app.db.database import engine, Base, SessionLocal
from app.db.models import AnalysisJob, AnalysisMemory
import json
from app.utils.security import validate_table_name, setup_redacting_logger
from app.utils.rate_limit import (
    check_rate_limit, can_start_analysis, extract_client_id,
    increment_concurrent_job, decrement_concurrent_job,
    RATE_LIMIT_UPLOADS_PER_MINUTE, RATE_LIMIT_ANALYSIS_PER_MINUTE
)
from app.utils.cleanup import cleanup_uploads, cleanup_reports, log_storage_stats

# Try to create DB tables, but don't fail startup if DB is unavailable (for development)
try:
    Base.metadata.create_all(bind=engine)
except Exception as db_init_error:
    print(f"[WARNING] Could not initialize database at startup: {db_init_error}")

# ── Configuration ────────────────────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")

# Privacy-first controls (UPDATED README)
ENABLE_SECURE_MODE = os.getenv("ENABLE_SECURE_MODE", "false").lower() == "true"
DISABLE_DATA_PERSISTENCE = os.getenv("DISABLE_DATA_PERSISTENCE", "false").lower() == "true"
ENABLE_DATA_MASKING = os.getenv("ENABLE_DATA_MASKING", "false").lower() == "true"

# Default 200 MB per UPDATED README spec (previous code defaulted to 50 MB incorrectly)
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_BYTES", str(200 * 1024 * 1024)))

REQUIRE_API_KEY = os.getenv("REQUIRE_API_KEY", "false").lower() == "true"
API_KEY = os.getenv("API_KEY", "")
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "60000"))
TRUSTED_HOSTS_ENV = os.getenv("TRUSTED_HOSTS", "*")
TRUSTED_HOSTS = [host.strip() for host in TRUSTED_HOSTS_ENV.split(",") if host.strip()]

app = FastAPI(
    title="Autonomous AI Data Intelligence Agent (Privacy-First)",
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc"
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# Apply log redaction to all handlers
setup_redacting_logger(logger)
setup_redacting_logger(logging.getLogger())  # Root logger
setup_redacting_logger(logging.getLogger("uvicorn"))
setup_redacting_logger(logging.getLogger("uvicorn.access"))
setup_redacting_logger(logging.getLogger("uvicorn.error"))

cors_origins_env = os.getenv(
    "CORS_ORIGINS",
    "http://localhost:8000,http://127.0.0.1:8000,http://localhost:5000,http://127.0.0.1:5000"
)
cors_origins = [origin.strip() for origin in cors_origins_env.split(",") if origin.strip()]


@contextmanager
def get_db_session() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    if not REQUIRE_API_KEY:
        return
    if not API_KEY:
        logger.error("REQUIRE_API_KEY=true but API_KEY is empty — server misconfiguration")
        raise HTTPException(status_code=500, detail="Server API key is not configured")
    if not x_api_key or not secrets.compare_digest(x_api_key, API_KEY):
        raise HTTPException(status_code=401, detail="Invalid API key")


app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-API-Key"],
)
app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=TRUSTED_HOSTS or ["*"])

# Ensure storage directories exist (reports still written even in secure mode)
os.makedirs("data/uploads", exist_ok=True)
os.makedirs("data/reports", exist_ok=True)

# Mount frontend directly in FastAPI for 1-click cloud deployments without CORS issues
app.mount("/static", StaticFiles(directory="frontend/static"), name="static")
templates = Jinja2Templates(directory="frontend/templates")

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """
    Apply sliding-window rate limits:
    - Upload endpoints: RATE_LIMIT_UPLOADS_PER_MINUTE per client
    - Analysis start: RATE_LIMIT_ANALYSIS_PER_MINUTE per client
    """
    if request.url.path in {"/health", "/ready"}:
        return await call_next(request)

    client_id = extract_client_id(request)

    if request.url.path.startswith("/upload"):
        allowed, msg = check_rate_limit(client_id, RATE_LIMIT_UPLOADS_PER_MINUTE, window_seconds=60)
        if not allowed:
            logger.warning("Upload rate limit exceeded for client %s: %s", client_id, msg)
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many upload requests. Please try again later."}
            )

    if request.url.path in {"/start_analysis"}:
        allowed, msg = check_rate_limit(client_id, RATE_LIMIT_ANALYSIS_PER_MINUTE, window_seconds=60)
        if not allowed:
            logger.warning("Analysis rate limit exceeded for client %s: %s", client_id, msg)
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many analysis requests. Please try again later."}
            )

    return await call_next(request)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    """Attach X-Request-ID and X-Response-Time-Ms to every response."""
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "Unhandled server error request_id=%s path=%s", request_id, request.url.path
        )
        response = JSONResponse(status_code=500, content={"detail": "Internal server error"})

    duration_ms = (time.perf_counter() - start) * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time-Ms"] = f"{duration_ms:.2f}"
    logger.info(
        "request_id=%s method=%s path=%s status=%s duration_ms=%.2f",
        request_id, request.method, request.url.path,
        response.status_code, duration_ms
    )
    if duration_ms > REQUEST_TIMEOUT_MS:
        logger.warning(
            "Slow request detected request_id=%s path=%s duration_ms=%.2f",
            request_id, request.url.path, duration_ms
        )
    return response


@app.on_event("startup")
def startup_checks() -> None:
    # API key validation
    groq_key = os.getenv("GROQ_API_KEY") or os.getenv("XAI_API_KEY")
    if not groq_key:
        logger.warning(
            "GROQ_API_KEY is not set. LLM-powered analysis steps will fail. "
            "Set GROQ_API_KEY in your .env file."
        )

    # Log active privacy mode
    if ENABLE_SECURE_MODE or DISABLE_DATA_PERSISTENCE:
        logger.info(
            "Privacy-first mode active: SECURE_MODE=%s DATA_PERSISTENCE_DISABLED=%s MASKING=%s",
            ENABLE_SECURE_MODE, DISABLE_DATA_PERSISTENCE, ENABLE_DATA_MASKING
        )

    # Reset any jobs stuck in processing state from a previous crash
    with get_db_session() as db:
        try:
            stuck_jobs = db.query(AnalysisJob).filter(
                AnalysisJob.status.in_(["processing", "pending_cleaning", "pending_approval"])
            ).all()
            for job in stuck_jobs:
                job.status = "error"
                job.error_message = "Job abruptly terminated due to server restart."
            if stuck_jobs:
                db.commit()
                logger.warning(
                    "Reverted %d stuck processing job(s) to error state on startup.",
                    len(stuck_jobs)
                )
        except Exception as e:
            logger.error("Failed to revert stuck jobs on startup: %s", e)

    # Run file cleanup
    if not DISABLE_DATA_PERSISTENCE:
        uploads_deleted, uploads_freed = cleanup_uploads(dry_run=False)
        reports_deleted, reports_freed = cleanup_reports(dry_run=False)
        logger.info(
            "Cleanup: %d uploads freed %.2f MB, %d reports freed %.2f MB",
            uploads_deleted, uploads_freed / 1024 / 1024,
            reports_deleted, reports_freed / 1024 / 1024
        )
        log_storage_stats()

    logger.info("Application startup complete. version=%s", APP_VERSION)


# ── Utility ──────────────────────────────────────────────────────────────────

def validate_dataframe_schema(df) -> None:
    if df is None or df.empty:
        raise HTTPException(status_code=400, detail="Dataset is empty. Please upload a non-empty dataset.")
    if len(df.columns) < 1:
        raise HTTPException(status_code=400, detail="Dataset must contain at least one column.")


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_frontend(request: Request):
    """Serve the main dashboard directly from FastAPI."""
    return templates.TemplateResponse("index.html", {
        "request": request,
        "app_version": APP_VERSION,
        "require_api_key": REQUIRE_API_KEY,
        "backend_url": ""  # Leave empty to enforce relative paths in JS
    })

@app.get("/api/config")
async def get_frontend_config():
    """Provide configuration flags to the browser JavaScript."""
    return {
        "backend_url": "",
        "require_api_key": REQUIRE_API_KEY,
        "app_version": APP_VERSION
    }


@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "api_key_required": REQUIRE_API_KEY,
        "secure_mode": ENABLE_SECURE_MODE,
        "data_masking": ENABLE_DATA_MASKING
    }


@app.get("/ready")
async def readiness_check():
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except Exception as exc:
        logger.error("Readiness database check failed: %s", exc)
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "database_unavailable"}
        )

    # Skip upload dir check when persistence is disabled (uploads live in memory)
    if not DISABLE_DATA_PERSISTENCE:
        uploads_ok = os.path.isdir("data/uploads") and os.access("data/uploads", os.W_OK)
        if not uploads_ok:
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "reason": "upload_storage_unavailable"}
            )

    reports_ok = os.path.isdir("data/reports") and os.access("data/reports", os.W_OK)
    if not reports_ok:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "reason": "reports_storage_unavailable"}
        )

    return {"status": "ready", "version": APP_VERSION}


@app.post("/upload_dataset")
async def upload_dataset(file: UploadFile = File(...), _: None = Depends(verify_api_key)):
    """
    Upload a dataset for analysis.
    - DISABLE_DATA_PERSISTENCE=true: processed in-memory only, never written to disk.
    - ENABLE_DATA_MASKING=true: sensitive fields are masked before storage.
    """
    import pandas as pd

    original_name = file.filename or "uploaded.csv"
    safe_name = os.path.basename(original_name)
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", safe_name)

    if not safe_name.lower().endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are supported currently.")

    # Stream content to enforce size limit securely without memory-bombing
    content_chunks = []
    total_size = 0
    while True:
        chunk = await file.read(1024 * 1024)  # 1MB chunks
        if not chunk:
            break
        total_size += len(chunk)
        if total_size > MAX_UPLOAD_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"File too large. Max allowed: {MAX_UPLOAD_SIZE // (1024 * 1024)} MB."
            )
        content_chunks.append(chunk)
    content = b"".join(content_chunks)

    file_id = str(uuid.uuid4())

    if DISABLE_DATA_PERSISTENCE:
        # ── Secure mode: parse to DataFrame, never write to disk ─────────────
        def _validate_and_store_in_memory():
            df = pd.read_csv(io.BytesIO(content))
            validate_dataframe_schema(df)
            if ENABLE_DATA_MASKING:
                from app.utils.security import mask_sensitive_dataframe
                df = mask_sensitive_dataframe(df)
                logger.info("Sensitive fields masked for job %s before in-memory storage.", file_id)
            from app.utils.data_store import store_dataset
            store_dataset(file_id, df)

        try:
            await run_in_threadpool(_validate_and_store_in_memory)
        except HTTPException:
            raise
        except Exception as parse_err:
            raise HTTPException(
                status_code=400,
                detail=f"Dataset validation failed: {parse_err}"
            ) from parse_err

        file_path = f"memory://{file_id}"

    else:
        # ── Legacy mode: write to disk ────────────────────────────────────────
        file_path = f"data/uploads/{file_id}_{safe_name}"

        def _save_file():
            with open(file_path, "wb") as buffer:
                buffer.write(content)
        await run_in_threadpool(_save_file)

        try:
            def _validate_csv():
                validation_df = pd.read_csv(file_path, nrows=1000)
                validate_dataframe_schema(validation_df)
            await run_in_threadpool(_validate_csv)
        except HTTPException:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise
        except Exception as parse_err:
            if os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(
                status_code=400,
                detail=f"Dataset schema validation failed: {parse_err}"
            ) from parse_err

    # Persist job record to DB
    with get_db_session() as db:
        try:
            job = AnalysisJob(id=file_id, status="uploaded", file_path=file_path, filename=safe_name)
            db.add(job)
            db.commit()
        except SQLAlchemyError as db_err:
            db.rollback()
            logger.exception("Failed to persist uploaded job: %s", db_err)
            if not DISABLE_DATA_PERSISTENCE and os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(status_code=500, detail="Failed to create analysis job") from db_err

    return {"message": "File uploaded successfully", "job_id": file_id}


class SQLUploadRequest(BaseModel):
    database_url: str
    table_name: str
    limit: int = 30000


@app.post("/upload_sql_table")
async def upload_sql_table(req: SQLUploadRequest, _: None = Depends(verify_api_key)):
    """
    Load a table from a trusted database for analysis.
    Executes aggregation-safe SELECT with a row limit.
    """
    import pandas as pd

    table_name = req.table_name.strip()
    if not validate_table_name(table_name):
        raise HTTPException(
            status_code=400,
            detail="Invalid table name (must be alphanumeric with underscore, max 63 chars)"
        )
    if req.limit < 1 or req.limit > 30000:
        raise HTTPException(status_code=400, detail="Limit must be between 1 and 30000")

    file_id = str(uuid.uuid4())

    if DISABLE_DATA_PERSISTENCE:
        def _load_sql_to_memory():
            source_engine = create_engine(req.database_url)
            try:
                query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                df = pd.read_sql(query, source_engine, params={"limit": req.limit})
                validate_dataframe_schema(df)
                if ENABLE_DATA_MASKING:
                    from app.utils.security import mask_sensitive_dataframe
                    df = mask_sensitive_dataframe(df)
                from app.utils.data_store import store_dataset
                store_dataset(file_id, df)
            finally:
                source_engine.dispose()

        try:
            await run_in_threadpool(_load_sql_to_memory)
        except HTTPException:
            raise
        except Exception as sql_err:
            logger.exception("Failed SQL table load to memory")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to load SQL table: {sql_err}"
            ) from sql_err

        file_path = f"memory://{file_id}"
    else:
        safe_filename = f"{table_name}.csv"
        file_path = f"data/uploads/{file_id}_{safe_filename}"

        def _load_sql():
            source_engine = create_engine(req.database_url)
            try:
                query = text(f"SELECT * FROM {table_name} LIMIT :limit")
                df = pd.read_sql(query, source_engine, params={"limit": req.limit})
                validate_dataframe_schema(df)
                df.to_csv(file_path, index=False)
            finally:
                source_engine.dispose()

        try:
            await run_in_threadpool(_load_sql)
        except HTTPException:
            raise
        except Exception as sql_err:
            logger.exception("Failed SQL table upload")
            raise HTTPException(
                status_code=400,
                detail=f"Failed to load SQL table: {sql_err}"
            ) from sql_err

    with get_db_session() as db:
        try:
            job = AnalysisJob(
                id=file_id, status="uploaded",
                file_path=file_path,
                filename=f"{table_name}.csv"
            )
            db.add(job)
            db.commit()
        except SQLAlchemyError as db_err:
            db.rollback()
            if not DISABLE_DATA_PERSISTENCE and os.path.exists(file_path):
                os.remove(file_path)
            raise HTTPException(status_code=500, detail="Failed to create analysis job") from db_err

    return {"message": "SQL table uploaded successfully", "job_id": file_id}


class AnalysisRequest(BaseModel):
    job_id: str


@app.post("/start_analysis")
async def start_analysis(
    req: AnalysisRequest,
    background_tasks: BackgroundTasks,
    request: Request,
    _: None = Depends(verify_api_key)
):
    """
    Start autonomous analysis pipeline (UPDATED README workflow):
    Profile → Question → Plan → Execute → Insight → Report
    """
    client_id = extract_client_id(request)

    can_start, msg = can_start_analysis(client_id)
    if not can_start:
        logger.warning("Concurrent job limit exceeded for client %s: %s", client_id, msg)
        raise HTTPException(status_code=429, detail=msg)

    with get_db_session() as db:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == req.job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")

        if job.status == "processing":
            return {"message": "Analysis already in progress", "job_id": req.job_id}
        if job.status == "completed":
            return {"message": "Analysis already completed", "job_id": req.job_id}

        if job.status not in {"uploaded", "error"}:
            raise HTTPException(
                status_code=409,
                detail=f"Cannot start analysis from status: {job.status}"
            )

        job.status = "processing"
        job.error_message = ""
        try:
            db.commit()
        except SQLAlchemyError as db_err:
            db.rollback()
            logger.exception("Failed to transition job to processing: %s", db_err)
            raise HTTPException(status_code=500, detail="Failed to start analysis") from db_err

    increment_concurrent_job(client_id)

    def cleanup_job():
        decrement_concurrent_job(client_id)

    from app.agent.graph import run_autonomous_pipeline
    background_tasks.add_task(run_autonomous_pipeline, req.job_id)
    background_tasks.add_task(cleanup_job)
    return {"message": "Analysis started", "job_id": req.job_id}


@app.get("/analysis_status/{job_id}")
async def get_status(job_id: str, _: None = Depends(verify_api_key)):
    with get_db_session() as db:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {
            "job_id": job.id,
            "status": job.status,
            "progress_logs": job.progress_logs,
            "error_message": job.error_message
        }



@app.get("/download_report/{job_id}")
async def download_report(
    job_id: str,
    format: str = Query(default="json", pattern="^(json|html|pdf)$"),
    _: None = Depends(verify_api_key)
):
    report_path = ""
    with get_db_session() as db:
        job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
        if not job or job.status != "completed":
            raise HTTPException(status_code=404, detail="Report not available")
        if format == "json":
            report_path = job.result_path or f"data/reports/{job_id}.json"
        elif format == "html":
            report_path = f"data/reports/{job_id}.html"
        else:
            report_path = f"data/reports/{job_id}.pdf"

    if not os.path.exists(report_path):
        raise HTTPException(status_code=404, detail="Report file not found")

    media_type = None
    if format == "html":
        media_type = "text/html"
    elif format == "pdf":
        media_type = "application/pdf"
    return FileResponse(report_path, media_type=media_type)


@app.get("/analysis_history")
async def analysis_history(
    limit: int = Query(default=20, ge=1, le=200),
    _: None = Depends(verify_api_key)
):
    """
    Returns schema-based historical insights only.
    No raw data is ever stored or returned here — only column schema fingerprints
    and LLM-generated insight summaries (aggregated, safe outputs).
    """
    with get_db_session() as db:
        rows = (
            db.query(AnalysisMemory)
            .order_by(AnalysisMemory.created_at.desc())
            .limit(limit)
            .all()
        )

    return {
        "history": [
            {
                "job_id": row.job_id,
                "schema_fingerprint": row.schema_fingerprint,
                "insights_summary": row.insights_summary,
                "created_at": row.created_at.isoformat()
            }
            for row in rows
        ]
    }
