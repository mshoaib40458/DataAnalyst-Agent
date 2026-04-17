"""
Rate limiting and request throttling utilities.
Thread-safe for single-instance deployments via threading.Lock.
For multi-instance deployments, replace _store with Redis.
"""
import time
import threading
import logging
from collections import defaultdict
from typing import Dict, Tuple
import os

logger = logging.getLogger(__name__)

# Rate limiting configuration
RATE_LIMIT_REQUESTS_PER_MINUTE = int(os.getenv("RATE_LIMIT_REQUESTS_PER_MINUTE", "60"))
RATE_LIMIT_UPLOADS_PER_MINUTE = int(os.getenv("RATE_LIMIT_UPLOADS_PER_MINUTE", "10"))
RATE_LIMIT_ANALYSIS_PER_MINUTE = int(os.getenv("RATE_LIMIT_ANALYSIS_PER_MINUTE", "20"))
RATE_LIMIT_ANALYSIS_CONCURRENT = int(os.getenv("RATE_LIMIT_ANALYSIS_CONCURRENT", "5"))

# Thread-safe in-memory rate limit state
# WARNING: per-process only — does not share state across multiple Uvicorn workers.
_lock = threading.Lock()
_rate_limit_store: Dict[str, list] = defaultdict(list)
_concurrent_jobs: Dict[str, int] = defaultdict(int)


def check_rate_limit(client_id: str, limit: int, window_seconds: int = 60) -> Tuple[bool, str]:
    """
    Check if a client has exceeded its rate limit (thread-safe).
    Slide-window counter — automatically expires old timestamps.
    Returns (is_allowed, message).
    """
    now = time.time()
    window_start = now - window_seconds

    with _lock:
        # Evict expired timestamps then check + record atomically
        _rate_limit_store[client_id] = [
            ts for ts in _rate_limit_store[client_id]
            if ts > window_start
        ]
        request_count = len(_rate_limit_store[client_id])
        if request_count >= limit:
            return False, f"Rate limit exceeded: {limit} requests per {window_seconds}s"
        _rate_limit_store[client_id].append(now)

    return True, ""


def can_start_analysis(client_id: str) -> Tuple[bool, str]:
    """
    Check if a client can start a new analysis job (thread-safe concurrent limit).
    The check and the subsequent increment must be performed as an atomic pair
    at the call site to avoid a race; use increment_concurrent_job immediately after.
    Returns (is_allowed, message).
    """
    with _lock:
        if _concurrent_jobs[client_id] >= RATE_LIMIT_ANALYSIS_CONCURRENT:
            return False, f"Too many concurrent analysis jobs. Max: {RATE_LIMIT_ANALYSIS_CONCURRENT}"
    return True, ""


def increment_concurrent_job(client_id: str) -> None:
    """Record that a client started a new analysis job (thread-safe)."""
    with _lock:
        _concurrent_jobs[client_id] += 1


def decrement_concurrent_job(client_id: str) -> None:
    """Record that a client's analysis job finished (thread-safe, never goes negative)."""
    with _lock:
        if _concurrent_jobs[client_id] > 0:
            _concurrent_jobs[client_id] -= 1


def extract_client_id(request) -> str:
    """
    Extract client identifier securely from a FastAPI Request.
    Protects against X-Forwarded-For spoofing by requiring explicit proxy trust.
    """
    trust_proxy = os.getenv("TRUST_FORWARDED_IP", "false").lower() == "true"
    if trust_proxy:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"
