"""
In-memory dataset store for privacy-first (secure) mode.
Datasets are held in process memory and never written to disk.
Automatically discarded after analysis completes or on error.
"""
import threading
import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)

_lock = threading.Lock()
_store: Dict[str, pd.DataFrame] = {}


def store_dataset(job_id: str, df: pd.DataFrame) -> None:
    """Store a DataFrame in-memory for a given job. Thread-safe."""
    with _lock:
        _store[job_id] = df.copy()
    logger.info(
        "Dataset stored in-memory for job %s (%d rows, %d cols)",
        job_id, len(df), len(df.columns)
    )


def get_dataset(job_id: str) -> Optional[pd.DataFrame]:
    """Retrieve a DataFrame for a given job. Returns None if not found. Thread-safe."""
    with _lock:
        df = _store.get(job_id)
        return df.copy() if df is not None else None


def update_dataset(job_id: str, df: pd.DataFrame) -> None:
    """Replace the in-memory dataset for a job (e.g., after cleaning). Thread-safe."""
    with _lock:
        if job_id in _store:
            _store[job_id] = df.copy()
            logger.info(
                "In-memory dataset updated for job %s (%d rows remaining)",
                job_id, len(df)
            )
        else:
            logger.warning(
                "Attempted to update non-existent in-memory dataset for job %s", job_id
            )


def discard_dataset(job_id: str) -> None:
    """
    Remove dataset from memory. Called after analysis completes or on error.
    Implements the 'Discard Data' step of the secure model.
    """
    with _lock:
        if job_id in _store:
            del _store[job_id]
            logger.info("In-memory dataset discarded for job %s", job_id)


def has_dataset(job_id: str) -> bool:
    """Check if a dataset exists in memory for a given job. Thread-safe."""
    with _lock:
        return job_id in _store
