"""
File cleanup and retention policy utilities.
"""
import os
import time
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

# Retention policy (hours)
UPLOAD_RETENTION_HOURS = int(os.getenv("UPLOAD_RETENTION_HOURS", "72"))   # 3 days
REPORT_RETENTION_HOURS = int(os.getenv("REPORT_RETENTION_HOURS", "720"))  # 30 days
COMPLETED_JOB_RETENTION_HOURS = int(os.getenv("COMPLETED_JOB_RETENTION_HOURS", "1440"))  # 60 days


def cleanup_old_files(directory: str, cutoff_hours: int, dry_run: bool = False) -> Tuple[int, int]:
    """
    Clean up files older than cutoff_hours.
    Returns (deleted_count, freed_bytes)

    Args:
        directory: Path to clean
        cutoff_hours: Delete files older than this many hours
        dry_run: If True, don't actually delete
    """
    if not os.path.isdir(directory):
        return 0, 0

    now = time.time()
    cutoff_seconds = cutoff_hours * 3600
    deleted_count = 0
    freed_bytes = 0

    try:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if not os.path.isfile(filepath):
                continue

            file_age_seconds = now - os.path.getmtime(filepath)
            if file_age_seconds > cutoff_seconds:
                file_size = os.path.getsize(filepath)

                if not dry_run:
                    try:
                        os.remove(filepath)
                        deleted_count += 1
                        freed_bytes += file_size
                        logger.info(f"Deleted old file: {filename} ({file_size} bytes)")
                    except OSError as e:
                        logger.warning(f"Failed to delete {filename}: {e}")
                else:
                    deleted_count += 1
                    freed_bytes += file_size

    except Exception as e:
        logger.error(f"Error during cleanup of {directory}: {e}")

    return deleted_count, freed_bytes


def cleanup_uploads(dry_run: bool = False) -> Tuple[int, int]:
    """Clean old upload files."""
    return cleanup_old_files("data/uploads", UPLOAD_RETENTION_HOURS, dry_run)


def cleanup_reports(dry_run: bool = False) -> Tuple[int, int]:
    """Clean old report files."""
    return cleanup_old_files("data/reports", REPORT_RETENTION_HOURS, dry_run)


def get_directory_size(directory: str) -> int:
    """Get total size of all files in directory (recursively)."""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if os.path.isfile(filepath):
                    total_size += os.path.getsize(filepath)
    except Exception as e:
        logger.error(f"Error calculating directory size: {e}")

    return total_size


def log_storage_stats() -> None:
    """Log current storage usage."""
    uploads_size = get_directory_size("data/uploads") / (1024 * 1024)  # MB
    reports_size = get_directory_size("data/reports") / (1024 * 1024)  # MB

    logger.info(
        f"Storage usage - Uploads: {uploads_size:.2f} MB, Reports: {reports_size:.2f} MB, "
        f"Total: {uploads_size + reports_size:.2f} MB"
    )
