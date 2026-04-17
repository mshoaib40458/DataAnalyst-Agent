"""
Security utilities: input validation, log redaction, HTML sanitization,
and privacy-first data masking for sensitive fields.
"""
import logging
import re
from typing import Optional

import bleach
import pandas as pd

logger = logging.getLogger(__name__)

# ── Log redaction patterns ──────────────────────────────────────────────────

API_KEY_PATTERN = re.compile(
    r'(api[_\-]?key|x-api-key|authorization[:\s]+bearer\s+)(\S+)',
    re.IGNORECASE
)
DATABASE_URL_PATTERN = re.compile(
    r'(postgres|mysql|sqlite|mongodb)://\S+',
    re.IGNORECASE
)
TOKEN_PATTERN = re.compile(
    r'(token|secret|password)[:\s]*(\S{8,})',
    re.IGNORECASE
)


def redact_sensitive_data(text: str) -> str:
    """Redacts API keys, database URLs, and tokens from log messages."""
    text = API_KEY_PATTERN.sub(r'\1[REDACTED]', text)
    text = DATABASE_URL_PATTERN.sub('[DATABASE_URL_REDACTED]', text)
    text = TOKEN_PATTERN.sub(r'\1[REDACTED]', text)
    return text


class RedactingFormatter(logging.Formatter):
    """Custom logging formatter that redacts sensitive data from all log records."""
    def format(self, record: logging.LogRecord) -> str:
        original_msg = super().format(record)
        return redact_sensitive_data(original_msg)


def setup_redacting_logger(logger_instance: logging.Logger) -> None:
    """Configures a logger to use the redacting formatter on all its handlers."""
    for handler in logger_instance.handlers:
        if isinstance(handler, logging.StreamHandler):
            handler.setFormatter(
                RedactingFormatter(
                    "%(asctime)s %(levelname)s %(name)s %(message)s"
                )
            )


# ── Input validation ────────────────────────────────────────────────────────

def validate_table_name(table_name: str, max_length: int = 63) -> bool:
    """
    Validates SQL table name (PostgreSQL: max 63 chars, alphanumeric + underscore).
    Returns True if valid, False otherwise.
    """
    if not table_name or len(table_name) > max_length:
        return False
    return bool(re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", table_name))


def validate_column_name(column_name: str, max_length: int = 63) -> bool:
    """Validates SQL column name using same rules as table name validation."""
    return validate_table_name(column_name, max_length)


def sanitize_html(html_text: str) -> str:
    """
    Sanitizes HTML to prevent XSS. Allows safe structural tags,
    strips script tags entirely (including contents).
    """
    allowed_tags = {
        'p', 'br', 'strong', 'em', 'u', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
        'ul', 'ol', 'li', 'blockquote', 'code', 'pre', 'table', 'tr', 'td', 'th',
        'div', 'span'
    }
    allowed_attributes: dict = {
        '*': ['class', 'id'],
        'a': ['href', 'title']
    }
    html_text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    return bleach.clean(html_text, tags=allowed_tags, attributes=allowed_attributes, strip=True)


def sanitize_markdown_output(markdown_text: str) -> str:
    """
    Sanitizes LLM markdown output before sending to frontend.
    Strips script tags and inline event handlers.
    Note: Pair with DOMPurify + markdown-it on the client for full XSS protection.
    """
    markdown_text = re.sub(r'<script[^>]*>.*?</script>', '', markdown_text, flags=re.DOTALL | re.IGNORECASE)
    markdown_text = re.sub(r'on\w+\s*=', '', markdown_text, flags=re.IGNORECASE)
    return markdown_text


def validate_file_size(file_size_bytes: int, max_size_bytes: int) -> tuple[bool, str]:
    """
    Validates file size against a maximum.
    Returns (is_valid, error_message).
    """
    if file_size_bytes <= 0:
        return False, "File size must be greater than 0 bytes"
    if file_size_bytes > max_size_bytes:
        max_mb = max_size_bytes / (1024 * 1024)
        return False, f"File too large. Maximum allowed size is {max_mb:.1f} MB"
    return True, ""


# ── Data masking (Privacy-First) ────────────────────────────────────────────

_EMAIL_CONTENT_RE = re.compile(r'^[\w.+\-]+@[\w\-]+\.[\w.]+$')

_SENSITIVE_COL_PATTERNS: dict[str, re.Pattern] = {
    'email': re.compile(
        r'\b(email|e_mail|mail|e[-_]mail)\b', re.IGNORECASE
    ),
    'id': re.compile(
        r'\b(cnic|nic|national_id|ssn|passport|id_no|id_number|nid|tax_id)\b',
        re.IGNORECASE
    ),
    'financial': re.compile(
        r'\b(salary|income|wage|payment|balance|account_no|credit|debit|bank_acc|revenue|earnings)\b',
        re.IGNORECASE
    ),
    'phone': re.compile(
        r'\b(phone|mobile|cell|contact_no|tel|telephone)\b', re.IGNORECASE
    ),
}


def _mask_email_value(value: str) -> str:
    """Masks email: john.doe@example.com → j***@example.com"""
    if not isinstance(value, str) or '@' not in value:
        return value
    local, domain = value.split('@', 1)
    return f"{local[0]}***@{domain}"


def _mask_id_value(value: str) -> str:
    """Masks ID/CNIC: shows first 2 and last 2 chars only."""
    s = str(value) if not isinstance(value, str) else value
    if len(s) <= 4:
        return '****'
    return s[:2] + '*' * (len(s) - 4) + s[-2:]


def _mask_financial_value(value) -> str:
    """Masks financial: shows only last 3 digits of integer portion."""
    try:
        num = float(value)
        s = f"{abs(num):,.0f}"
        prefix = '-' if num < 0 else ''
        return f"{prefix}***{s[-3:]}" if len(s) > 3 else f"{prefix}***"
    except (ValueError, TypeError):
        return '***'


def _detect_sensitive_type(col_name: str, series: pd.Series) -> Optional[str]:
    """
    Detect sensitive column type by name pattern first, then content sampling.
    Returns one of: 'email', 'id', 'financial', 'phone', or None.
    """
    for sens_type, pattern in _SENSITIVE_COL_PATTERNS.items():
        if pattern.search(col_name):
            return sens_type
    # Content-based email detection for object columns
    if series.dtype == object:
        sample = series.dropna().head(30)
        if len(sample) > 0:
            email_hits = sum(
                1 for v in sample
                if isinstance(v, str) and _EMAIL_CONTENT_RE.match(v)
            )
            if email_hits / len(sample) > 0.5:
                return 'email'
    return None


def mask_sensitive_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a copy of df with automatically detected sensitive columns masked:
    - Emails     → a***@domain.com
    - ID / CNIC  → XX***XX (first 2 + stars + last 2)
    - Financial  → ***XYZ  (last 3 digits of integer portion)
    - Phone      → XX***XX

    Detection uses column name patterns and content sampling.
    Only affects identified sensitive columns; all others are unchanged.
    """
    masked = df.copy()
    for col in masked.columns:
        sens_type = _detect_sensitive_type(col, masked[col])
        if sens_type == 'email':
            masked[col] = masked[col].apply(
                lambda v: _mask_email_value(str(v)) if pd.notna(v) else v
            )
        elif sens_type in ('id', 'phone'):
            masked[col] = masked[col].apply(
                lambda v: _mask_id_value(str(v)) if pd.notna(v) else v
            )
        elif sens_type == 'financial':
            masked[col] = masked[col].apply(
                lambda v: _mask_financial_value(v) if pd.notna(v) else v
            )
    return masked
