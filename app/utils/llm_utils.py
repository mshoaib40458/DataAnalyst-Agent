"""
LLM utilities with retry logic and token budgeting for the Groq API.
"""
import logging
import os
from typing import Any, Callable

from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
    RetryError
)

logger = logging.getLogger(__name__)

LLM_MAX_RETRIES = int(os.getenv("LLM_MAX_RETRIES", "3"))
LLM_INITIAL_WAIT_MS = int(os.getenv("LLM_INITIAL_WAIT_MS", "1000"))

# Import Groq-specific exception types so the retry decorator actually fires
# on real API failures (rate limits, timeouts, connection drops).
# Falls back to generic exception types if groq package is absent.
try:
    from groq import (
        APIStatusError as _GroqAPIStatusError,
        RateLimitError as _GroqRateLimitError,
        APIConnectionError as _GroqAPIConnectionError,
        APITimeoutError as _GroqAPITimeoutError,
    )
    _RETRYABLE_EXCEPTIONS: tuple = (
        _GroqAPIStatusError,
        _GroqRateLimitError,
        _GroqAPIConnectionError,
        _GroqAPITimeoutError,
        RuntimeError,
        OSError,
    )
    logger.debug("Groq-specific exception types loaded for LLM retry targeting.")
except ImportError:
    _RETRYABLE_EXCEPTIONS = (RuntimeError, OSError)
    logger.debug(
        "groq package exceptions not available; "
        "LLM retry will only fire on RuntimeError/OSError."
    )


def with_llm_retry(func: Callable) -> Callable:
    """
    Decorator for LLM calls with exponential backoff retry.
    Correctly targets Groq API exceptions (rate limit, connection, timeout, status).
    """
    @retry(
        retry=retry_if_exception_type(_RETRYABLE_EXCEPTIONS),
        wait=wait_exponential(
            multiplier=1,
            min=LLM_INITIAL_WAIT_MS / 1000,
            max=60
        ),
        stop=stop_after_attempt(LLM_MAX_RETRIES),
        reraise=True,
        before_sleep=lambda retry_state: logger.warning(
            "LLM call failed (attempt %d/%d), retrying in %.1fs...",
            retry_state.attempt_number,
            LLM_MAX_RETRIES,
            retry_state.next_action.sleep
        )
    )
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


def safe_llm_call(llm_func: Callable, fallback_value: Any = None) -> Callable:
    """
    Wrapper for non-critical LLM operations that returns a fallback value
    on exhausted retries or unexpected failures.
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return with_llm_retry(llm_func)(*args, **kwargs)
        except RetryError as e:
            logger.error("LLM call exhausted all retries: %s", e)
            return fallback_value
        except Exception as e:
            logger.error("Unexpected LLM call failure: %s", e)
            return fallback_value

    return wrapper


def estimate_prompt_tokens(text: str) -> int:
    """
    Rough token count estimation: 1 token ≈ 4 characters.
    Use Groq's tokenizer endpoint for accurate counting.
    """
    return len(text) // 4


def enforce_token_budget(text: str, max_tokens: int = 2000) -> str:
    """
    Truncate text to stay within an estimated token budget.
    Attempts to cut at a clean newline boundary to avoid producing
    structurally broken JSON or mid-sentence truncations.
    """
    estimated = estimate_prompt_tokens(text)
    if estimated <= max_tokens:
        return text

    max_chars = max_tokens * 4
    truncated = text[:max_chars]

    # Prefer cutting at a newline close to (but not past) the limit
    break_pos = truncated.rfind('\n')
    if break_pos > int(max_chars * 0.8):
        truncated = truncated[:break_pos]

    logger.warning(
        "Text truncated from ~%d to ~%d tokens to respect budget of %d tokens.",
        estimated,
        estimate_prompt_tokens(truncated),
        max_tokens
    )
    return truncated
