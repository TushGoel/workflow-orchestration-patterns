"""
Retry with exponential backoff and jitter.

Production pattern: transient failures are normal in distributed systems.
Never retry with fixed intervals — always use exponential backoff with jitter
to prevent thundering herd when many services recover simultaneously.
"""

import time
import random
import functools
import logging
from typing import Callable, Tuple, Type

logger = logging.getLogger(__name__)


def retry(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    backoff_rate: float = 2.0,
    max_delay: float = 30.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True,
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_attempts: Maximum number of attempts (including the first).
        base_delay: Initial delay in seconds.
        backoff_rate: Multiplier applied after each failure.
        max_delay: Maximum delay cap in seconds.
        exceptions: Exception types to retry on (default: all exceptions).
        jitter: Add random jitter to prevent thundering herd.

    Pattern: delay = min(base_delay * backoff_rate^attempt, max_delay) ± jitter
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exception = exc
                    if attempt == max_attempts - 1:
                        break
                    delay = min(base_delay * (backoff_rate ** attempt), max_delay)
                    if jitter:
                        delay *= (0.5 + random.random())  # ±50% jitter
                    logger.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.2fs",
                        attempt + 1, max_attempts, func.__name__, exc, delay
                    )
                    time.sleep(delay)
            raise last_exception
        return wrapper
    return decorator


class RetryableError(Exception):
    """Marker exception — signals the caller should retry."""


class NonRetryableError(Exception):
    """Marker exception — signals the caller should NOT retry."""
