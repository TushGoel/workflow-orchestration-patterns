"""
Circuit breaker pattern for resilient service calls.

Production pattern: when a downstream service is failing, retrying immediately
makes it worse. The circuit breaker opens after threshold failures, fast-fails
requests during the open state, and probes recovery with a half-open state.

States:
  CLOSED  → normal operation, tracking failure count
  OPEN    → fast-fail all requests (downstream is unhealthy)
  HALF_OPEN → allow one probe request to test recovery
"""

import time
import functools
import logging
from enum import Enum
from threading import Lock
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerOpenError(Exception):
    """Raised when a call is rejected by an open circuit breaker."""


class CircuitBreaker:
    """
    Thread-safe circuit breaker.

    Args:
        failure_threshold: Failures before opening the circuit.
        recovery_timeout: Seconds to wait before probing recovery (HALF_OPEN).
        success_threshold: Successes in HALF_OPEN before closing.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        success_threshold: int = 2,
        name: str = "circuit-breaker",
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._evaluate_state()

    def _evaluate_state(self) -> CircuitState:
        if self._state == CircuitState.OPEN:
            if self._last_failure_time and \
               time.monotonic() - self._last_failure_time >= self.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info("[%s] Circuit → HALF_OPEN (probing recovery)", self.name)
        return self._state

    def call(self, func: Callable, *args, **kwargs):
        with self._lock:
            state = self._evaluate_state()

            if state == CircuitState.OPEN:
                raise CircuitBreakerOpenError(
                    f"Circuit '{self.name}' is OPEN — downstream service unhealthy"
                )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as exc:
            self._on_failure()
            raise exc

    def _on_success(self) -> None:
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info("[%s] Circuit → CLOSED (service recovered)", self.name)
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    def _on_failure(self) -> None:
        with self._lock:
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning("[%s] Circuit → OPEN (probe failed)", self.name)
            elif self._state == CircuitState.CLOSED:
                self._failure_count += 1
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        "[%s] Circuit → OPEN after %d failures",
                        self.name, self._failure_count
                    )

    def __call__(self, func: Callable) -> Callable:
        """Use as a decorator."""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper
