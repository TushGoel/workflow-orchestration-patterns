"""Tests for the circuit breaker pattern."""

import pytest
import time
from python.patterns.circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerOpenError


def test_closed_by_default():
    cb = CircuitBreaker()
    assert cb.state == CircuitState.CLOSED


def test_passes_through_on_success():
    cb = CircuitBreaker()
    result = cb.call(lambda: "ok")
    assert result == "ok"
    assert cb.state == CircuitState.CLOSED


def test_opens_after_threshold_failures():
    cb = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
    assert cb.state == CircuitState.OPEN


def test_fast_fails_when_open():
    cb = CircuitBreaker(failure_threshold=1)
    with pytest.raises(Exception):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("fail")))
    assert cb.state == CircuitState.OPEN
    with pytest.raises(CircuitBreakerOpenError):
        cb.call(lambda: "should not reach")


def test_transitions_to_half_open_after_timeout():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01)
    with pytest.raises(Exception):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("fail")))
    assert cb.state == CircuitState.OPEN
    time.sleep(0.02)
    assert cb.state == CircuitState.HALF_OPEN


def test_closes_after_successful_probe():
    cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.01, success_threshold=1)
    with pytest.raises(Exception):
        cb.call(lambda: (_ for _ in ()).throw(RuntimeError("fail")))
    time.sleep(0.02)
    assert cb.state == CircuitState.HALF_OPEN
    cb.call(lambda: "recovered")
    assert cb.state == CircuitState.CLOSED
