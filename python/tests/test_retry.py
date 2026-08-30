"""Tests for retry with exponential backoff."""

import pytest
from unittest.mock import patch
from python.patterns.retry import retry, RetryableError


def test_succeeds_on_first_attempt():
    call_count = 0

    @retry(max_attempts=3)
    def succeeds():
        nonlocal call_count
        call_count += 1
        return "ok"

    assert succeeds() == "ok"
    assert call_count == 1


def test_retries_and_succeeds():
    call_count = 0

    @retry(max_attempts=3, base_delay=0)
    def fails_twice():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RetryableError("transient failure")
        return "ok"

    assert fails_twice() == "ok"
    assert call_count == 3


def test_raises_after_max_attempts():
    call_count = 0

    @retry(max_attempts=3, base_delay=0)
    def always_fails():
        nonlocal call_count
        call_count += 1
        raise RetryableError("permanent failure")

    with pytest.raises(RetryableError):
        always_fails()
    assert call_count == 3


def test_only_retries_specified_exceptions():
    call_count = 0

    @retry(max_attempts=3, base_delay=0, exceptions=(RetryableError,))
    def raises_value_error():
        nonlocal call_count
        call_count += 1
        raise ValueError("not retryable")

    with pytest.raises(ValueError):
        raises_value_error()
    assert call_count == 1  # no retry — ValueError not in exceptions list


@patch("time.sleep")
def test_exponential_backoff_delays(mock_sleep):
    call_count = 0

    @retry(max_attempts=3, base_delay=1.0, backoff_rate=2.0, jitter=False)
    def always_fails():
        nonlocal call_count
        call_count += 1
        raise RetryableError("fail")

    with pytest.raises(RetryableError):
        always_fails()

    delays = [call.args[0] for call in mock_sleep.call_args_list]
    assert len(delays) == 2  # 3 attempts → 2 sleeps
    assert delays[0] == pytest.approx(1.0)   # base_delay * 2^0
    assert delays[1] == pytest.approx(2.0)   # base_delay * 2^1
