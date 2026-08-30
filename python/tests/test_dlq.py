"""Tests for DLQ consumer and bulkhead patterns."""

import pytest
from python.patterns.dlq_consumer import (
    Bulkhead,
    DLQConsumer,
    DLQMessage,
    DLQOutcome,
)


def _msg(id="msg-1", body="data", failure_reason="timeout"):
    return DLQMessage(
        message_id=id,
        body=body,
        original_failure_reason=failure_reason,
    )


# ── DLQ Consumer ──────────────────────────────────────────────────────────────

def test_successful_retry_reprocessed():
    consumer = DLQConsumer(handler=lambda msg: None, max_attempts=3, base_delay_seconds=0)
    result = consumer.process(_msg())
    assert result.outcome == DLQOutcome.REPROCESSED
    assert result.attempts == 1


def test_transient_failure_recovers_on_retry():
    attempts = [0]

    def flaky_handler(msg):
        attempts[0] += 1
        if attempts[0] < 3:
            raise ValueError("transient error")

    consumer = DLQConsumer(handler=flaky_handler, max_attempts=3, base_delay_seconds=0)
    result = consumer.process(_msg())
    assert result.outcome == DLQOutcome.REPROCESSED
    assert result.attempts == 3


def test_all_retries_exhausted_discarded():
    def always_fails(msg):
        raise RuntimeError("poison pill")

    consumer = DLQConsumer(handler=always_fails, max_attempts=3, base_delay_seconds=0)
    result = consumer.process(_msg())
    assert result.outcome == DLQOutcome.DISCARDED
    assert result.attempts == 3
    assert "poison pill" in result.final_error


def test_custom_failure_handler_requeued():
    def always_fails(msg):
        raise RuntimeError("needs manual fix")

    def requeue_handler(msg, exc):
        return DLQOutcome.REQUEUED

    consumer = DLQConsumer(
        handler=always_fails,
        max_attempts=2,
        base_delay_seconds=0,
        failure_handler=requeue_handler,
    )
    result = consumer.process(_msg())
    assert result.outcome == DLQOutcome.REQUEUED


def test_batch_process_mixed_outcomes():
    call_counts = {}

    def selective_handler(msg):
        call_counts[msg.message_id] = call_counts.get(msg.message_id, 0) + 1
        if msg.message_id == "bad":
            raise RuntimeError("bad message")

    consumer = DLQConsumer(handler=selective_handler, max_attempts=2, base_delay_seconds=0)
    messages = [_msg("good", "ok"), _msg("bad", "fail")]
    results = consumer.process_batch(messages)

    outcomes = {r.message_id: r.outcome for r in results}
    assert outcomes["good"] == DLQOutcome.REPROCESSED
    assert outcomes["bad"] == DLQOutcome.DISCARDED


def test_invalid_max_attempts_raises():
    with pytest.raises(ValueError):
        DLQConsumer(handler=lambda m: None, max_attempts=0)


def test_summary_counts():
    consumer = DLQConsumer(handler=lambda msg: None, max_attempts=1, base_delay_seconds=0)
    consumer.process(_msg("a"))
    consumer.process(_msg("b"))
    assert "reprocessed=2" in consumer.summary()


# ── Bulkhead ──────────────────────────────────────────────────────────────────

def test_bulkhead_allows_within_limit():
    bh = Bulkhead(limits={"realtime": 2, "batch": 5})
    with bh.acquire("realtime"):
        pass  # no exception


def test_bulkhead_blocks_at_capacity():
    bh = Bulkhead(limits={"realtime": 1})
    ctx = bh.acquire("realtime")
    ctx.__enter__()
    with pytest.raises(Bulkhead.BulkheadFullError):
        bh.acquire("realtime")
    ctx.__exit__(None, None, None)


def test_bulkhead_unknown_partition_raises():
    bh = Bulkhead(limits={"batch": 5})
    with pytest.raises(KeyError):
        bh.acquire("unknown")


def test_bulkhead_releases_on_exit():
    bh = Bulkhead(limits={"batch": 1})
    with bh.acquire("batch"):
        pass
    # Should succeed again after context exits
    with bh.acquire("batch"):
        pass
