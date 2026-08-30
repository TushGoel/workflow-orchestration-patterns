"""Tests for deployment slot coordinator and rollback monitor."""

import threading
import time
import pytest
from python.patterns.slot_coordinator import SlotCoordinator, SlotTimeoutError, SlotNotHeldError
from python.patterns.rollback_monitor import (
    RollbackTrigger, RollbackMonitor, RollbackStatus,
)


# ── Slot Coordinator ──────────────────────────────────────────────────────────

def test_acquire_and_release():
    coord = SlotCoordinator(max_slots=3)
    slot_id = coord.acquire("deployment-1")
    assert coord.active_count == 1
    coord.release(slot_id)
    assert coord.active_count == 0


def test_slots_enforce_max_concurrent():
    coord = SlotCoordinator(max_slots=2, timeout_seconds=0.05)
    slot_a = coord.acquire("dep-a")
    slot_b = coord.acquire("dep-b")
    with pytest.raises(SlotTimeoutError):
        coord.acquire("dep-c")  # no slots available
    coord.release(slot_a)
    coord.release(slot_b)


def test_slot_releases_on_completion():
    coord = SlotCoordinator(max_slots=1)
    slot_id = coord.acquire("dep-1")
    coord.release(slot_id)
    # Should be able to acquire again
    slot_id2 = coord.acquire("dep-2")
    coord.release(slot_id2)
    assert coord.stats.total_acquired == 2


def test_invalid_release_raises():
    coord = SlotCoordinator(max_slots=2)
    with pytest.raises(SlotNotHeldError):
        coord.release("nonexistent-slot-id")


def test_available_slots_count():
    coord = SlotCoordinator(max_slots=3)
    assert coord.available_slots == 3
    slot = coord.acquire("dep-1")
    assert coord.available_slots == 2
    coord.release(slot)
    assert coord.available_slots == 3


def test_concurrent_slot_acquisition():
    coord = SlotCoordinator(max_slots=5)
    slots = []
    errors = []

    def acquire_and_hold():
        try:
            slot = coord.acquire(f"dep-{threading.current_thread().name}")
            time.sleep(0.05)
            coord.release(slot)
            slots.append(slot)
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=acquire_and_hold) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 0
    assert len(slots) == 5
    assert coord.active_count == 0


# ── Rollback Monitor ──────────────────────────────────────────────────────────

def test_rollback_completes_successfully():
    trigger = RollbackTrigger()
    monitor = RollbackMonitor(poll_interval_seconds=0.01, timeout_seconds=5.0)

    op_id = trigger.trigger("dep-abc", reason="health check failed")

    calls = [0]
    def check_fn(oid):
        calls[0] += 1
        return RollbackStatus.COMPLETED if calls[0] >= 3 else RollbackStatus.IN_PROGRESS

    result = monitor.wait_for_completion(op_id, "dep-abc", check_fn)
    assert result.succeeded
    assert result.poll_attempts >= 3


def test_rollback_timeout():
    monitor = RollbackMonitor(poll_interval_seconds=0.01, timeout_seconds=0.05)
    result = monitor.wait_for_completion(
        "op-123", "dep-abc",
        check_fn=lambda _: RollbackStatus.IN_PROGRESS,
    )
    assert result.status == RollbackStatus.TIMED_OUT
    assert result.error is not None


def test_rollback_failure_detected():
    monitor = RollbackMonitor(poll_interval_seconds=0.01, timeout_seconds=5.0)
    result = monitor.wait_for_completion(
        "op-123", "dep-abc",
        check_fn=lambda _: RollbackStatus.FAILED,
    )
    assert result.status == RollbackStatus.FAILED
    assert not result.succeeded


def test_rollback_max_attempts():
    monitor = RollbackMonitor(poll_interval_seconds=0.01, timeout_seconds=30, max_attempts=3)
    result = monitor.wait_for_completion(
        "op-123", "dep-abc",
        check_fn=lambda _: RollbackStatus.IN_PROGRESS,
    )
    assert result.status == RollbackStatus.TIMED_OUT
    assert result.poll_attempts > 3
