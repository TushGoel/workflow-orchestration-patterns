"""
Deployment slot coordinator — limit concurrent deployments.

Problem: in high-volume deployment platforms, multiple deployments running
simultaneously cause conflicts: SPICE quota exhaustion, API rate limiting,
asset bundle conflicts, DynamoDB write contention.

The slot coordinator is a distributed semaphore: only N deployments can
hold a slot at any moment. Others wait in a queue. Unlike the bulkhead
(which fails-fast), the slot coordinator queues — deployments eventually
proceed, they just don't pile up simultaneously.

Production context: this pattern is critical in any platform where:
- The downstream service has per-minute API rate limits
- Concurrent writes cause data races on shared resources
- Resource consumption is bursty (SPICE refresh = 20-60 min per deployment)

Design:
  - N slots available (configurable per workflow type)
  - Acquire slot before starting deployment
  - Release slot on completion, success or failure
  - Queue depth visible for monitoring

Usage:
    coordinator = SlotCoordinator(max_slots=3, timeout_seconds=300)

    # Blocks until a slot is available (or timeout)
    slot_id = coordinator.acquire("deployment-d-abc123")
    try:
        run_deployment(...)
    finally:
        coordinator.release(slot_id)
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional


class SlotTimeoutError(Exception):
    """Raised when a deployment waits too long for a slot."""


class SlotNotHeldError(Exception):
    """Raised when releasing a slot that was not acquired by this caller."""


@dataclass
class SlotRecord:
    slot_id: str
    deployment_id: str
    acquired_at: float = field(default_factory=time.monotonic)

    @property
    def held_seconds(self) -> float:
        return time.monotonic() - self.acquired_at


@dataclass
class CoordinatorStats:
    total_acquired: int = 0
    total_released: int = 0
    total_timeouts: int = 0
    total_wait_time_ms: float = 0.0

    @property
    def avg_wait_ms(self) -> float:
        return self.total_wait_time_ms / self.total_acquired if self.total_acquired > 0 else 0.0


class SlotCoordinator:
    """
    Deployment slot coordinator — distributed semaphore for concurrency control.

    Thread-safe for use across concurrent deployment triggers.
    In production: replace the in-memory semaphore with a DynamoDB
    conditional-write item for cross-Lambda coordination.
    """

    def __init__(
        self,
        max_slots: int = 3,
        timeout_seconds: float = 300.0,
        poll_interval_seconds: float = 1.0,
    ) -> None:
        if max_slots < 1:
            raise ValueError("max_slots must be >= 1")
        self.max_slots = max_slots
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval_seconds
        self._semaphore = threading.Semaphore(max_slots)
        self._active: dict[str, SlotRecord] = {}
        self._lock = threading.Lock()
        self.stats = CoordinatorStats()

    def acquire(self, deployment_id: str) -> str:
        """
        Acquire a deployment slot. Blocks until a slot is available
        or timeout_seconds is exceeded.

        Returns slot_id — must be passed to release().
        Raises SlotTimeoutError if no slot becomes available in time.
        """
        start = time.monotonic()
        acquired = self._semaphore.acquire(timeout=self.timeout_seconds)

        if not acquired:
            self.stats.total_timeouts += 1
            raise SlotTimeoutError(
                f"Deployment '{deployment_id}' waited {self.timeout_seconds:.0f}s "
                f"for a slot but none became available. "
                f"Current active deployments: {self.active_count}/{self.max_slots}"
            )

        wait_ms = (time.monotonic() - start) * 1000
        slot_id = str(uuid.uuid4())[:8]

        with self._lock:
            self._active[slot_id] = SlotRecord(
                slot_id=slot_id,
                deployment_id=deployment_id,
            )
            self.stats.total_acquired += 1
            self.stats.total_wait_time_ms += wait_ms

        return slot_id

    def release(self, slot_id: str) -> None:
        """Release a deployment slot. Always call in a finally block."""
        with self._lock:
            if slot_id not in self._active:
                raise SlotNotHeldError(f"Slot '{slot_id}' is not held — cannot release.")
            del self._active[slot_id]
            self.stats.total_released += 1

        self._semaphore.release()

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    @property
    def available_slots(self) -> int:
        return max(0, self.max_slots - self.active_count)

    def active_deployments(self) -> list[SlotRecord]:
        with self._lock:
            return list(self._active.values())

    def summary(self) -> str:
        return (
            f"SlotCoordinator | slots={self.active_count}/{self.max_slots} "
            f"available={self.available_slots} | "
            f"acquired={self.stats.total_acquired} "
            f"avg_wait={self.stats.avg_wait_ms:.0f}ms "
            f"timeouts={self.stats.total_timeouts}"
        )
