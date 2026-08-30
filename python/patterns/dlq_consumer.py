"""
DLQ (Dead Letter Queue) consumer pattern.

Messages land in a DLQ when they fail processing N times in the main queue.
A DLQ consumer runs separately, applies retry logic with backoff, and routes
messages to one of three outcomes:

  REPROCESSED  — succeeded on retry (bug was transient)
  REQUEUED     — needs manual fix, put back in main queue after correction
  DISCARDED    — unrecoverable (poison pill), log and drop

This is the pattern that prevents DLQ depth from growing indefinitely
while maintaining an audit trail of every failure.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class DLQOutcome(str, Enum):
    REPROCESSED = "reprocessed"   # retry succeeded
    REQUEUED = "requeued"         # returned to main queue for manual fix
    DISCARDED = "discarded"       # unrecoverable — logged and dropped
    RETRY_EXHAUSTED = "retry_exhausted"  # retries exceeded max_attempts


@dataclass
class DLQMessage:
    message_id: str
    body: Any
    original_failure_reason: str
    receive_count: int = 1       # how many times it failed in the main queue
    metadata: dict = field(default_factory=dict)


@dataclass
class DLQResult:
    message_id: str
    outcome: DLQOutcome
    attempts: int
    final_error: Optional[str] = None
    processing_time_ms: float = 0.0

    def __str__(self) -> str:
        return (
            f"[{self.outcome.value.upper()}] msg={self.message_id} "
            f"attempts={self.attempts} time={self.processing_time_ms:.0f}ms"
            + (f" error={self.final_error}" if self.final_error else "")
        )


class DLQConsumer:
    """
    Processes messages from a dead letter queue with configurable retry logic.

    The consumer applies a handler function to each message. If the handler
    succeeds, the message is REPROCESSED. If it fails after max_attempts,
    the message routes to the failure handler (default: DISCARDED).

    Usage:
        consumer = DLQConsumer(
            handler=process_deployment_event,
            max_attempts=3,
            base_delay_seconds=1.0,
        )

        for message in dlq.receive_messages():
            result = consumer.process(message)
            if result.outcome == DLQOutcome.REPROCESSED:
                dlq.delete(message.message_id)
            elif result.outcome == DLQOutcome.DISCARDED:
                alert_oncall(result)
    """

    def __init__(
        self,
        handler: Callable[[DLQMessage], Any],
        max_attempts: int = 3,
        base_delay_seconds: float = 1.0,
        backoff_multiplier: float = 2.0,
        failure_handler: Optional[Callable[[DLQMessage, Exception], DLQOutcome]] = None,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self.handler = handler
        self.max_attempts = max_attempts
        self.base_delay_seconds = base_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.failure_handler = failure_handler or self._default_failure_handler
        self._results: list[DLQResult] = []

    def process(self, message: DLQMessage) -> DLQResult:
        """
        Process a single DLQ message with exponential backoff retry.

        Returns a DLQResult describing the outcome.
        """
        start = time.monotonic()
        last_error: Optional[Exception] = None

        for attempt in range(1, self.max_attempts + 1):
            try:
                self.handler(message)
                elapsed = (time.monotonic() - start) * 1000
                result = DLQResult(
                    message_id=message.message_id,
                    outcome=DLQOutcome.REPROCESSED,
                    attempts=attempt,
                    processing_time_ms=elapsed,
                )
                self._results.append(result)
                return result

            except Exception as exc:
                last_error = exc
                if attempt < self.max_attempts:
                    delay = self.base_delay_seconds * (self.backoff_multiplier ** (attempt - 1))
                    time.sleep(delay)

        # All attempts exhausted — route to failure handler
        elapsed = (time.monotonic() - start) * 1000
        outcome = self.failure_handler(message, last_error)
        result = DLQResult(
            message_id=message.message_id,
            outcome=outcome,
            attempts=self.max_attempts,
            final_error=str(last_error),
            processing_time_ms=elapsed,
        )
        self._results.append(result)
        return result

    def process_batch(self, messages: list[DLQMessage]) -> list[DLQResult]:
        """Process a batch of DLQ messages sequentially."""
        return [self.process(msg) for msg in messages]

    def summary(self) -> str:
        counts = {outcome: 0 for outcome in DLQOutcome}
        for r in self._results:
            counts[r.outcome] += 1
        total = len(self._results)
        return (
            f"DLQConsumer | total={total} "
            f"reprocessed={counts[DLQOutcome.REPROCESSED]} "
            f"discarded={counts[DLQOutcome.DISCARDED]} "
            f"retry_exhausted={counts[DLQOutcome.RETRY_EXHAUSTED]}"
        )

    @staticmethod
    def _default_failure_handler(message: DLQMessage, exc: Exception) -> DLQOutcome:
        """Default: discard after all retries exhausted."""
        return DLQOutcome.DISCARDED


class Bulkhead:
    """
    Bulkhead pattern — isolate concurrent load per workflow type.

    In production, a surge in one workflow type (e.g. batch jobs) should not
    exhaust the thread/connection pool for other workflows (e.g. real-time alerts).
    Bulkheads partition capacity so one type can't starve another.

    This implementation is a counting semaphore: each workflow type gets a
    fixed concurrency limit. Attempts to exceed it raise BulkheadFullError
    rather than blocking — callers can decide to queue, shed load, or fail fast.

    Usage:
        bulkhead = Bulkhead(limits={"batch": 5, "realtime": 20, "dlq": 2})

        with bulkhead.acquire("realtime"):
            process_alert(alert)  # at most 20 concurrent
    """

    class BulkheadFullError(Exception):
        pass

    def __init__(self, limits: dict[str, int]) -> None:
        import threading
        self._limits = limits
        self._semaphores = {k: threading.Semaphore(v) for k, v in limits.items()}

    def acquire(self, partition: str) -> "BulkheadContext":
        if partition not in self._semaphores:
            raise KeyError(f"Unknown partition {partition!r}. Known: {list(self._limits)}")
        sem = self._semaphores[partition]
        if not sem.acquire(blocking=False):
            raise self.BulkheadFullError(
                f"Bulkhead '{partition}' is at capacity ({self._limits[partition]} concurrent). "
                "Shed load or queue the request."
            )
        return BulkheadContext(sem)

    def available(self, partition: str) -> int:
        """Approximate available slots (for monitoring — not exact under concurrency)."""
        return self._semaphores[partition]._value


class BulkheadContext:
    def __init__(self, sem) -> None:
        self._sem = sem

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._sem.release()
