"""
Rollback with polling monitor — automated rollback with completion tracking.

Pattern from production CI/CD: when a deployment fails, rollback is triggered
but rollback itself is a long-running async operation (10-30 minutes for large
datasets). A naive implementation fires rollback and returns — but the caller
has no way to know when rollback completed, or if it failed.

This pattern separates rollback into two phases:
  1. Trigger: initiate the rollback operation (fast, synchronous)
  2. Monitor: poll until rollback completes, times out, or fails (async)

Design mirrors the Step Functions Wait+Choice polling loop pattern used in
production for SPICE refresh operations that exceed Lambda's 15-minute limit.

Usage:
    trigger = RollbackTrigger()
    monitor = RollbackMonitor(poll_interval_seconds=2.0, timeout_seconds=300.0)

    # Phase 1: trigger rollback (fast)
    operation_id = trigger.trigger(deployment_id="d-abc123", reason="health check failed")

    # Phase 2: poll until complete
    result = monitor.wait_for_completion(
        operation_id=operation_id,
        check_fn=lambda op_id: get_rollback_status(op_id),
    )
    print(result)
    # RollbackResult(status=COMPLETED, duration_ms=28400, attempts=15)
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


class RollbackStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"    # rollback succeeded
    FAILED = "failed"          # rollback itself failed
    TIMED_OUT = "timed_out"    # monitor gave up waiting


@dataclass
class RollbackOperation:
    operation_id: str
    deployment_id: str
    reason: str
    triggered_at: float = field(default_factory=time.monotonic)
    status: RollbackStatus = RollbackStatus.PENDING

    @property
    def age_seconds(self) -> float:
        return time.monotonic() - self.triggered_at


@dataclass
class RollbackResult:
    operation_id: str
    deployment_id: str
    status: RollbackStatus
    duration_ms: float
    poll_attempts: int
    error: Optional[str] = None

    @property
    def succeeded(self) -> bool:
        return self.status == RollbackStatus.COMPLETED

    def __str__(self) -> str:
        return (
            f"Rollback[{self.deployment_id}] "
            f"status={self.status.value.upper()} | "
            f"duration={self.duration_ms:.0f}ms | "
            f"polls={self.poll_attempts}"
            + (f" | error={self.error}" if self.error else "")
        )


class RollbackTrigger:
    """
    Initiates a rollback operation. In production, this calls the
    rollback Lambda / Step Functions execution.
    """

    def __init__(self) -> None:
        self._operations: dict[str, RollbackOperation] = {}

    def trigger(self, deployment_id: str, reason: str) -> str:
        """
        Trigger a rollback. Returns operation_id for monitoring.
        Fast — does not wait for completion.
        """
        op_id = str(uuid.uuid4())[:12]
        op = RollbackOperation(
            operation_id=op_id,
            deployment_id=deployment_id,
            reason=reason,
            status=RollbackStatus.IN_PROGRESS,
        )
        self._operations[op_id] = op
        return op_id

    def get_operation(self, operation_id: str) -> Optional[RollbackOperation]:
        return self._operations.get(operation_id)


class RollbackMonitor:
    """
    Polls a rollback operation until it completes, fails, or times out.

    The check_fn callable is called periodically with the operation_id.
    It should return:
      - RollbackStatus.COMPLETED if rollback is done
      - RollbackStatus.IN_PROGRESS if still running
      - RollbackStatus.FAILED if rollback itself failed

    This mirrors the Step Functions Wait+Choice polling loop: instead of
    a blocking thread, in production this runs as a periodic Lambda triggered
    by EventBridge, checking status in DynamoDB.
    """

    def __init__(
        self,
        poll_interval_seconds: float = 5.0,
        timeout_seconds: float = 300.0,
        max_attempts: Optional[int] = None,
    ) -> None:
        self.poll_interval = poll_interval_seconds
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts

    def wait_for_completion(
        self,
        operation_id: str,
        deployment_id: str,
        check_fn: Callable[[str], RollbackStatus],
    ) -> RollbackResult:
        """
        Poll check_fn until completion, timeout, or max_attempts.

        Args:
            operation_id: returned by RollbackTrigger.trigger()
            deployment_id: for logging/result
            check_fn: callable(operation_id) → RollbackStatus
        """
        start = time.monotonic()
        attempts = 0

        while True:
            attempts += 1
            elapsed = time.monotonic() - start

            # Check timeout
            if elapsed >= self.timeout_seconds:
                return RollbackResult(
                    operation_id=operation_id,
                    deployment_id=deployment_id,
                    status=RollbackStatus.TIMED_OUT,
                    duration_ms=elapsed * 1000,
                    poll_attempts=attempts,
                    error=f"Rollback timed out after {self.timeout_seconds:.0f}s ({attempts} polls)",
                )

            # Check max attempts
            if self.max_attempts and attempts > self.max_attempts:
                return RollbackResult(
                    operation_id=operation_id,
                    deployment_id=deployment_id,
                    status=RollbackStatus.TIMED_OUT,
                    duration_ms=elapsed * 1000,
                    poll_attempts=attempts,
                    error=f"Rollback exceeded {self.max_attempts} poll attempts",
                )

            # Poll current status
            status = check_fn(operation_id)

            if status == RollbackStatus.COMPLETED:
                return RollbackResult(
                    operation_id=operation_id,
                    deployment_id=deployment_id,
                    status=RollbackStatus.COMPLETED,
                    duration_ms=(time.monotonic() - start) * 1000,
                    poll_attempts=attempts,
                )

            if status == RollbackStatus.FAILED:
                return RollbackResult(
                    operation_id=operation_id,
                    deployment_id=deployment_id,
                    status=RollbackStatus.FAILED,
                    duration_ms=(time.monotonic() - start) * 1000,
                    poll_attempts=attempts,
                    error="Rollback operation reported failure",
                )

            time.sleep(self.poll_interval)
