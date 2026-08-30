# workflow-orchestration-patterns

![CI](https://github.com/TushGoel/workflow-orchestration-patterns/actions/workflows/ci.yml/badge.svg)
![Scale](https://img.shields.io/badge/scale-500K%2B%20customers-blue)
![TypeScript](https://img.shields.io/badge/TypeScript-CDK-blue)
![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Tests](https://img.shields.io/badge/tests-32%20passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-green)

Production patterns for durable workflow orchestration — TypeScript CDK infrastructure and Python resilience primitives.

Built around the principle: **distributed systems fail. Design for failure from day one.**

These patterns run in production on a CI/CD and migration platform serving **500,000+ end customers** and processing **millions of dashboard assets** in a large-scale BI migration. At this scale, a single unhandled failure in an orchestration workflow cascades into customer-facing incidents.

---

## What's In Here

| Layer | Language | What It Demonstrates |
|-------|----------|---------------------|
| `cdk/` | **TypeScript** | AWS CDK stack: Step Functions + SQS + Lambda + DynamoDB + CloudWatch |
| `python/patterns/retry.py` | Python | Exponential backoff with jitter — prevents thundering herd |
| `python/patterns/circuit_breaker.py` | Python | Circuit breaker — prevents cascading failures |

---

## The Problem → Solution → Impact

| | |
|---|---|
| **Problem** | Distributed workflows fail in unpredictable ways: transient errors, downstream outages, partial failures spanning long-running operations (20-60+ min). Ad-hoc retry logic is inconsistent; missing circuit breakers cause cascading failures; Lambda chains lose state on timeout for operations exceeding 15 minutes. |
| **Solution** | Durable workflow execution via Step Functions (survives Lambda restarts, exactly-once semantics, built-in retry + DLQ), combined with battle-tested resilience primitives for service calls. |
| **Impact** | Production platform: **19 CDK stacks, 7-stage Step Functions pipeline, 12 Lambda handlers** processing **1,000+ weekly deployments** with zero state loss. ~30 state transitions per deployment, $0.75/day Step Functions cost. **99.8% platform availability**. Transient failures auto-recovered; downstream outages circuit-break instead of cascade. **Zero recurrence in 12+ months** after retry policy fix that eliminated a 16-service cascade. |

---

## System Design

```mermaid
graph TD
    A[📬 SQS Queue<br/>decoupling · DLQ · dedup] --> B

    subgraph Step Functions State Machine — Durable Execution
        B[Validate] -->|retry 3×, backoff 2s→4s→8s| C[Deploy]
        C -->|retry 2×| D[Verify Health]
        D --> E{Health Check}
        E -->|passed| F[✅ Success]
        E -->|failed| G[Rollback]
        G --> H[❌ Failed]
    end

    subgraph Python Resilience Layer
        I[retry decorator<br/>exponential backoff + jitter]
        J[circuit breaker<br/>CLOSED → OPEN → HALF_OPEN]
    end

    subgraph CloudWatch SLOs
        K[DLQ messages alarm<br/>threshold: 1]
        L[Workflow failure alarm<br/>threshold: 5% over 15min]
    end
```

---

## CDK Stack (TypeScript)

The `DeploymentPipelineStack` provisions a complete deployment pipeline with a single CDK command:

```typescript
// Deploy to all three environments
new DeploymentPipelineStack(app, 'DeploymentPipeline-prod', {
  environment: 'prod',
  env: { account: '123456789', region: 'us-east-1' },
});
```

**What it creates:**

| Resource | Configuration |
|----------|--------------|
| SQS Queue | Dead-letter queue after 3 failures, 14-day retention |
| Step Functions | Durable state machine, CloudWatch tracing, ERROR-level logs |
| DynamoDB | PAY_PER_REQUEST, PITR enabled in prod, TTL for auto-cleanup |
| Lambda (×4) | Validate → Deploy → Verify → Rollback, least-privilege IAM |
| CloudWatch Alarms | DLQ message alarm + workflow failure rate SLO |

**Deploy:**

```bash
cd cdk
npm install
npx cdk deploy DeploymentPipeline-dev   # dev first
npx cdk deploy DeploymentPipeline-prod  # promote after validation
```

---

## Python: Retry with Exponential Backoff

```python
from python.patterns.retry import retry, RetryableError

@retry(
    max_attempts=3,
    base_delay=1.0,    # seconds
    backoff_rate=2.0,  # delay doubles each attempt: 1s → 2s → 4s
    max_delay=30.0,    # cap at 30s regardless of backoff
    jitter=True,       # ±50% jitter prevents thundering herd
    exceptions=(RetryableError, ConnectionError),
)
def call_downstream_service(request_id: str) -> dict:
    # transient failures retry automatically
    ...
```

**Why jitter matters:** Without jitter, all retrying clients wake up at exactly the same moment and overwhelm a recovering service. Jitter spreads the load.

---

## Python: Circuit Breaker

```python
from python.patterns.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError

breaker = CircuitBreaker(
    failure_threshold=5,      # open after 5 consecutive failures
    recovery_timeout=30.0,    # probe recovery after 30s
    success_threshold=2,      # 2 successes in HALF_OPEN to close
    name="payment-service",
)

try:
    result = breaker.call(payment_service.charge, amount=100)
except CircuitBreakerOpenError:
    # Fast-fail — don't wait for timeout, return fallback immediately
    result = use_fallback()
```

**State transitions:**

```
         5 failures                   30s timeout
CLOSED ──────────────► OPEN ─────────────────────► HALF_OPEN
  ▲                                                     │
  │ 2 successes                     probe fails         │
  └─────────────────────────────────────────────────────┘
                                         │
                                    back to OPEN
```

---

## Running Tests

```bash
# Python tests
pip install pytest
pytest python/tests/ -v

# TypeScript typecheck (no CDK deploy needed)
cd cdk && npm install && npx tsc --noEmit
```

---

## Part of the Agentic Infrastructure Stack

| Repo | What It Is |
|------|-----------|
| **[agentic-ops](https://github.com/TushGoel/agentic-ops)** | Full system design — Step Functions orchestration in production context |
| **[production-mcp-server](https://github.com/TushGoel/production-mcp-server)** | MCP governance layer (Python) |
| **[agent-eval-framework](https://github.com/TushGoel/agent-eval-framework)** | Agent quality measurement (Python) |
| **[iam-policy-scanner](https://github.com/TushGoel/iam-policy-scanner)** | IAM compliance scanning (Go) |
| **[workflow-orchestration-patterns](https://github.com/TushGoel/workflow-orchestration-patterns)** | ← You are here: CDK (TypeScript) + resilience patterns |

---

## License

MIT
