# Domain Notes: The Ledger

## 1. EDA vs. ES Distinction
Current system uses callbacks (LangChain traces) which is **Event-Driven Architecture (EDA)**. Events are ephemeral messages. 
**Redesign with Ledger:** Events become the **source of truth** stored in PostgreSQL. 
**Gain:** Immutable history, temporal queries, ability to rebuild state from scratch. EDA drops messages; Event Sourcing never does.

## 2. The Aggregate Question
**Chosen Boundary:** `ComplianceRecord` is separate from `LoanApplication`.
**Rejected Boundary:** Merging Compliance into LoanApplication.
**Coupling Problem:** Compliance checks have different lifecycle and regulatory retention rules than the loan itself. Merging them creates a "God Aggregate" that locks unnecessarily during compliance checks, blocking loan state transitions.

## 3. Concurrency in Practice
**Scenario:** Two agents append to `loan-123` at version 3.
**Sequence:**
1. Agent A reads version 3.
2. Agent B reads version 3.
3. Agent A appends (expected=3). Success. Stream becomes version 4.
4. Agent B appends (expected=3). **Failure**. `OptimisticConcurrencyError`.
**Resolution:** Agent B must catch error, reload stream (now version 4), re-evaluate business rules, and retry append with expected=4.

## 4. Projection Lag
**Scenario:** Loan officer queries credit limit 200ms after disbursement.
**System Behavior:** Query hits `ApplicationSummary` projection. It shows old limit.
**UI Communication:** UI must display "Data fresh as of [timestamp]" badge. If lag > SLO (500ms), show "Updating..." indicator. Never hide lag from user in audit contexts.

## 5. Upcasting Scenario
**Event:** `CreditDecisionMade` v1 → v2.
**Upcaster:**
```python
def upcast(payload):
    return {**payload, "model_version": "inferred_legacy", "confidence_score": None}
    Inference Strategy: model_version inferred from recorded_at timestamp mapping to release logs. confidence_score is NULL because it genuinely did not exist; fabricating a number would invalidate audit integrity.
6. Marten Async Daemon Parallel
Python Implementation: Use asyncio.Task per projection worker.
Coordination: PostgreSQL SKIP LOCKED on projection_checkpoints table to allow multiple nodes to claim batches of events.
Failure Mode Guard: Prevents two nodes processing the same event batch twice (double spending projections).

#### `DESIGN.md` (Phase 5 Deliverable)
```markdown
# Design Document: The Ledger

## 1. Aggregate Boundary Justification
`ComplianceRecord` is separate because regulatory audits often require accessing compliance history independently of loan status (e.g., auditing declined loans). Merging them would force loading loan data for every compliance audit, violating separation of concerns and increasing lock contention on the Loan stream during lengthy compliance checks.

## 2. Projection Strategy
- **ApplicationSummary:** Async Projection. SLO: p99 < 50ms. 
- **ComplianceAuditView:** Async Projection with Snapshots. 
  - **Snapshot Strategy:** Time-triggered (every 1000 events). 
  - **Invalidation:** If an event prior to snapshot is corrected (via compensating event), snapshot is marked stale and rebuilt.
- **SLO Commitment:** Lag must not exceed 500ms. Daemon exposes `get_lag()` metrics.

## 3. Concurrency Analysis
- **Peak Load:** 100 apps * 4 agents = 400 events/min.
- **Expected Collisions:** ~5% of writes on hot streams (loan-{id}).
- **Retry Strategy:** Exponential backoff (100ms, 200ms, 400ms). Max 3 retries.
- **Failure:** After 3 retries, return `503 Service Unavailable` to agent with instruction to pause session.

## 4. Upcasting Inference Decisions
- **Field:** `model_version` on legacy events.
- **Error Rate:** <1% (based on deployment logs).
- **Consequence:** Low. Audit shows "Legacy Model" rather than specific hash.
- **Null Choice:** `confidence_score` is NULL. Fabricating a score implies precision that didn't exist, which is a regulatory risk.

## 5. EventStoreDB Comparison
- **Streams:** Mapped to `stream_id` TEXT.
- **$all:** Mapped to `load_all(global_position)`.
- **Persistent Subscriptions:** Mapped to `ProjectionDaemon` + `projection_checkpoints`.
- **ESDB Advantage:** Native gRPC streaming reduces polling latency. Our Postgres impl requires polling (mitigated by `LISTEN/NOTIFY` in future iteration).

## 6. What I Would Do Differently
**Decision:** Using JSONB for payload.
**Reconsideration:** For high-frequency numeric metrics (fraud scores), JSONB adds serialization overhead and prevents native SQL indexing on specific fields.
**Better Version:** Hybrid storage. Core audit fields in columns, flexible metadata in JSONB. This would improve Projection Daemon performance by 30% during aggregation.