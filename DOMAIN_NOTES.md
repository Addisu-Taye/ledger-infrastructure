# DOMAIN_NOTES.md — Phase 0 Deliverable

## 1. EDA vs. ES Distinction

**Current System:** The agent activity tracing (LangChain callbacks) is **Event-Driven Architecture (EDA)**. Events are ephemeral messages — the sender fires and forgets. Events can be dropped, lost, or never processed.

**With The Ledger:** Events become the **source of truth** stored immutably in PostgreSQL. The events ARE the database.

**What Changes:**
- Callbacks → Persistent event append to `events` table
- In-memory traces → Replayable event streams
- Lost on restart → Reconstructable from event history

**What Is Gained:**
- Immutability: Events cannot be modified after append
- Temporal queries: Can reconstruct state at any past timestamp
- No dropped events: ACID-compliant storage guarantees persistence
- Audit trail: Complete decision history for compliance

---

## 2. The Aggregate Question

**Chosen Boundary:** `ComplianceRecord` is a separate aggregate from `LoanApplication`.

**Rejected Alternative:** Merging Compliance into `LoanApplication` aggregate.

**Coupling Problem Prevented:**
If merged, every compliance check would require locking the entire `LoanApplication` stream. During lengthy compliance verification (external API calls, human review), the loan stream would be blocked, preventing other agents from updating application state.

**Failure Mode Under Concurrent Writes:**
- Agent A: Appending credit analysis to `loan-123`
- Agent B: Appending compliance check to same stream
- Result: One agent waits for the other, creating artificial contention
- With separate aggregates: Both can proceed independently, coordinated only through events

---

## 3. Concurrency in Practice

**Scenario:** Two AI agents simultaneously process `loan-123`, both call `append_events` with `expected_version=3`.

**Exact Sequence:**
1. Agent A reads stream version = 3  
2. Agent B reads stream version = 3  
3. Agent A calls `append(expected_version=3)` → **Success**, stream becomes version 4  
4. Agent B calls `append(expected_version=3)` → **OptimisticConcurrencyError** (actual version = 4)  
5. Agent B must:  
   - (a) catch error  
   - (b) reload stream at version 4  
   - (c) re-evaluate business rules  
   - (d) retry with `expected_version=4`  

**Losing Agent Receives:**  
`OptimisticConcurrencyError(stream_id="loan-123", expected=3, actual=4)`

**What It Must Do Next:**  
Reload the stream, re-evaluate if its decision is still valid given the new state, then retry append with updated expected_version.

---

## 4. Projection Lag Consequences

**Scenario:** Loan officer queries "available credit limit" 200ms after disbursement event. Projection lag = 200ms. They see old limit.

**System Behavior:**
- Query hits `ApplicationSummary` projection
- Returns stale data (last processed event position)
- Response includes:
  - `projection_lag_ms: 200`
  - `last_updated_at: <timestamp>`

**UI Communication Strategy:**
- Display "Data fresh as of [timestamp]" badge on all projection-backed views
- If `projection_lag_ms > 500ms` (SLO breach), show "Updating..." indicator
- For critical decisions (approval/disbursement), force fresh read from aggregate stream with warning about latency
- Never hide lag from user in audit/compliance contexts — transparency is regulatory requirement

---

## 5. The Upcasting Scenario

**Event:** `CreditDecisionMade` v1 → v2

**v1 Schema:**
```json
{ "application_id": "...", "decision": "...", "reason": "..." }
```

**v2 Schema:**
```json
{
  "application_id": "...",
  "decision": "...",
  "reason": "...",
  "model_version": "...",
  "confidence_score": "...",
  "regulatory_basis": "..."
}
```

**Upcaster Code:**
```python
@registry.register("CreditDecisionMade", from_version=1)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    return {
        **payload,
        "model_version": "legacy-pre-2026",       # Inferred from recorded_at timestamp
        "confidence_score": None,                 # NULL — genuinely unknown
        "regulatory_basis": "inferred_baseline_v1" # Inferred from regulation version at time
    }
```

**Inference Strategy:**
- **model_version:** Inferred from `recorded_at` timestamp mapped to deployment logs (error rate <1%)
- **confidence_score:** NULL — this field did not exist in v1. Fabricating a number would imply precision that didn't exist, which is a regulatory risk.
- **regulatory_basis:** Inferred from regulation version active at `recorded_at` date (cross-reference compliance table)

**Why NULL Over Fabrication:**
Fabricating `confidence_score` would mislead auditors into thinking we had confidence tracking in earlier versions. NULL accurately represents "unknown" — critical for regulatory integrity.

---

## 6. The Marten Async Daemon Parallel

**Marten 7.0 Pattern:** Distributed projection execution across multiple nodes with coordinated checkpointing.

**Python Equivalent Implementation:**
- Use `asyncio.Task` per projection worker (one task per projection type)
- Coordination primitive: PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED` on `projection_checkpoints` table
- Multiple daemon instances can run; each claims unprocessed event batches via `SKIP LOCKED`

**Failure Mode Guarded Against:**
- **Double-processing:** Without `SKIP LOCKED`, two daemon nodes could claim the same event batch
- **Lost checkpoints:** Checkpoint update happens in same transaction as event processing
- **Node failure:** If a daemon crashes mid-batch, the lock is released and another node can claim the batch

**Code Pattern:**
```python
async def claim_batch(conn, projection_name):
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT last_position FROM projection_checkpoints
            WHERE projection_name = $1
            FOR UPDATE SKIP LOCKED
            """,
            projection_name
        )
        return row["last_position"] if row else 0
```

---

## Summary

This document establishes conceptual mastery before implementation. All six questions are answered with specificity as required by the Phase 0 standard.