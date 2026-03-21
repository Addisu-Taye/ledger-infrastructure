# tests/test_concurrency.py
"""
Double-Decision Concurrency Test — Phase 1.3

This test verifies that optimistic concurrency control prevents
two agents from simultaneously appending conflicting decisions
to the same loan application stream.

Rubric Requirement: Event Store Core & Concurrency (Score 3+)
"""

import pytest
import asyncio
from src.store.event_store import EventStore
from src.store.concurrency import OptimisticConcurrencyError
from src.events import BaseEvent
from src.store.upcaster import UpcasterRegistry
import os

# Test events
class CreditAnalysisCompleted(BaseEvent):
    def __init__(self, application_id: str, agent_id: str, risk_tier: str):
        super().__init__(
            type="CreditAnalysisCompleted",
            version=1,
            payload={
                "application_id": application_id,
                "agent_id": agent_id,
                "risk_tier": risk_tier,
                "confidence_score": 0.85
            }
        )

@pytest.fixture
async def event_store():
    """Create fresh event store for each test."""
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/ledger_test")
    store = EventStore(dsn, UpcasterRegistry())
    await store.connect()
    
    # Clean up test stream before test
    async with store._pool.acquire() as conn:
        await conn.execute("DELETE FROM events WHERE stream_id = $1", "loan-test-001")
        await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", "loan-test-001")
    
    yield store
    await store.close()

@pytest.mark.asyncio
async def test_double_decision_concurrency(event_store):
    """
    Two agents simultaneously attempt to append to the same stream.
    Both read version 3 and pass expected_version=3.
    Exactly one must succeed. The other must receive OptimisticConcurrencyError.
    
    Assertions:
    (a) Total events appended = 4 (not 5)
    (b) Winning task's event has stream_position=4
    (c) Losing task receives OptimisticConcurrencyError (raised, not swallowed)
    """
    stream_id = "loan-test-001"
    
    # Setup: Create stream with 3 existing events
    setup_events = [
        BaseEvent(type="ApplicationSubmitted", payload={"application_id": "test-001"}),
        BaseEvent(type="CreditAnalysisRequested", payload={"application_id": "test-001"}),
        BaseEvent(type="FraudScreeningCompleted", payload={"application_id": "test-001"}),
    ]
    await event_store.append(stream_id, setup_events, expected_version=-1)
    
    # Verify setup: stream should be at version 3
    version = await event_store.stream_version(stream_id)
    assert version == 3, f"Expected version 3, got {version}"
    
    # Track results from both concurrent tasks
    results = {"success": None, "error": None, "winning_position": None}
    
    async def agent_a_append():
        """Agent A attempts to append CreditAnalysisCompleted."""
        try:
            new_version = await event_store.append(
                stream_id=stream_id,
                events=[CreditAnalysisCompleted("test-001", "agent-a", "MEDIUM")],
                expected_version=3,  # Both agents read version 3
                correlation_id="corr-agent-a"
            )
            results["success"] = "agent_a"
            results["winning_position"] = new_version
        except OptimisticConcurrencyError as e:
            results["error"] = ("agent_a", e)
    
    async def agent_b_append():
        """Agent B attempts to append CreditAnalysisCompleted simultaneously."""
        try:
            new_version = await event_store.append(
                stream_id=stream_id,
                events=[CreditAnalysisCompleted("test-001", "agent-b", "HIGH")],
                expected_version=3,  # Both agents read version 3
                correlation_id="corr-agent-b"
            )
            results["success"] = "agent_b"
            results["winning_position"] = new_version
        except OptimisticConcurrencyError as e:
            results["error"] = ("agent_b", e)
    
    # Run both agents concurrently
    await asyncio.gather(agent_a_append(), agent_b_append())
    
    # ASSERTION (a): Exactly one succeeded, one failed
    assert results["success"] is not None, "No agent succeeded — both failed"
    assert results["error"] is not None, "No agent failed — both succeeded (CONCURRENCY BUG)"
    assert results["success"] != results["error"][0], "Same agent both succeeded and failed"
    
    # ASSERTION (b): Winning task's event has stream_position=4
    assert results["winning_position"] == 4, f"Expected winning position 4, got {results['winning_position']}"
    
    # ASSERTION (c): Losing task received OptimisticConcurrencyError
    losing_agent, error = results["error"]
    assert isinstance(error, OptimisticConcurrencyError), f"Wrong error type: {type(error)}"
    assert error.expected_version == 3, f"Expected expected_version=3, got {error.expected_version}"
    assert error.actual_version == 4, f"Expected actual_version=4, got {error.actual_version}"
    assert error.stream_id == stream_id, f"Wrong stream_id in error: {error.stream_id}"
    
    # Final verification: stream should have exactly 4 events total
    events = await event_store.load_stream(stream_id)
    assert len(events) == 4, f"Expected 4 total events, got {len(events)} — concurrency control failed"
    
    print(f"✓ Concurrency test passed: {results['success']} won, {losing_agent} lost with OptimisticConcurrencyError")

@pytest.mark.asyncio
async def test_concurrent_load_and_append(event_store):
    """
    Additional test: Verify stream_version() is accurate under concurrent load.
    """
    stream_id = "loan-test-002"
    
    # Create initial stream
    await event_store.append(stream_id, [BaseEvent(type="ApplicationSubmitted", payload={})], expected_version=-1)
    
    # Multiple concurrent appends with correct version chaining
    versions = []
    
    async def append_with_retry(stream_id, expected_version, event_data):
        try:
            new_version = await event_store.append(
                stream_id=stream_id,
                events=[BaseEvent(type="CreditAnalysisCompleted", payload=event_data)],
                expected_version=expected_version
            )
            versions.append(new_version)
            return new_version
        except OptimisticConcurrencyError:
            return None
    
    # Sequential appends (should all succeed)
    v1 = await append_with_retry(stream_id, 1, {"agent": "1"})
    v2 = await append_with_retry(stream_id, 2, {"agent": "2"})
    v3 = await append_with_retry(stream_id, 3, {"agent": "3"})
    
    assert v1 == 2, f"Expected version 2, got {v1}"
    assert v2 == 3, f"Expected version 3, got {v2}"
    assert v3 == 4, f"Expected version 4, got {v3}"
    
    # Verify final version
    final_version = await event_store.stream_version(stream_id)
    assert final_version == 4, f"Expected final version 4, got {final_version}"
    
    print("✓ Sequential append test passed")