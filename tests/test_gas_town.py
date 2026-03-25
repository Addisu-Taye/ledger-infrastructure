"""
Gas Town Crash Recovery Test — Phase 4C
"""
import pytest
import asyncio
from src.store.event_store import EventStore
from src.store.upcaster import UpcasterRegistry
from src.integrity.gas_town import reconstruct_agent_context, AgentContext
from src.events import BaseEvent
import os

@pytest.fixture
async def event_store():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:Pass%4012345@localhost:5433/ledger_infrastructure")
    store = EventStore(dsn, UpcasterRegistry())
    await store.connect()
    
    # Aggressive cleanup: use TRUNCATE with CASCADE for test streams
    async with store._pool.acquire() as conn:
        # Clean up all agent-gas-town-test streams
        await conn.execute("""
            DELETE FROM outbox 
            WHERE event_id IN (
                SELECT event_id FROM events 
                WHERE stream_id LIKE 'agent-agent-test-%'
            )
        """)
        await conn.execute("DELETE FROM events WHERE stream_id LIKE 'agent-agent-test-%'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE 'agent-agent-test-%'")
    
    yield store
    await store.close()

@pytest.mark.asyncio
async def test_reconstruct_context_from_empty_stream(event_store):
    """Test reconstruction when no events exist (fresh session)."""
    agent_id = "agent-test-001"
    session_id = "session-fresh-001"
    
    context = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=8000
    )
    
    assert context.agent_id == agent_id
    assert context.session_id == session_id
    assert context.context_text == ""
    assert context.total_events == 0
    assert context.session_health_status == "FAILED"
    assert context.needs_reconciliation == False
    
    print("✓ Empty stream reconstruction test passed")

@pytest.mark.asyncio
async def test_reconstruct_context_with_events(event_store):
    """Test reconstruction with multiple events in session stream."""
    agent_id = "agent-test-002"
    session_id = "session-events-002"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # First, ensure stream is clean by checking version
    try:
        version = await event_store.stream_version(stream_id)
        if version > 0:
            # Stream exists with events - clean it
            async with event_store._pool.acquire() as conn:
                await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = $1)", stream_id)
                await conn.execute("DELETE FROM events WHERE stream_id = $1", stream_id)
                await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", stream_id)
    except:
        pass  # Stream doesn't exist yet, that's fine
    
    # Simulate agent session with 5 events
    events_to_append = [
        BaseEvent(type="AgentContextLoaded", version=1, payload={
            "agent_id": agent_id, "session_id": session_id,
            "model_version": "v2026.1", "context_source": "event_replay",
            "context_token_count": 1000, "loaded_at": "2026-03-26T00:00:00Z"
        }),
        BaseEvent(type="AgentNodeExecuted", version=1, payload={
            "node_name": "validate_inputs", "node_sequence": 1,
            "duration_ms": 50, "executed_at": "2026-03-26T00:01:00Z"
        }),
        BaseEvent(type="AgentNodeExecuted", version=1, payload={
            "node_name": "analyze_credit_risk", "node_sequence": 2,
            "duration_ms": 1500, "llm_called": True,
            "executed_at": "2026-03-26T00:02:00Z"
        }),
        BaseEvent(type="AgentToolCalled", version=1, payload={
            "tool_name": "query_applicant_registry",
            "tool_duration_ms": 200, "called_at": "2026-03-26T00:03:00Z"
        }),
        BaseEvent(type="AgentOutputWritten", version=1, payload={
            "events_written": [{"stream_id": f"loan-001", "event_type": "CreditAnalysisCompleted"}],
            "output_summary": "Credit analysis complete",
            "written_at": "2026-03-26T00:04:00Z"
        }),
    ]
    
    # Append all events with correct expected_version
    expected_version = -1  # First event: new stream
    for event in events_to_append:
        expected_version = await event_store.append(stream_id, [event], expected_version=expected_version)
    
    # Now reconstruct context (simulating crash recovery)
    context = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=8000
    )
    
    # Verify reconstructed context
    assert context.agent_id == agent_id
    assert context.session_id == session_id
    assert context.total_events == 5, f"Expected 5 events, got {context.total_events}"
    assert context.last_event_position >= 4
    assert context.last_completed_action == "AgentOutputWritten"
    assert context.session_health_status == "HEALTHY"
    assert context.needs_reconciliation == False
    assert context.model_version == "v2026.1"
    assert context.token_count > 0
    assert len(context.context_text) > 0
    assert "HISTORICAL SUMMARY" in context.context_text or "RECENT EVENTS" in context.context_text
    
    print(f"✓ Context reconstruction test passed: {context.total_events} events, {context.token_count} tokens")

@pytest.mark.asyncio
async def test_reconstruct_context_needs_reconciliation(event_store):
    """Test that partial decisions are flagged for reconciliation."""
    agent_id = "agent-test-003"
    session_id = "session-partial-003"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Clean stream first
    try:
        version = await event_store.stream_version(stream_id)
        if version > 0:
            async with event_store._pool.acquire() as conn:
                await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = $1)", stream_id)
                await conn.execute("DELETE FROM events WHERE stream_id = $1", stream_id)
                await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", stream_id)
    except:
        pass
    
    # Simulate agent that crashed mid-decision (partial state)
    events_to_append = [
        BaseEvent(type="AgentContextLoaded", version=1, payload={
            "agent_id": agent_id, "session_id": session_id,
            "model_version": "v2026.1", "context_source": "event_replay",
            "context_token_count": 1000, "loaded_at": "2026-03-26T00:00:00Z"
        }),
        BaseEvent(type="CreditAnalysisCompleted", version=1, payload={
            "application_id": "loan-partial-001",
            "risk_tier": "MEDIUM", "confidence_score": 0.75,
            "completed_at": "2026-03-26T00:01:00Z"
        }),
    ]
    
    # Append with correct expected_version
    expected_version = -1
    for event in events_to_append:
        expected_version = await event_store.append(stream_id, [event], expected_version=expected_version)
    
    # Reconstruct context
    context = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=8000
    )
    
    # Verify partial state is detected
    assert context.needs_reconciliation == True
    assert context.session_health_status == "NEEDS_RECONCILIATION"
    assert len(context.pending_work) > 0
    assert context.pending_work[0]["action"] == "complete_or_rollback"
    
    print(f"✓ Reconciliation detection test passed: {len(context.pending_work)} pending items")

@pytest.mark.asyncio
async def test_reconstruct_context_gas_town_invariant(event_store):
    """Test that ContextLoaded must be first event (Gas Town invariant)."""
    agent_id = "agent-test-004"
    session_id = "session-invalid-004"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Clean stream first
    try:
        version = await event_store.stream_version(stream_id)
        if version > 0:
            async with event_store._pool.acquire() as conn:
                await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = $1)", stream_id)
                await conn.execute("DELETE FROM events WHERE stream_id = $1", stream_id)
                await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", stream_id)
    except:
        pass
    
    # Violate Gas Town invariant: first event is NOT ContextLoaded
    invalid_events = [
        BaseEvent(type="AgentNodeExecuted", version=1, payload={
            "node_name": "validate_inputs", "node_sequence": 1,
            "duration_ms": 50, "executed_at": "2026-03-26T00:00:00Z"
        }),
    ]
    
    for event in invalid_events:
        await event_store.append(stream_id, [event], expected_version=-1)
    
    # Should raise DomainError
    from src.integrity.gas_town import DomainError
    with pytest.raises(DomainError) as exc_info:
        await reconstruct_agent_context(event_store, agent_id, session_id)
    
    assert "AgentContextLoaded" in str(exc_info.value)
    print("✓ Gas Town invariant test passed")

@pytest.mark.asyncio
async def test_reconstruct_context_token_budget(event_store):
    """Test that token budget is respected in context summarization."""
    agent_id = "agent-test-005"
    session_id = "session-budget-005"
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Clean stream first
    try:
        version = await event_store.stream_version(stream_id)
        if version > 0:
            async with event_store._pool.acquire() as conn:
                await conn.execute("DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = $1)", stream_id)
                await conn.execute("DELETE FROM events WHERE stream_id = $1", stream_id)
                await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", stream_id)
    except:
        pass
    
    # Create many events to test token budgeting
    events_to_append = [
        BaseEvent(type="AgentContextLoaded", version=1, payload={
            "agent_id": agent_id, "session_id": session_id,
            "model_version": "v2026.1", "context_source": "event_replay",
            "context_token_count": 1000, "loaded_at": "2026-03-26T00:00:00Z"
        }),
    ]
    
    # Add 20 more events
    for i in range(20):
        events_to_append.append(BaseEvent(type="AgentNodeExecuted", version=1, payload={
            "node_name": f"node_{i}", "node_sequence": i + 1,
            "duration_ms": 50 + i * 10, "executed_at": f"2026-03-26T00:{i:02d}:00Z"
        }))
    
    # Append with correct expected_version tracking
    expected_version = -1
    for event in events_to_append:
        expected_version = await event_store.append(stream_id, [event], expected_version=expected_version)
    
    # Reconstruct with tight token budget
    context = await reconstruct_agent_context(
        event_store, agent_id, session_id, token_budget=2000  # Tight budget
    )
    
    assert context.token_count <= 2000
    assert context.total_events == 21, f"Expected 21 events, got {context.total_events}"
    assert len(context.context_text) > 0
    
    print(f"✓ Token budget test passed: {context.token_count} tokens (budget: 2000)")
