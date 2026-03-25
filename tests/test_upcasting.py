import pytest
import asyncio
import json
from src.store.event_store import EventStore
from src.store.upcaster import UpcasterRegistry, registry
from src.events import BaseEvent, StoredEvent
import os

@pytest.fixture
async def event_store():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:Pass%4012345@localhost:5433/ledger_infrastructure")
    store = EventStore(dsn, registry)
    await store.connect()
    
    # Clean up test streams before each test
    async with store._pool.acquire() as conn:
        test_streams = ["upcast-stream-001", "immutability-stream-001", "inference-test-001"]
        for stream_id in test_streams:
            # Delete outbox entries first (FK dependency)
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = $1)",
                stream_id
            )
            await conn.execute("DELETE FROM events WHERE stream_id = $1", stream_id)
            await conn.execute("DELETE FROM event_streams WHERE stream_id = $1", stream_id)
    
    yield store
    await store.close()

@pytest.mark.asyncio
async def test_upcaster_registry_chain(event_store):
    """Test that upcasters are applied in correct version order."""
    stream_id = "upcast-stream-001"
    
    # Create a v1 event and append it
    v1_event = BaseEvent(
        type="CreditAnalysisCompleted",
        version=1,
        payload={
            "application_id": "upcast-test-001",
            "agent_id": "agent-test",
            "risk_tier": "MEDIUM"
        }
    )
    
    await event_store.append(
        stream_id=stream_id,
        events=[v1_event],
        expected_version=-1
    )
    
    # Load the event — should be upcasted to v2 automatically
    events = await event_store.load_stream(stream_id)
    assert len(events) == 1, f"Expected 1 event, got {len(events)}"
    
    upcasted = events[0]
    assert upcasted.event_version == 2, f"Expected v2, got v{upcasted.event_version}"
    assert upcasted.payload["model_version"] == "legacy-pre-2026"
    assert upcasted.payload["confidence_score"] is None  # NULL is correct
    assert upcasted.payload["regulatory_basis"] == "inferred_baseline_v1"
    
    print("✓ Upcaster chain test passed")

@pytest.mark.asyncio
async def test_immutability_guarantee(event_store):
    """
    CRITICAL TEST: Verify that upcasting does NOT modify stored payloads.
    Raw database data must remain unchanged — upcasting happens at read-time only.
    """
    stream_id = "immutability-stream-001"
    
    # Append a v1 event
    v1_payload = {
        "application_id": "immutability-test-001",
        "agent_id": "agent-immutability",
        "risk_tier": "HIGH"
    }
    
    await event_store.append(
        stream_id=stream_id,
        events=[BaseEvent(type="CreditAnalysisCompleted", version=1, payload=v1_payload)],
        expected_version=-1
    )
    
    # 1. Directly query the database for RAW stored payload
    async with event_store._pool.acquire() as conn:
        raw_row = await conn.fetchrow(
            "SELECT payload, event_version FROM events WHERE stream_id = $1 ORDER BY stream_position DESC LIMIT 1",
            stream_id
        )
        raw_payload = raw_row["payload"]
        raw_version = raw_row["event_version"]
        
        # Verify it's still v1 in the database
        assert raw_version == 1, f"Database should store v1, got v{raw_version}"
        
        # Parse raw payload (may be string or dict depending on asyncpg)
        if isinstance(raw_payload, str):
            raw_payload = json.loads(raw_payload)
        
        # Verify v1 fields only — no upcasted fields in storage
        assert "risk_tier" in raw_payload
        assert "model_version" not in raw_payload, "Database should NOT contain upcasted fields"
        assert "confidence_score" not in raw_payload
    
    # 2. Load through EventStore — should return upcasted v2
    events = await event_store.load_stream(stream_id)
    upcasted_event = events[0]
    
    # Verify upcasted version has new fields
    assert upcasted_event.event_version == 2
    assert "model_version" in upcasted_event.payload
    assert upcasted_event.payload["confidence_score"] is None
    
    # 3. Re-query database to confirm it STILL hasn't changed
    async with event_store._pool.acquire() as conn:
        raw_row_after = await conn.fetchrow(
            "SELECT payload FROM events WHERE stream_id = $1 ORDER BY stream_position DESC LIMIT 1",
            stream_id
        )
        raw_payload_after = raw_row_after["payload"]
        if isinstance(raw_payload_after, str):
            raw_payload_after = json.loads(raw_payload_after)
        
        # CRITICAL ASSERTION: Raw storage unchanged after upcast read
        assert raw_payload_after == raw_payload, "IMMUTABILITY VIOLATION: Database payload was modified!"
        assert "model_version" not in raw_payload_after, "Database should never contain upcasted fields"
    
    print("✓ Immutability guarantee test passed — upcasting is read-time only")

@pytest.mark.asyncio
async def test_upcaster_inference_strategy(event_store):
    """Test that NULL inference is used for genuinely unknown fields."""
    stream_id = "inference-test-001"
    
    await event_store.append(
        stream_id=stream_id,
        events=[BaseEvent(
            type="CreditAnalysisCompleted",
            version=1,
            payload={"application_id": "infer-001", "agent_id": "test", "risk_tier": "LOW"}
        )],
        expected_version=-1
    )
    
    events = await event_store.load_stream(stream_id)
    upcasted = events[0]
    
    # confidence_score should be NULL, not fabricated
    assert upcasted.payload["confidence_score"] is None, "NULL is correct for unknown historical data"
    
    # model_version should be inferred, not guessed
    assert upcasted.payload["model_version"] == "legacy-pre-2026", "Inferred from timestamp"
    
    print("✓ Inference strategy test passed — NULL for unknown, inferred for derivable")
