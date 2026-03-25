import pytest
import asyncio
import json
from src.store.event_store import EventStore
from src.store.upcaster import UpcasterRegistry
from src.integrity.audit_chain import CryptographicAuditChain, IntegrityCheckResult
from src.events import BaseEvent
import os

@pytest.fixture
async def event_store():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:Pass%4012345@localhost:5433/ledger_infrastructure")
    store = EventStore(dsn, UpcasterRegistry())
    await store.connect()
    
    # Clean up test streams before each test
    async with store._pool.acquire() as conn:
        # Clean up regular test streams
        test_streams = ["chain-test", "tamper-test", "record-test"]
        for prefix in test_streams:
            await conn.execute(
                "DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id LIKE $1)",
                f"{prefix}%"
            )
            await conn.execute("DELETE FROM events WHERE stream_id LIKE $1", f"{prefix}%")
            await conn.execute("DELETE FROM event_streams WHERE stream_id LIKE $1", f"{prefix}%")
        
        # Clean up audit stream for fresh integrity checks
        await conn.execute(
            "DELETE FROM outbox WHERE event_id IN (SELECT event_id FROM events WHERE stream_id = 'system:audit:integrity')"
        )
        await conn.execute("DELETE FROM events WHERE stream_id = 'system:audit:integrity'")
        await conn.execute("DELETE FROM event_streams WHERE stream_id = 'system:audit:integrity'")
    
    yield store
    await store.close()

@pytest.fixture
def audit_chain():
    return CryptographicAuditChain(algorithm="sha256")

@pytest.mark.asyncio
async def test_cryptographic_chain_computation(event_store, audit_chain):
    """Test that hash chain is computed correctly."""
    # Append some test events
    for i in range(3):
        await event_store.append(
            stream_id=f"chain-test-{i}",
            events=[BaseEvent(
                type="ApplicationSubmitted",
                version=1,
                payload={"application_id": f"chain-app-{i}", "amount": 10000 + i}
            )],
            expected_version=-1
        )
    
    # Run integrity check WITHOUT previous hash (fresh chain)
    async with event_store._pool.acquire() as conn:
        result = await audit_chain.run_integrity_check(conn, from_position=0, previous_check_hash=None)
        
        assert result.chain_valid, "Fresh chain should be valid"
        assert not result.tamper_detected
        assert result.next_expected_hash is not None
        assert len(result.next_expected_hash) == 64  # SHA256 hex length
    
    print("✓ Cryptographic chain computation test passed")

@pytest.mark.asyncio
async def test_tamper_detection(event_store, audit_chain):
    """Test that tampering with stored data is detected."""
    # Append events
    for i in range(2):
        await event_store.append(
            stream_id=f"tamper-test-{i}",
            events=[BaseEvent(
                type="CreditAnalysisCompleted",
                version=1,
                payload={"application_id": f"tamper-{i}", "risk_tier": "MEDIUM"}
            )],
            expected_version=-1
        )
    
    # Record baseline integrity WITH explicit previous hash (None = first check)
    async with event_store._pool.acquire() as conn:
        baseline = await audit_chain.run_integrity_check(conn, from_position=0, previous_check_hash=None)
        assert baseline.chain_valid, "First check should be valid"
        
        # Record the check to create a baseline hash
        await audit_chain.record_integrity_check(conn, baseline, auditor_id="test-auditor")
        
        # NOW TAMPER: Directly modify a payload in the database
        await conn.execute("""
            UPDATE events 
            SET payload = jsonb_set(payload, '{risk_tier}', '"HIGH"')
            WHERE event_type = 'CreditAnalysisCompleted' 
            AND stream_position = 1
        """)
        
        # Run integrity check AGAIN with the baseline hash - should detect tampering
        tamper_result = await audit_chain.run_integrity_check(
            conn, 
            from_position=0, 
            previous_check_hash=baseline.next_expected_hash  # Explicit comparison
        )
        
        assert tamper_result.tamper_detected, "Should detect payload tampering"
        assert not tamper_result.chain_valid
        assert tamper_result.tamper_details is not None
    
    print("✓ Tamper detection test passed — cryptographic chain catches modifications")

@pytest.mark.asyncio
async def test_integrity_check_recording(event_store, audit_chain):
    """Test that integrity checks are recorded as audit events."""
    # Append an event
    await event_store.append(
        stream_id="record-test-001",
        events=[BaseEvent(type="ApplicationSubmitted", version=1, payload={"app": "test"})],
        expected_version=-1
    )
    
    # Run and record integrity check
    async with event_store._pool.acquire() as conn:
        result = await audit_chain.run_integrity_check(conn, from_position=0, previous_check_hash=None)
        event_id = await audit_chain.record_integrity_check(conn, result, auditor_id="auto-auditor")
        
        # Verify the audit event was recorded
        audit_events = await conn.fetch("""
            SELECT * FROM events 
            WHERE stream_id = 'system:audit:integrity'
            AND payload->>'check_id' = $1
        """, result.check_id)
        
        assert len(audit_events) == 1, "Audit check should be recorded"
        assert audit_events[0]["event_type"] == "AuditIntegrityCheckRun"
    
    print("✓ Integrity check recording test passed")
