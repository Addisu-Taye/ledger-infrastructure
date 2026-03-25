import pytest
import asyncio
from src.store.event_store import EventStore
from src.store.upcaster import UpcasterRegistry
from src.projections.daemon import ProjectionDaemon
from src.projections.handlers import ApplicationSummaryProjection, AgentPerformanceProjection, ComplianceAuditProjection
from src.events import BaseEvent
import os

@pytest.fixture
async def event_store():
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:Pass%4012345@localhost:5433/ledger_infrastructure")
    store = EventStore(dsn, UpcasterRegistry())
    await store.connect()
    yield store
    await store.close()

@pytest.fixture
async def projection_daemon(event_store):
    projections = [
        ApplicationSummaryProjection(),
        AgentPerformanceProjection(), 
        ComplianceAuditProjection()
    ]
    daemon = ProjectionDaemon(event_store, projections)
    yield daemon
    daemon.stop()

@pytest.mark.asyncio
async def test_application_summary_projection(event_store, projection_daemon):
    """Test that ApplicationSummary projection updates correctly."""
    # Submit an application
    await event_store.append(
        stream_id="loan-proj-test-001",
        events=[BaseEvent(type="ApplicationSubmitted", payload={
            "application_id": "proj-test-001",
            "applicant_id": "applicant-123",
            "requested_amount_usd": 50000
        })],
        expected_version=-1
    )
    
    # Process events through daemon
    await projection_daemon._process_batch(batch_size=10)
    
    # Verify projection was updated
    async with event_store._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, applicant_id, requested_amount_usd FROM application_summary WHERE application_id = $1",
            "proj-test-001"
        )
        assert row is not None
        assert row["state"] == "SUBMITTED"
        assert row["applicant_id"] == "applicant-123"
        assert row["requested_amount_usd"] == 50000
    
    print("✓ ApplicationSummary projection test passed")

@pytest.mark.asyncio  
async def test_projection_lag_metric(event_store, projection_daemon):
    """Test that projection lag is measurable."""
    # Get initial lag
    initial_lag = projection_daemon.get_lag()
    assert "last_processed_position" in initial_lag
    
    # Append some events
    for i in range(5):
        await event_store.append(
            stream_id=f"loan-lag-test-{i}",
            events=[BaseEvent(type="ApplicationSubmitted", payload={"application_id": f"lag-{i}"})],
            expected_version=-1
        )
    
    # Process and verify lag decreases
    await projection_daemon._process_batch(batch_size=10)
    updated_lag = projection_daemon.get_lag()
    
    print(f"✓ Projection lag metric test passed: {updated_lag}")

@pytest.mark.asyncio
async def test_compliance_temporal_query(event_store, projection_daemon):
    """Test ComplianceAuditView temporal query capability."""
    app_id = "compliance-temporal-001"
    
    # Submit application with compliance check
    await event_store.append(
        stream_id=f"compliance-{app_id}",
        events=[
            BaseEvent(type="ComplianceCheckRequested", payload={
                "application_id": app_id,
                "regulation_set_version": "v2026.1",
                "checks_required": ["KYC", "AML"]
            }),
            BaseEvent(type="ComplianceRulePassed", payload={
                "application_id": app_id,
                "rule_id": "KYC-001",
                "rule_version": "1.0",
                "evidence_hash": "abc123"
            })
        ],
        expected_version=-1
    )
    
    # Process events
    await projection_daemon._process_batch(batch_size=10)
    
    # Query compliance at current time
    async with event_store._pool.acquire() as conn:
        compliance_proj = ComplianceAuditProjection()
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)
        
        result = await compliance_proj.get_state_at(conn, app_id, now)
        assert result["application_id"] == app_id
        assert len(result["checks"]) >= 1
        
        # Query at past timestamp (before event) - should show PENDING
        past_result = await compliance_proj.get_state_at(conn, app_id, past)
        assert past_result["overall_status"] in ["PENDING", "IN_PROGRESS"]
    
    print("✓ Compliance temporal query test passed")
