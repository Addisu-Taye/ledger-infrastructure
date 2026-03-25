from typing import Protocol, List
from src.events import StoredEvent
from src.projections.models import ApplicationSummary, AgentPerformanceMetrics, ComplianceAuditView, ComplianceCheck
import asyncpg
from datetime import datetime, timezone
import json

def _safe_get(obj, key, default=None):
    """Safely get value from dict or JSON string."""
    if isinstance(obj, str):
        obj = json.loads(obj)
    return obj.get(key, default) if isinstance(obj, dict) else default

class Projection(Protocol):
    """Interface for all projections."""
    name: str
    subscribed_events: List[str]
    
    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        ...
    
    async def get_lag(self, conn: asyncpg.Connection) -> int:
        ...

class ApplicationSummaryProjection:
    """Projection 1: ApplicationSummary handler."""
    name = "application_summary"
    subscribed_events = [
        "ApplicationSubmitted", "CreditAnalysisCompleted", 
        "FraudScreeningCompleted", "ComplianceRulePassed", 
        "ComplianceRuleFailed", "DecisionGenerated",
        "HumanReviewCompleted", "ApplicationApproved", "ApplicationDeclined"
    ]
    
    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        payload = event.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        app_id = payload.get("application_id")
        if not app_id:
            return
        
        if event.event_type == "ApplicationSubmitted":
            await conn.execute("""
                INSERT INTO application_summary 
                (application_id, state, applicant_id, requested_amount_usd, last_event_type, last_event_at)
                VALUES ($1, 'SUBMITTED', $2, $3, $4, $5)
                ON CONFLICT (application_id) DO UPDATE SET
                    state = EXCLUDED.state,
                    applicant_id = EXCLUDED.applicant_id,
                    requested_amount_usd = EXCLUDED.requested_amount_usd,
                    last_event_type = EXCLUDED.last_event_type,
                    last_event_at = EXCLUDED.last_event_at
            """, app_id, payload.get("applicant_id"), payload.get("requested_amount_usd"),
                event.event_type, event.recorded_at)
        
        elif event.event_type == "CreditAnalysisCompleted":
            await conn.execute("""
                UPDATE application_summary SET 
                    risk_tier = $1, last_event_type = $2, last_event_at = $3
                WHERE application_id = $4
            """, payload.get("risk_tier"), event.event_type, event.recorded_at, app_id)
        
        elif event.event_type == "FraudScreeningCompleted":
            await conn.execute("""
                UPDATE application_summary SET 
                    fraud_score = $1, last_event_type = $2, last_event_at = $3
                WHERE application_id = $4
            """, payload.get("fraud_score"), event.event_type, event.recorded_at, app_id)
        
        elif event.event_type in ("ComplianceRulePassed", "ComplianceRuleFailed"):
            status = "COMPLIANT" if event.event_type == "ComplianceRulePassed" else "NON_COMPLIANT"
            await conn.execute("""
                UPDATE application_summary SET 
                    compliance_status = $1, last_event_type = $2, last_event_at = $3
                WHERE application_id = $4
            """, status, event.event_type, event.recorded_at, app_id)
        
        elif event.event_type == "DecisionGenerated":
            await conn.execute("""
                UPDATE application_summary SET 
                    decision = $1, last_event_type = $2, last_event_at = $3
                WHERE application_id = $4
            """, payload.get("recommendation"), event.event_type, event.recorded_at, app_id)
        
        elif event.event_type == "HumanReviewCompleted":
            await conn.execute("""
                UPDATE application_summary SET 
                    human_reviewer_id = $1, final_decision_at = $2,
                    state = $3, last_event_type = $4, last_event_at = $5
                WHERE application_id = $6
            """, payload.get("reviewer_id"), event.recorded_at, 
                "FINAL_" + payload.get("final_decision", "PENDING").upper(),
                event.event_type, event.recorded_at, app_id)
        
        elif event.event_type == "ApplicationApproved":
            await conn.execute("""
                UPDATE application_summary SET 
                    approved_amount_usd = $1, state = 'FINAL_APPROVED',
                    last_event_type = $2, last_event_at = $3
                WHERE application_id = $4
            """, payload.get("approved_amount_usd"), event.event_type, event.recorded_at, app_id)
        
        elif event.event_type == "ApplicationDeclined":
            await conn.execute("""
                UPDATE application_summary SET 
                    state = 'FINAL_DECLINED', last_event_type = $1, last_event_at = $2
                WHERE application_id = $3
            """, event.event_type, event.recorded_at, app_id)
    
    async def get_lag(self, conn: asyncpg.Connection) -> int:
        result = await conn.fetchrow("""
            SELECT 
                (SELECT COALESCE(MAX(global_position), 0) FROM events) as latest,
                (SELECT COALESCE(last_position, 0) FROM projection_checkpoints WHERE projection_name = $1) as checkpoint
        """, self.name)
        if result:
            return max(0, (result["latest"] - result["checkpoint"]) * 10)
        return 0

class AgentPerformanceProjection:
    """Projection 2: AgentPerformanceLedger handler."""
    name = "agent_performance"
    subscribed_events = ["CreditAnalysisCompleted", "DecisionGenerated", "HumanReviewCompleted"]
    
    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        payload = event.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        agent_id = payload.get("agent_id")
        model_version = payload.get("model_version")
        if not agent_id or not model_version:
            return
        
        if event.event_type == "CreditAnalysisCompleted":
            await conn.execute("""
                INSERT INTO agent_performance 
                (agent_id, model_version, analyses_completed, avg_duration_ms, last_seen_at)
                VALUES ($1, $2, 1, $3, $4)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    analyses_completed = agent_performance.analyses_completed + 1,
                    avg_duration_ms = CASE 
                        WHEN agent_performance.avg_duration_ms IS NULL THEN $3
                        ELSE (agent_performance.avg_duration_ms * agent_performance.analyses_completed + $3) / 
                             (agent_performance.analyses_completed + 1)
                    END,
                    last_seen_at = $4
            """, agent_id, model_version, payload.get("analysis_duration_ms"), event.recorded_at)
        
        elif event.event_type == "DecisionGenerated":
            await conn.execute("""
                INSERT INTO agent_performance (agent_id, model_version, decisions_generated, last_seen_at)
                VALUES ($1, $2, 1, $3)
                ON CONFLICT (agent_id, model_version) DO UPDATE SET
                    decisions_generated = agent_performance.decisions_generated + 1,
                    last_seen_at = $3
            """, agent_id, model_version, event.recorded_at)
    
    async def get_lag(self, conn: asyncpg.Connection) -> int:
        result = await conn.fetchrow("""
            SELECT 
                (SELECT COALESCE(MAX(global_position), 0) FROM events) as latest,
                (SELECT COALESCE(last_position, 0) FROM projection_checkpoints WHERE projection_name = $1) as checkpoint
        """, self.name)
        if result:
            return max(0, (result["latest"] - result["checkpoint"]) * 10)
        return 0

class ComplianceAuditProjection:
    """Projection 3: ComplianceAuditView with temporal query support."""
    name = "compliance_audit"
    subscribed_events = ["ComplianceCheckRequested", "ComplianceRulePassed", "ComplianceRuleFailed"]
    
    async def handle(self, event: StoredEvent, conn: asyncpg.Connection) -> None:
        payload = event.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        app_id = payload.get("application_id")
        if not app_id:
            return
        
        if event.event_type == "ComplianceCheckRequested":
            await conn.execute("""
                INSERT INTO compliance_audit (application_id, regulation_set_version, last_updated_at)
                VALUES ($1, $2, $3)
                ON CONFLICT (application_id) DO NOTHING
            """, app_id, payload.get("regulation_set_version"), event.recorded_at)
        
        elif event.event_type in ("ComplianceRulePassed", "ComplianceRuleFailed"):
            status = "PASSED" if event.event_type == "ComplianceRulePassed" else "FAILED"
            await conn.execute("""
                INSERT INTO compliance_checks 
                (application_id, rule_id, rule_version, status, evaluation_timestamp, 
                 evidence_hash, failure_reason, remediation_required)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """, app_id, payload.get("rule_id"), payload.get("rule_version"), status,
                event.recorded_at, payload.get("evidence_hash"), 
                payload.get("failure_reason"), payload.get("remediation_required", False))
            
            await conn.execute("""
                UPDATE compliance_audit SET 
                    overall_status = CASE 
                        WHEN EXISTS (
                            SELECT 1 FROM compliance_checks 
                            WHERE application_id = $1 AND status = 'FAILED' AND remediation_required = true
                        ) THEN 'FAILED'
                        WHEN NOT EXISTS (
                            SELECT 1 FROM compliance_checks 
                            WHERE application_id = $1 AND status != 'PASSED'
                        ) THEN 'COMPLIANT'
                        ELSE 'IN_PROGRESS'
                    END,
                    last_updated_at = $2
                WHERE application_id = $1
            """, app_id, event.recorded_at)
    
    async def get_lag(self, conn: asyncpg.Connection) -> int:
        result = await conn.fetchrow("""
            SELECT 
                (SELECT COALESCE(MAX(global_position), 0) FROM events) as latest,
                (SELECT COALESCE(last_position, 0) FROM projection_checkpoints WHERE projection_name = $1) as checkpoint
        """, self.name)
        if result:
            return max(0, (result["latest"] - result["checkpoint"]) * 10)
        return 0
    
    async def get_state_at(self, conn: asyncpg.Connection, application_id: str, timestamp: datetime) -> dict:
        """Temporal query: get compliance state at a specific timestamp."""
        checks = await conn.fetch("""
            SELECT rule_id, rule_version, status, evaluation_timestamp, 
                   evidence_hash, failure_reason, remediation_required
            FROM compliance_checks
            WHERE application_id = $1 AND evaluation_timestamp <= $2
            ORDER BY evaluation_timestamp
        """, application_id, timestamp)
        
        if not checks:
            status = "PENDING"
        elif any(c["status"] == "FAILED" and c["remediation_required"] for c in checks):
            status = "FAILED"
        elif all(c["status"] == "PASSED" for c in checks):
            status = "COMPLIANT"
        else:
            status = "IN_PROGRESS"
        
        return {
            "application_id": application_id,
            "regulation_set_version": "unknown",
            "checks": [dict(c) for c in checks],
            "overall_status": status,
            "as_of_timestamp": timestamp.isoformat()
        }
