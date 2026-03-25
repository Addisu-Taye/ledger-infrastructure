"""
MCP Tools — Command implementations for The Ledger

Each tool validates preconditions, enforces business rules, and appends events atomically.
Returns structured results or typed errors for agent recovery.
"""

import uuid
import json
from datetime import datetime
from typing import Optional
from src.store.event_store import EventStore, OptimisticConcurrencyError
from src.events import BaseEvent
from src.aggregates.loan_application import LoanApplicationAggregate
from src.aggregates.agent_session import AgentSessionAggregate
from src.aggregates.compliance_record import ComplianceRecordAggregate


async def submit_application(
    store: EventStore,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    applicant_data: Optional[dict] = None,
    **kwargs
) -> dict:
    """Submit a new loan application."""
    # Validate preconditions
    if requested_amount_usd <= 0:
        return {"error": "InvalidAmount", "message": "requested_amount_usd must be > 0", "suggested_action": "retry_with_valid_amount"}
    
    # Create and append event
    event = BaseEvent(
        type="ApplicationSubmitted",
        version=1,
        payload={
            "application_id": application_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": requested_amount_usd,
            "applicant_data": applicant_data or {},
            "submitted_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=f"loan:{application_id}",
            events=[event],
            expected_version=-1,  # New stream
            correlation_id=kwargs.get("correlation_id"),
            causation_id=kwargs.get("causation_id")
        )
        return {"success": True, "stream_version": version, "application_id": application_id}
    
    except OptimisticConcurrencyError as e:
        return {
            "error": "ConcurrencyConflict",
            "message": f"Application already exists: {e}",
            "suggested_action": "load_and_retry"
        }


async def record_credit_analysis(
    store: EventStore,
    application_id: str,
    agent_id: str,
    risk_tier: str,
    confidence_score: Optional[float] = None,
    analysis_duration_ms: Optional[int] = None,
    **kwargs
) -> dict:
    """Record AI agent credit analysis."""
    # Load aggregate to validate state
    stream_id = f"loan:{application_id}"
    events = await store.load_stream(stream_id)
    
    if not events:
        return {"error": "ApplicationNotFound", "message": f"No events for {application_id}", "suggested_action": "submit_application_first"}
    
    # Reconstruct aggregate and validate business rules
    aggregate = LoanApplicationAggregate.load(events)
    
    try:
        aggregate.assert_awaiting_credit_analysis()  # Rule: must be in SUBMITTED state
    except ValueError as e:
        return {"error": "InvalidState", "message": str(e), "suggested_action": "check_application_state"}
    
    # Create and append event
    event = BaseEvent(
        type="CreditAnalysisCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "agent_id": agent_id,
            "risk_tier": risk_tier,
            "confidence_score": confidence_score,
            "analysis_duration_ms": analysis_duration_ms,
            "analyzed_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=aggregate.current_version,
            correlation_id=kwargs.get("correlation_id"),
            causation_id=kwargs.get("causation_id")
        )
        return {"success": True, "stream_version": version, "risk_tier": risk_tier}
    
    except OptimisticConcurrencyError as e:
        return {
            "error": "ConcurrencyConflict",
            "message": "Another agent updated this application",
            "suggested_action": "reload_and_reanalyze"
        }


async def record_fraud_screening(
    store: EventStore,
    application_id: str,
    fraud_score: float,
    flags: Optional[list] = None,
    review_required: bool = False,
    **kwargs
) -> dict:
    """Record fraud screening result."""
    stream_id = f"loan:{application_id}"
    events = await store.load_stream(stream_id)
    aggregate = LoanApplicationAggregate.load(events)
    
    try:
        aggregate.assert_credit_analysis_complete()  # Rule: fraud check requires prior credit analysis
    except ValueError as e:
        return {"error": "InvalidState", "message": str(e), "suggested_action": "complete_credit_analysis_first"}
    
    event = BaseEvent(
        type="FraudScreeningCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "fraud_score": fraud_score,
            "flags": flags or [],
            "review_required": review_required,
            "screened_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(stream_id, [event], expected_version=aggregate.current_version, **kwargs)
        return {"success": True, "stream_version": version, "fraud_score": fraud_score}
    except OptimisticConcurrencyError:
        return {"error": "ConcurrencyConflict", "suggested_action": "reload_and_retry"}


async def record_compliance_check(
    store: EventStore,
    application_id: str,
    rule_id: str,
    rule_version: str,
    status: str,
    evidence_hash: Optional[str] = None,
    failure_reason: Optional[str] = None,
    remediation_required: bool = False,
    **kwargs
) -> dict:
    """Record compliance rule evaluation."""
    stream_id = f"loan:{application_id}"
    events = await store.load_stream(stream_id)
    aggregate = LoanApplicationAggregate.load(events)
    
    try:
        aggregate.assert_fraud_screening_complete()  # Rule: compliance requires prior fraud check
    except ValueError as e:
        return {"error": "InvalidState", "message": str(e), "suggested_action": "complete_fraud_screening_first"}
    
    event_type = "ComplianceRulePassed" if status == "PASSED" else "ComplianceRuleFailed"
    event = BaseEvent(
        type=event_type,
        version=1,
        payload={
            "application_id": application_id,
            "rule_id": rule_id,
            "rule_version": rule_version,
            "status": status,
            "evidence_hash": evidence_hash,
            "failure_reason": failure_reason,
            "remediation_required": remediation_required,
            "evaluated_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(stream_id, [event], expected_version=aggregate.current_version, **kwargs)
        return {"success": True, "stream_version": version, "rule_status": status}
    except OptimisticConcurrencyError:
        return {"error": "ConcurrencyConflict", "suggested_action": "reload_and_retry"}


async def generate_decision(
    store: EventStore,
    application_id: str,
    recommendation: str,
    reason: str,
    approved_amount_usd: Optional[float] = None,
    **kwargs
) -> dict:
    """Generate final decision recommendation."""
    stream_id = f"loan:{application_id}"
    events = await store.load_stream(stream_id)
    aggregate = LoanApplicationAggregate.load(events)
    
    # Rule: cannot approve without compliance
    if recommendation == "APPROVE":
        try:
            aggregate.assert_compliance_complete()
        except ValueError as e:
            return {"error": "ComplianceIncomplete", "message": str(e), "suggested_action": "complete_all_compliance_checks"}
    
    # Rule: confidence floor — low confidence = REFER
    confidence = kwargs.get("confidence_score", 1.0)
    if confidence < 0.6 and recommendation != "REFER":
        return {
            "error": "LowConfidence",
            "message": f"Confidence {confidence} < 0.6 requires REFER recommendation",
            "suggested_action": "change_recommendation_to_refer"
        }
    
    event = BaseEvent(
        type="DecisionGenerated",
        version=1,
        payload={
            "application_id": application_id,
            "recommendation": recommendation,
            "reason": reason,
            "approved_amount_usd": approved_amount_usd if recommendation == "APPROVE" else None,
            "confidence_score": confidence,
            "generated_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(stream_id, [event], expected_version=aggregate.current_version, **kwargs)
        return {"success": True, "stream_version": version, "recommendation": recommendation}
    except OptimisticConcurrencyError:
        return {"error": "ConcurrencyConflict", "suggested_action": "reload_and_retry"}


async def record_human_review(
    store: EventStore,
    application_id: str,
    reviewer_id: str,
    final_decision: str,
    notes: Optional[str] = None,
    override_reason: Optional[str] = None,
    **kwargs
) -> dict:
    """Record human reviewer decision."""
    stream_id = f"loan:{application_id}"
    events = await store.load_stream(stream_id)
    aggregate = LoanApplicationAggregate.load(events)
    
    try:
        aggregate.assert_decision_generated()  # Rule: human review requires prior AI decision
    except ValueError as e:
        return {"error": "InvalidState", "message": str(e), "suggested_action": "generate_decision_first"}
    
    event = BaseEvent(
        type="HumanReviewCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "reviewer_id": reviewer_id,
            "final_decision": final_decision,
            "notes": notes,
            "override_reason": override_reason,
            "reviewed_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(stream_id, [event], expected_version=aggregate.current_version, **kwargs)
        
        # Append final state event
        final_event_type = "ApplicationApproved" if final_decision == "APPROVED" else "ApplicationDeclined"
        final_event = BaseEvent(
            type=final_event_type,
            version=1,
            payload={
                "application_id": application_id,
                "final_decision": final_decision,
                "decided_at": datetime.utcnow().isoformat()
            }
        )
        await store.append(stream_id, [final_event], expected_version=version, **kwargs)
        
        return {"success": True, "final_decision": final_decision}
    except OptimisticConcurrencyError:
        return {"error": "ConcurrencyConflict", "suggested_action": "reload_and_retry"}


async def start_agent_session(
    store: EventStore,
    session_id: str,
    agent_id: str,
    model_version: str,
    context_data: Optional[dict] = None,
    **kwargs
) -> dict:
    """Initialize agent session context (Gas Town pattern)."""
    stream_id = f"agent-session:{session_id}"
    
    # Rule: ContextLoaded must be first event in session stream
    events = await store.load_stream(stream_id)
    if events:
        return {
            "error": "SessionAlreadyInitialized",
            "message": f"Session {session_id} already has events",
            "suggested_action": "use_existing_session_or_create_new"
        }
    
    event = BaseEvent(
        type="AgentContextLoaded",
        version=1,
        payload={
            "session_id": session_id,
            "agent_id": agent_id,
            "model_version": model_version,
            "context_data": context_data or {},
            "loaded_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(stream_id, [event], expected_version=-1, **kwargs)
        return {"success": True, "session_id": session_id, "stream_version": version}
    except OptimisticConcurrencyError:
        return {"error": "ConcurrencyConflict", "suggested_action": "retry_with_new_session_id"}


async def run_integrity_check(
    store: EventStore,
    auditor_id: str,
    from_position: int = 0,
    to_position: Optional[int] = None,
    **kwargs
) -> dict:
    """Run cryptographic audit chain verification."""
    from src.integrity.audit_chain import CryptographicAuditChain
    
    chain = CryptographicAuditChain()
    
    async with store._pool.acquire() as conn:
        result = await chain.run_integrity_check(
            conn,
            from_position=from_position,
            to_position=to_position,
            previous_check_hash=kwargs.get("previous_check_hash")
        )
        
        # Record the check as an audit event
        await chain.record_integrity_check(conn, result, auditor_id=auditor_id)
    
    return {
        "success": True,
        "check_id": result.check_id,
        "chain_valid": result.chain_valid,
        "tamper_detected": result.tamper_detected,
        "last_verified_position": result.last_verified_position,
        "tamper_details": result.tamper_details
    }
