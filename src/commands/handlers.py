"""
Command Handlers — Load → Validate → Determine → Append Pattern

All command handlers follow this exact structure:
1. Reconstruct current aggregate state from event history
2. Validate — all business rules checked BEFORE any state change
3. Determine new events — pure logic, no I/O
4. Append atomically — optimistic concurrency enforced by store

Required for Final Submission: Phase 2
"""
from __future__ import annotations
from datetime import datetime
from typing import Optional, Dict, Any, List
from src.store.event_store import EventStore, OptimisticConcurrencyError
from src.aggregates.loan_application import LoanApplicationAggregate, DomainError as LoanDomainError
from src.aggregates.agent_session import AgentSessionAggregate, DomainError as AgentDomainError
from src.aggregates.compliance_record import ComplianceRecordAggregate, DomainError as ComplianceDomainError
from src.events import BaseEvent


class CommandHandlerError(Exception):
    """Raised when a command handler fails validation or execution."""
    def __init__(self, error_type: str, message: str, suggested_action: str):
        self.error_type = error_type
        self.message = message
        self.suggested_action = suggested_action
        super().__init__(message)


# ─────────────────────────────────────────────────────────────────────────────
# SUBMIT APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

async def handle_submit_application(
    store: EventStore,
    application_id: str,
    applicant_id: str,
    requested_amount_usd: float,
    loan_purpose: str,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Submit a new loan application.
    
    Preconditions:
    - application_id must be unique
    - requested_amount_usd must be > 0
    - applicant_id must exist in registry (validated externally)
    """
    if requested_amount_usd <= 0:
        raise CommandHandlerError(
            "InvalidAmount",
            "requested_amount_usd must be greater than 0",
            "retry_with_valid_amount"
        )
    
    # Check for existing application
    try:
        app = await LoanApplicationAggregate.load(store, application_id)
        if app.version > 0:
            raise CommandHandlerError(
                "DuplicateApplication",
                f"Application {application_id} already exists",
                "use_different_application_id"
            )
    except Exception:
        pass  # New application, continue
    
    # Create submit event
    event = BaseEvent(
        type="ApplicationSubmitted",
        version=1,
        payload={
            "application_id": application_id,
            "applicant_id": applicant_id,
            "requested_amount_usd": requested_amount_usd,
            "loan_purpose": loan_purpose,
            "submitted_at": datetime.utcnow().isoformat()
        }
    )
    
    # Append with expected_version=-1 (new stream)
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[event],
            expected_version=-1,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "state": "SUBMITTED"
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            f"Application already exists: {e}",
            "load_and_retry"
        )


# ─────────────────────────────────────────────────────────────────────────────
# CREDIT ANALYSIS COMPLETED
# ─────────────────────────────────────────────────────────────────────────────

async def handle_credit_analysis_completed(
    store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    model_version: str,
    confidence_score: float,
    risk_tier: str,
    recommended_limit_usd: float,
    analysis_duration_ms: int,
    input_data_hash: str,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record credit analysis completion.
    
    Preconditions:
    - Application must be in SUBMITTED or AWAITING_ANALYSIS state
    - Agent session must have ContextLoaded event (Gas Town pattern)
    - Model version must be current
    """
    # Load aggregates
    app = await LoanApplicationAggregate.load(store, application_id)
    agent = await AgentSessionAggregate.load(store, agent_id, session_id)
    
    # Validate business rules
    try:
        app.assert_awaiting_credit_analysis()
        agent.assert_context_loaded()
        agent.assert_model_version_current(model_version)
    except (LoanDomainError, AgentDomainError) as e:
        raise CommandHandlerError(
            "ValidationFailed",
            str(e),
            "check_application_state_and_agent_session"
        )
    
    # Create event
    event = BaseEvent(
        type="CreditAnalysisCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "model_version": model_version,
            "confidence_score": confidence_score,
            "risk_tier": risk_tier,
            "recommended_limit_usd": recommended_limit_usd,
            "analysis_duration_ms": analysis_duration_ms,
            "input_data_hash": input_data_hash,
            "completed_at": datetime.utcnow().isoformat()
        }
    )
    
    # Append with optimistic concurrency
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[event],
            expected_version=app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "risk_tier": risk_tier,
            "confidence_score": confidence_score
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Another agent updated this application",
            "reload_and_reanalyze"
        )


# ─────────────────────────────────────────────────────────────────────────────
# FRAUD SCREENING COMPLETED
# ─────────────────────────────────────────────────────────────────────────────

async def handle_fraud_screening_completed(
    store: EventStore,
    application_id: str,
    agent_id: str,
    session_id: str,
    fraud_score: float,
    anomaly_flags: List[str],
    screening_model_version: str,
    input_data_hash: str,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record fraud screening completion.
    
    Preconditions:
    - Credit analysis must be complete
    - Fraud score must be between 0.0 and 1.0
    """
    if not (0.0 <= fraud_score <= 1.0):
        raise CommandHandlerError(
            "InvalidFraudScore",
            "fraud_score must be between 0.0 and 1.0",
            "retry_with_valid_score"
        )
    
    app = await LoanApplicationAggregate.load(store, application_id)
    
    try:
        app.assert_credit_analysis_complete()
    except LoanDomainError as e:
        raise CommandHandlerError(
            "ValidationFailed",
            str(e),
            "complete_credit_analysis_first"
        )
    
    event = BaseEvent(
        type="FraudScreeningCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "agent_id": agent_id,
            "session_id": session_id,
            "fraud_score": fraud_score,
            "anomaly_flags": anomaly_flags,
            "screening_model_version": screening_model_version,
            "input_data_hash": input_data_hash,
            "completed_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[event],
            expected_version=app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "fraud_score": fraud_score
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Another agent updated this application",
            "reload_and_retry"
        )


# ─────────────────────────────────────────────────────────────────────────────
# COMPLIANCE CHECK
# ─────────────────────────────────────────────────────────────────────────────

async def handle_compliance_check(
    store: EventStore,
    application_id: str,
    rule_id: str,
    rule_version: str,
    status: str,  # "PASSED" or "FAILED"
    failure_reason: Optional[str] = None,
    remediation_required: bool = False,
    is_hard_block: bool = False,
    evidence_hash: Optional[str] = None,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record compliance rule evaluation.
    
    Preconditions:
    - Fraud screening must be complete
    - Rule must exist in active regulation set
    """
    app = await LoanApplicationAggregate.load(store, application_id)
    compliance = await ComplianceRecordAggregate.load(store, application_id)
    
    try:
        app.assert_fraud_screening_complete()
        compliance.assert_can_evaluate_rule(rule_id)
    except (LoanDomainError, ComplianceDomainError) as e:
        raise CommandHandlerError(
            "ValidationFailed",
            str(e),
            "complete_prerequisites_first"
        )
    
    event_type = "ComplianceRulePassed" if status == "PASSED" else "ComplianceRuleFailed"
    event = BaseEvent(
        type=event_type,
        version=1,
        payload={
            "application_id": application_id,
            "rule_id": rule_id,
            "rule_version": rule_version,
            "status": status,
            "failure_reason": failure_reason,
            "remediation_required": remediation_required,
            "is_hard_block": is_hard_block,
            "evidence_hash": evidence_hash,
            "evaluated_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=f"compliance-{application_id}",
            events=[event],
            expected_version=compliance.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "application_id": application_id,
            "rule_id": rule_id,
            "status": status,
            "stream_version": version
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Another agent updated compliance record",
            "reload_and_retry"
        )


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE DECISION
# ─────────────────────────────────────────────────────────────────────────────

async def handle_generate_decision(
    store: EventStore,
    application_id: str,
    orchestrator_agent_id: str,
    recommendation: str,  # "APPROVE", "DECLINE", "REFER"
    confidence_score: float,
    contributing_agent_sessions: List[str],
    decision_basis_summary: str,
    model_versions: Dict[str, str],
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate final decision recommendation.
    
    Preconditions:
    - All required analyses must be present (credit, fraud, compliance)
    - Confidence floor enforcement: < 0.6 → must REFER
    - Compliance must not be blocked
    """
    app = await LoanApplicationAggregate.load(store, application_id)
    
    # Enforce confidence floor (regulatory requirement)
    if confidence_score < 0.6 and recommendation != "REFER":
        raise CommandHandlerError(
            "ConfidenceFloorViolation",
            f"Confidence {confidence_score} < 0.6 requires REFER recommendation",
            "change_recommendation_to_refer"
        )
    
    try:
        app.assert_all_analyses_complete()
        app.assert_compliance_not_blocked()
    except LoanDomainError as e:
        raise CommandHandlerError(
            "ValidationFailed",
            str(e),
            "complete_all_analyses_first"
        )
    
    event = BaseEvent(
        type="DecisionGenerated",
        version=1,
        payload={
            "application_id": application_id,
            "orchestrator_agent_id": orchestrator_agent_id,
            "recommendation": recommendation,
            "confidence_score": confidence_score,
            "contributing_agent_sessions": contributing_agent_sessions,
            "decision_basis_summary": decision_basis_summary,
            "model_versions": model_versions,
            "generated_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[event],
            expected_version=app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "application_id": application_id,
            "stream_version": version,
            "recommendation": recommendation
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Another agent updated this application",
            "reload_and_retry"
        )


# ─────────────────────────────────────────────────────────────────────────────
# HUMAN REVIEW COMPLETED
# ─────────────────────────────────────────────────────────────────────────────

async def handle_human_review_completed(
    store: EventStore,
    application_id: str,
    reviewer_id: str,
    final_decision: str,  # "APPROVED", "DECLINED", "REFERRED"
    override: bool = False,
    override_reason: Optional[str] = None,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Record human reviewer final decision.
    
    Preconditions:
    - Decision must be generated
    - If override=True, override_reason is required
    """
    if override and not override_reason:
        raise CommandHandlerError(
            "MissingOverrideReason",
            "override_reason is required when override=True",
            "provide_override_reason"
        )
    
    app = await LoanApplicationAggregate.load(store, application_id)
    
    try:
        app.assert_decision_generated()
    except LoanDomainError as e:
        raise CommandHandlerError(
            "ValidationFailed",
            str(e),
            "generate_decision_first"
        )
    
    event = BaseEvent(
        type="HumanReviewCompleted",
        version=1,
        payload={
            "application_id": application_id,
            "reviewer_id": reviewer_id,
            "override": override,
            "final_decision": final_decision,
            "override_reason": override_reason,
            "reviewed_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=f"loan-{application_id}",
            events=[event],
            expected_version=app.version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
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
        await store.append(
            stream_id=f"loan-{application_id}",
            events=[final_event],
            expected_version=version,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        
        return {
            "success": True,
            "application_id": application_id,
            "final_decision": final_decision,
            "override": override
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Another agent updated this application",
            "reload_and_retry"
        )


# ─────────────────────────────────────────────────────────────────────────────
# START AGENT SESSION
# ─────────────────────────────────────────────────────────────────────────────

async def handle_start_agent_session(
    store: EventStore,
    agent_id: str,
    session_id: str,
    model_version: str,
    context_source: str,
    context_token_count: int,
    correlation_id: Optional[str] = None,
    causation_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Initialize agent session context (Gas Town pattern).
    
    Preconditions:
    - Session must not already exist
    - ContextLoaded must be first event in session stream
    """
    stream_id = f"agent-{agent_id}-{session_id}"
    
    # Check if session already exists
    try:
        version = await store.stream_version(stream_id)
        if version > 0:
            raise CommandHandlerError(
                "SessionAlreadyInitialized",
                f"Session {session_id} already has events",
                "use_existing_session_or_create_new"
            )
    except Exception:
        pass  # New session, continue
    
    event = BaseEvent(
        type="AgentContextLoaded",
        version=1,
        payload={
            "agent_id": agent_id,
            "session_id": session_id,
            "model_version": model_version,
            "context_source": context_source,
            "context_token_count": context_token_count,
            "loaded_at": datetime.utcnow().isoformat()
        }
    )
    
    try:
        version = await store.append(
            stream_id=stream_id,
            events=[event],
            expected_version=-1,
            correlation_id=correlation_id,
            causation_id=causation_id
        )
        return {
            "success": True,
            "agent_id": agent_id,
            "session_id": session_id,
            "stream_version": version
        }
    except OptimisticConcurrencyError as e:
        raise CommandHandlerError(
            "ConcurrencyConflict",
            "Session already initialized",
            "retry_with_new_session_id"
        )
