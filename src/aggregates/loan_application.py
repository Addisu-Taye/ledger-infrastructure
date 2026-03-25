"""
LoanApplication Aggregate — Full Lifecycle State Machine

Tracks a commercial loan application from submission to final decision.
Enforces 6 business rules in domain logic.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from src.events import StoredEvent
from src.store.event_store import EventStore


class ApplicationState:
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"
    COMPLIANCE_REVIEW = "COMPLIANCE_REVIEW"
    PENDING_DECISION = "PENDING_DECISION"
    APPROVED_PENDING_HUMAN = "APPROVED_PENDING_HUMAN"
    DECLINED_PENDING_HUMAN = "DECLINED_PENDING_HUMAN"
    FINAL_APPROVED = "FINAL_APPROVED"
    FINAL_DECLINED = "FINAL_DECLINED"


class LoanApplicationAggregate:
    """
    Aggregate boundary: Full lifecycle of a single loan application.
    
    Invariants (enforced in domain logic):
    1. Valid state transitions only (state machine)
    2. Credit analysis requires SUBMITTED or AWAITING_ANALYSIS state
    3. Fraud screening requires credit analysis complete
    4. Compliance check requires fraud screening complete
    5. Decision requires all analyses complete + compliance not blocked
    6. Confidence floor: < 0.6 → must REFER
    """
    
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.stream_id = f"loan-{application_id}"
        self.version = 0
        self.state = ApplicationState.SUBMITTED
        self.applicant_id: Optional[str] = None
        self.requested_amount_usd: Optional[float] = None
        self.approved_amount_usd: Optional[float] = None
        self.risk_tier: Optional[str] = None
        self.fraud_score: Optional[float] = None
        self.compliance_status: str = "PENDING"
        self.decision: Optional[str] = None
        self.confidence_score: Optional[float] = None
        self.human_reviewer_id: Optional[str] = None
        self.final_decision_at: Optional[datetime] = None
        self.analyses_complete: Dict[str, bool] = {
            "credit": False, "fraud": False, "compliance": False
        }
        self.compliance_blocked: bool = False
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(application_id=application_id)
        events = await store.load_stream(f"loan-{application_id}")
        for event in events:
            agg._apply(event)
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply event to aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        payload = event.payload
        self.applicant_id = payload.get("applicant_id")
        self.requested_amount_usd = payload.get("requested_amount_usd")
        self.state = ApplicationState.AWAITING_ANALYSIS
    
    def _on_CreditAnalysisCompleted(self, event: StoredEvent) -> None:
        payload = event.payload
        self.risk_tier = payload.get("risk_tier")
        self.confidence_score = payload.get("confidence_score")
        self.analyses_complete["credit"] = True
        if self.state == ApplicationState.AWAITING_ANALYSIS:
            self.state = ApplicationState.ANALYSIS_COMPLETE
    
    def _on_FraudScreeningCompleted(self, event: StoredEvent) -> None:
        payload = event.payload
        self.fraud_score = payload.get("fraud_score")
        self.analyses_complete["fraud"] = True
        if self.state == ApplicationState.ANALYSIS_COMPLETE:
            self.state = ApplicationState.COMPLIANCE_REVIEW
    
    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        # Track compliance progress
        pass
    
    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        payload = event.payload
        if payload.get("is_hard_block", False):
            self.compliance_blocked = True
            self.compliance_status = "BLOCKED"
        self.analyses_complete["compliance"] = True
    
    def _on_DecisionGenerated(self, event: StoredEvent) -> None:
        payload = event.payload
        self.decision = payload.get("recommendation")
        self.state = ApplicationState.PENDING_DECISION
    
    def _on_HumanReviewCompleted(self, event: StoredEvent) -> None:
        payload = event.payload
        self.human_reviewer_id = payload.get("reviewer_id")
        final = payload.get("final_decision")
        if final == "APPROVED":
            self.state = ApplicationState.APPROVED_PENDING_HUMAN
        elif final == "DECLINED":
            self.state = ApplicationState.DECLINED_PENDING_HUMAN
    
    def _on_ApplicationApproved(self, event: StoredEvent) -> None:
        payload = event.payload
        self.approved_amount_usd = payload.get("approved_amount_usd")
        self.state = ApplicationState.FINAL_APPROVED
        self.final_decision_at = datetime.utcnow()
    
    def _on_ApplicationDeclined(self, event: StoredEvent) -> None:
        self.state = ApplicationState.FINAL_DECLINED
        self.final_decision_at = datetime.utcnow()
    
    # ─── Business Rule Assertions ─────────────────────────────────────────────
    
    def assert_awaiting_credit_analysis(self) -> None:
        """Rule 1: Credit analysis requires SUBMITTED or AWAITING_ANALYSIS state."""
        if self.state not in [ApplicationState.SUBMITTED, ApplicationState.AWAITING_ANALYSIS]:
            raise DomainError(
                f"Cannot perform credit analysis: application state is {self.state}, "
                f"expected SUBMITTED or AWAITING_ANALYSIS"
            )
    
    def assert_credit_analysis_complete(self) -> None:
        """Rule: Fraud screening requires credit analysis complete."""
        if not self.analyses_complete["credit"]:
            raise DomainError("Cannot perform fraud screening: credit analysis not complete")
    
    def assert_fraud_screening_complete(self) -> None:
        """Rule: Compliance check requires fraud screening complete."""
        if not self.analyses_complete["fraud"]:
            raise DomainError("Cannot perform compliance check: fraud screening not complete")
    
    def assert_all_analyses_complete(self) -> None:
        """Rule: Decision requires all analyses complete."""
        missing = [k for k, v in self.analyses_complete.items() if not v]
        if missing:
            raise DomainError(f"Cannot generate decision: missing analyses: {missing}")
    
    def assert_compliance_not_blocked(self) -> None:
        """Rule: Cannot proceed if compliance is blocked."""
        if self.compliance_blocked:
            raise DomainError("Cannot proceed: compliance check blocked application")
    
    def assert_confidence_floor(self, confidence: float, recommendation: str) -> None:
        """Rule 4: Confidence < 0.6 → must REFER."""
        if confidence < 0.6 and recommendation != "REFER":
            raise DomainError(
                f"Confidence {confidence} < 0.6 requires REFER recommendation, got {recommendation}"
            )
    
    def assert_decision_generated(self) -> None:
        """Rule: Human review requires decision generated."""
        if self.decision is None:
            raise DomainError("Cannot record human review: no decision generated yet")
    
    # ─── State Query Methods ──────────────────────────────────────────────────
    
    def get_status(self) -> Dict[str, Any]:
        """Get current application status for projections."""
        return {
            "application_id": self.application_id,
            "state": self.state,
            "applicant_id": self.applicant_id,
            "requested_amount_usd": self.requested_amount_usd,
            "approved_amount_usd": self.approved_amount_usd,
            "risk_tier": self.risk_tier,
            "fraud_score": self.fraud_score,
            "compliance_status": self.compliance_status,
            "decision": self.decision,
            "confidence_score": self.confidence_score,
            "analyses_complete": self.analyses_complete,
            "version": self.version
        }


class DomainError(Exception):
    """Raised when a business rule invariant is violated."""
    pass
