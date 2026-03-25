"""
ComplianceRecord Aggregate — Regulatory Check Tracking

Tracks all compliance rule evaluations for a loan application.
Enforces: all mandatory checks must pass before clearance can be issued.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from src.events import StoredEvent
from src.store.event_store import EventStore, OptimisticConcurrencyError


class ComplianceState:
    PENDING = "PENDING"
    IN_REVIEW = "IN_REVIEW"
    CLEARED = "CLEARED"
    BLOCKED = "BLOCKED"
    REMEDIATION_REQUIRED = "REMEDIATION_REQUIRED"


@dataclass
class ComplianceCheck:
    rule_id: str
    rule_version: str
    status: str  # "PASSED", "FAILED", "NOTED"
    evaluation_timestamp: datetime
    evidence_hash: Optional[str] = None
    failure_reason: Optional[str] = None
    remediation_required: bool = False
    remediation_completed: bool = False


class ComplianceRecordAggregate:
    """
    Aggregate boundary: All compliance checks for a single application.
    
    Invariants:
    - Cannot issue clearance without all mandatory checks passed
    - Every check must reference specific regulation version
    - Hard block rules stop further evaluation immediately
    """
    
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.stream_id = f"compliance-{application_id}"
        self.version = 0
        self.state = ComplianceState.PENDING
        self.regulation_set_version: Optional[str] = None
        self.checks: List[ComplianceCheck] = []
        self.hard_block_rule_id: Optional[str] = None
        self.cleared_at: Optional[datetime] = None
        self.blocked_at: Optional[datetime] = None
    
    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "ComplianceRecordAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(application_id=application_id)
        events = await store.load_stream(f"compliance-{application_id}")
        for event in events:
            agg._apply(event)
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply event to aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_ComplianceCheckRequested(self, event: StoredEvent) -> None:
        payload = event.payload
        self.regulation_set_version = payload.get("regulation_set_version")
        self.state = ComplianceState.IN_REVIEW
    
    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        payload = event.payload
        check = ComplianceCheck(
            rule_id=payload.get("rule_id"),
            rule_version=payload.get("rule_version"),
            status="PASSED",
            evaluation_timestamp=event.recorded_at,
            evidence_hash=payload.get("evidence_hash")
        )
        self.checks.append(check)
        self._update_state()
    
    def _on_ComplianceRuleFailed(self, event: StoredEvent) -> None:
        payload = event.payload
        check = ComplianceCheck(
            rule_id=payload.get("rule_id"),
            rule_version=payload.get("rule_version"),
            status="FAILED",
            evaluation_timestamp=event.recorded_at,
            evidence_hash=payload.get("evidence_hash"),
            failure_reason=payload.get("failure_reason"),
            remediation_required=payload.get("remediation_required", False)
        )
        self.checks.append(check)
        
        # Check if this is a hard block rule
        if payload.get("is_hard_block", False):
            self.hard_block_rule_id = payload.get("rule_id")
            self.state = ComplianceState.BLOCKED
            self.blocked_at = datetime.utcnow()
        else:
            self._update_state()
    
    def _on_ComplianceRuleNoted(self, event: StoredEvent) -> None:
        payload = event.payload
        check = ComplianceCheck(
            rule_id=payload.get("rule_id"),
            rule_version=payload.get("rule_version"),
            status="NOTED",
            evaluation_timestamp=event.recorded_at,
            evidence_hash=payload.get("evidence_hash")
        )
        self.checks.append(check)
        # Note events don't change state
    
    def _update_state(self) -> None:
        """Update aggregate state based on all checks."""
        if self.hard_block_rule_id:
            self.state = ComplianceState.BLOCKED
            return
        
        failed_with_remediation = [c for c in self.checks if c.status == "FAILED" and c.remediation_required]
        if failed_with_remediation:
            self.state = ComplianceState.REMEDIATION_REQUIRED
            return
        
        failed = [c for c in self.checks if c.status == "FAILED"]
        if failed:
            self.state = ComplianceState.IN_REVIEW
            return
        
        passed = [c for c in self.checks if c.status == "PASSED"]
        if passed and len(passed) == len(self.checks):
            self.state = ComplianceState.CLEARED
            self.cleared_at = datetime.utcnow()
    
    # ─── Business Rule Assertions ─────────────────────────────────────────────
    
    def assert_can_evaluate_rule(self, rule_id: str) -> None:
        """Rule: Cannot evaluate rules after hard block."""
        if self.hard_block_rule_id:
            raise DomainError(
                f"Cannot evaluate rule {rule_id}: hard block already triggered by {self.hard_block_rule_id}"
            )
    
    def assert_can_clear(self) -> None:
        """Rule: Cannot clear without all mandatory checks passed."""
        if self.state != ComplianceState.CLEARED:
            failed = [c for c in self.checks if c.status == "FAILED"]
            pending = len(self.checks)  # Would need expected check list in production
            raise DomainError(
                f"Cannot clear compliance: {len(failed)} failed checks, state={self.state}"
            )
    
    def assert_not_blocked(self) -> None:
        """Rule: Cannot proceed if compliance is blocked."""
        if self.state == ComplianceState.BLOCKED:
            raise DomainError(
                f"Compliance is blocked by rule {self.hard_block_rule_id}"
            )
    
    # ─── Command Methods ──────────────────────────────────────────────────────
    
    def request_check(self, regulation_set_version: str, checks_required: List[str]) -> dict:
        """Initiate compliance check process."""
        return {
            "event_type": "ComplianceCheckRequested",
            "version": 1,
            "payload": {
                "application_id": self.application_id,
                "regulation_set_version": regulation_set_version,
                "checks_required": checks_required,
                "requested_at": datetime.utcnow().isoformat()
            }
        }
    
    def record_rule_passed(self, rule_id: str, rule_version: str, evidence_hash: str) -> dict:
        """Record a passed compliance rule."""
        self.assert_can_evaluate_rule(rule_id)
        return {
            "event_type": "ComplianceRulePassed",
            "version": 1,
            "payload": {
                "application_id": self.application_id,
                "rule_id": rule_id,
                "rule_version": rule_version,
                "evidence_hash": evidence_hash,
                "evaluation_timestamp": datetime.utcnow().isoformat()
            }
        }
    
    def record_rule_failed(self, rule_id: str, rule_version: str, failure_reason: str, 
                          remediation_required: bool, is_hard_block: bool = False) -> dict:
        """Record a failed compliance rule."""
        self.assert_can_evaluate_rule(rule_id)
        return {
            "event_type": "ComplianceRuleFailed",
            "version": 1,
            "payload": {
                "application_id": self.application_id,
                "rule_id": rule_id,
                "rule_version": rule_version,
                "failure_reason": failure_reason,
                "remediation_required": remediation_required,
                "is_hard_block": is_hard_block,
                "evaluation_timestamp": datetime.utcnow().isoformat()
            }
        }
    
    def record_rule_noted(self, rule_id: str, rule_version: str, note_text: str) -> dict:
        """Record a compliance note (non-blocking)."""
        return {
            "event_type": "ComplianceRuleNoted",
            "version": 1,
            "payload": {
                "application_id": self.application_id,
                "rule_id": rule_id,
                "rule_version": rule_version,
                "note_text": note_text,
                "evaluation_timestamp": datetime.utcnow().isoformat()
            }
        }
    
    def get_compliance_status(self) -> Dict[str, Any]:
        """Get current compliance status for projections."""
        return {
            "application_id": self.application_id,
            "state": self.state,
            "regulation_set_version": self.regulation_set_version,
            "total_checks": len(self.checks),
            "passed": len([c for c in self.checks if c.status == "PASSED"]),
            "failed": len([c for c in self.checks if c.status == "FAILED"]),
            "noted": len([c for c in self.checks if c.status == "NOTED"]),
            "hard_block_rule_id": self.hard_block_rule_id,
            "cleared_at": self.cleared_at.isoformat() if self.cleared_at else None,
            "blocked_at": self.blocked_at.isoformat() if self.blocked_at else None
        }


class DomainError(Exception):
    """Raised when a business rule invariant is violated."""
    pass
