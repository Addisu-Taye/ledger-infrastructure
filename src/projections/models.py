from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List

@dataclass
class ApplicationSummary:
    """Projection 1: Read-optimised view of loan applications."""
    application_id: str
    state: str = "SUBMITTED"
    applicant_id: Optional[str] = None
    requested_amount_usd: Optional[float] = None
    approved_amount_usd: Optional[float] = None
    risk_tier: Optional[str] = None
    fraud_score: Optional[float] = None
    compliance_status: str = "PENDING"
    decision: Optional[str] = None
    agent_sessions_completed: List[str] = field(default_factory=list)
    last_event_type: Optional[str] = None
    last_event_at: Optional[datetime] = None
    human_reviewer_id: Optional[str] = None
    final_decision_at: Optional[datetime] = None
    projection_lag_ms: int = 0

@dataclass
class AgentPerformanceMetrics:
    """Projection 2: Aggregated performance per agent model version."""
    agent_id: str
    model_version: str
    analyses_completed: int = 0
    decisions_generated: int = 0
    avg_confidence_score: Optional[float] = None
    avg_duration_ms: Optional[float] = None
    approve_rate: float = 0.0
    decline_rate: float = 0.0
    refer_rate: float = 0.0
    human_override_rate: float = 0.0
    first_seen_at: Optional[datetime] = None
    last_seen_at: Optional[datetime] = None

@dataclass
class ComplianceCheck:
    """Single compliance check record for temporal queries."""
    rule_id: str
    rule_version: str
    status: str  # "PASSED" or "FAILED"
    evaluation_timestamp: datetime
    evidence_hash: Optional[str] = None
    failure_reason: Optional[str] = None
    remediation_required: bool = False

@dataclass
class ComplianceAuditView:
    """Projection 3: Regulatory read model with temporal query support."""
    application_id: str
    regulation_set_version: str
    checks: List[ComplianceCheck] = field(default_factory=list)
    overall_status: str = "PENDING"
    last_updated_at: Optional[datetime] = None
    projection_lag_ms: int = 0
    snapshot_position: Optional[int] = None  # For temporal query optimization
    
    def get_state_at(self, timestamp: datetime) -> "ComplianceAuditView":
        """Return compliance state as it existed at a specific timestamp."""
        filtered = ComplianceAuditView(
            application_id=self.application_id,
            regulation_set_version=self.regulation_set_version,
            checks=[c for c in self.checks if c.evaluation_timestamp <= timestamp],
            overall_status=self._compute_status_at(timestamp),
            last_updated_at=timestamp,
            projection_lag_ms=self.projection_lag_ms,
            snapshot_position=self.snapshot_position
        )
        return filtered
    
    def _compute_status_at(self, timestamp: datetime) -> str:
        """Compute overall compliance status at a given timestamp."""
        checks_at_time = [c for c in self.checks if c.evaluation_timestamp <= timestamp]
        if not checks_at_time:
            return "PENDING"
        if any(c.status == "FAILED" and c.remediation_required for c in checks_at_time):
            return "FAILED"
        if all(c.status == "PASSED" for c in checks_at_time):
            return "COMPLIANT"
        return "IN_PROGRESS"
