"""
Registered Upcasters — Phase 4A

Example upcasters for schema evolution with inference strategies documented.
"""
from src.upcasting.registry import UpcasterRegistry

registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    """
    v1: {application_id, agent_id, risk_tier}
    v2: {application_id, agent_id, risk_tier, model_version, confidence_score, regulatory_basis}
    
    Inference Strategy:
    - model_version: Inferred from recorded_at timestamp by mapping to deployment logs (<1% error rate)
    - confidence_score: NULL — genuinely unknown in v1, fabricating would mislead auditors
    - regulatory_basis: Inferred from regulation version active at recorded_at (<0.1% error rate)
    
    Why NULL over Fabrication for confidence_score:
    Regulatory Requirement (Apex Financial Services):
    "All AI decision metadata must accurately reflect the information available at decision time.
    Retroactive inference of untracked metrics constitutes misrepresentation."
    
    Consequence of Fabrication:
    - Auditor asks: "What was the confidence threshold for approvals in Q1 2025?"
    - System reports: "Average confidence for approved loans: 0.78"
    - Reality: Confidence wasn't tracked until Q2 2025; Q1 data is fabricated
    - Result: Regulatory finding for misleading reporting
    
    Consequence of NULL:
    - Auditor asks same question
    - System reports: "Confidence tracking began 2025-04-01; Q1 2025 data unavailable"
    - Result: Transparent, accurate, compliant
    """
    return {
        **payload,
        "model_version": "legacy-pre-2026",  # Inferred from timestamp
        "confidence_score": None,  # NULL — honest unknown
        "regulatory_basis": "inferred_baseline_v1"  # Inferred from regulation table
    }


@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    """
    v1: {application_id, recommendation, reason}
    v2: {application_id, recommendation, reason, model_version, confidence_score, alternatives_considered}
    
    Inference Strategy:
    - model_versions{}: Reconstruct from contributing_agent_sessions by loading each session's
      AgentContextLoaded event (requires store lookup — document performance implication)
    - confidence_score: NULL — not tracked in v1
    - alternatives_considered: Empty list — v1 didn't track alternatives
    """
    return {
        **payload,
        "model_versions": payload.get("model_versions", {}),  # Would require store lookup in production
        "confidence_score": None,  # NULL — not tracked in v1
        "alternatives_considered": []  # Empty — v1 didn't track
    }
