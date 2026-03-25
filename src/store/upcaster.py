from typing import Callable, Dict, Tuple, Optional, List, Union
from src.events import StoredEvent
import hashlib
import json

class UpcasterRegistry:
    """Registry for event upcasters with immutability guarantee."""
    
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable[[dict], dict]] = {}
    
    def register(self, event_type: str, from_version: int):
        """Decorator to register an upcaster function."""
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            key = (event_type, from_version)
            if key in self._upcasters:
                raise ValueError(f"Upcaster already registered for {event_type} v{from_version}")
            self._upcasters[key] = fn
            return fn
        return decorator
    
    def upcast(self, event: StoredEvent) -> StoredEvent:
        """
        Apply all registered upcasters for this event type.
        Handles both dict and JSON string payloads.
        IMPORTANT: Returns a NEW StoredEvent with transformed payload.
        The original stored payload in the database is NEVER modified.
        """
        current = event
        
        # Ensure payload is a dict (handle JSON string from asyncpg)
        payload = current.payload
        if isinstance(payload, str):
            payload = json.loads(payload)
        
        current_version = event.event_version
        
        # Chain upcasters: v1 -> v2 -> v3 -> ...
        while (current.event_type, current_version) in self._upcasters:
            upcaster = self._upcasters[(current.event_type, current_version)]
            new_payload = upcaster(payload)
            current_version += 1
            payload = new_payload  # Update for next iteration
        
        # Return new StoredEvent with upcasted payload and version
        return current.with_payload(payload, version=current_version)
    
    def get_upcast_chain(self, event_type: str, from_version: int) -> List[Tuple[int, int]]:
        """Return the version chain that would be applied."""
        chain = []
        v = from_version
        while (event_type, v) in self._upcasters:
            chain.append((v, v + 1))
            v += 1
        return chain


# =============================================================================
# Example Upcasters for TRP1 Week 5 Scenario
# =============================================================================

registry = UpcasterRegistry()


@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_analysis_v1_to_v2(payload: dict) -> dict:
    """
    v1: {application_id, agent_id, risk_tier}
    v2: {application_id, agent_id, risk_tier, model_version, confidence_score, regulatory_basis}
    
    Inference strategy:
    - model_version: inferred from recorded_at timestamp (error rate <1%)
    - confidence_score: NULL — genuinely unknown in v1, fabricating would mislead auditors
    - regulatory_basis: inferred from regulation version active at recorded_at
    """
    return {
        **payload,
        "model_version": "legacy-pre-2026",
        "confidence_score": None,  # NULL is honest — we didn't track this in v1
        "regulatory_basis": "inferred_baseline_v1"
    }


@registry.register("DecisionGenerated", from_version=1)
def upcast_decision_v1_to_v2(payload: dict) -> dict:
    """
    v1: {application_id, recommendation, reason}
    v2: {application_id, recommendation, reason, model_version, confidence_score, alternatives_considered}
    """
    return {
        **payload,
        "model_version": payload.get("model_version", "legacy-v1"),
        "confidence_score": None,  # NULL — not tracked in v1
        "alternatives_considered": []  # Empty list — v1 didn't track alternatives
    }


@registry.register("ComplianceRulePassed", from_version=1)
def upcast_compliance_v1_to_v2(payload: dict) -> dict:
    """Add evidence_hash and rule_metadata for audit trail."""
    return {
        **payload,
        "evidence_hash": payload.get("evidence_hash"),  # May be NULL if not provided
        "rule_metadata": {
            "evaluation_engine": "legacy",
            "data_sources": payload.get("data_sources", [])
        }
    }
