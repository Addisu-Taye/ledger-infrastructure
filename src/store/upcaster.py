from typing import Callable, Dict, Tuple
from src.events import StoredEvent

class UpcasterRegistry:
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable] = {}

    def register(self, event_type: str, from_version: int):
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event: StoredEvent) -> StoredEvent:
        current = event
        v = event.event_version
        # Apply chain v1 -> v2 -> v3
        while (event.event_type, v) in self._upcasters:
            new_payload = self._upcasters[(event.event_type, v)](current.payload)
            # Create new StoredEvent with updated payload/version
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        return current

# Global Registry
registry = UpcasterRegistry()

@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_credit_v1_to_v2(payload: dict) -> dict:
    # Inference strategy: Null for unknown confidence, legacy for model
    return {
        **payload,
        "model_version": "legacy-pre-2026",
        "confidence_score": None, 
        "regulatory_basis": "inferred_baseline_v1"
    }