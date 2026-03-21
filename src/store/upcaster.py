# src/store/upcaster.py
from typing import Callable, Dict, Tuple, Optional
from src.events import StoredEvent

class UpcasterRegistry:
    """Registry for event upcasters (Phase 4)."""
    
    def __init__(self):
        self._upcasters: Dict[Tuple[str, int], Callable] = {}
    
    def register(self, event_type: str, from_version: int):
        """Decorator to register an upcaster."""
        def decorator(fn: Callable[[dict], dict]) -> Callable:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator
    
    def upcast(self, event: StoredEvent) -> StoredEvent:
        """Apply all registered upcasters for this event type."""
        current = event
        v = event.event_version
        
        while (event.event_type, v) in self._upcasters:
            new_payload = self._upcasters[(event.event_type, v)](current.payload)
            current = current.with_payload(new_payload, version=v + 1)
            v += 1
        
        return current

# Global registry instance
registry = UpcasterRegistry()