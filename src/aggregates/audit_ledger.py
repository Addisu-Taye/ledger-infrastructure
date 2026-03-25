"""
AuditLedger Aggregate — Cross-Stream Causal Audit Trail

Maintains append-only audit trail linking events across all aggregates
for a single business entity. Enforces cryptographic hash chain integrity.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
import hashlib
import json
from src.events import StoredEvent
from src.store.event_store import EventStore, OptimisticConcurrencyError


@dataclass
class AuditEvent:
    event_id: str
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    correlation_id: Optional[str]
    causation_id: Optional[str]
    recorded_at: datetime
    payload_hash: str


class AuditLedgerAggregate:
    """
    Aggregate boundary: All audit events for a single entity.
    
    Invariants:
    - Append-only: no events may be removed
    - Hash chain: each integrity check links to previous hash
    - Causal ordering: correlation_id chains must be intact
    """
    
    def __init__(self, entity_type: str, entity_id: str):
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.stream_id = f"audit-{entity_type}-{entity_id}"
        self.version = 0
        self.events: List[AuditEvent] = []
        self.last_integrity_hash: Optional[str] = None
        self.last_integrity_check_at: Optional[datetime] = None
        self.events_verified_count: int = 0
    
    @classmethod
    async def load(cls, store: EventStore, entity_type: str, entity_id: str) -> "AuditLedgerAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(entity_type=entity_type, entity_id=entity_id)
        events = await store.load_stream(f"audit-{entity_type}-{entity_id}")
        for event in events:
            agg._apply(event)
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply event to aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_AuditEventRecorded(self, event: StoredEvent) -> None:
        payload = event.payload
        audit_event = AuditEvent(
            event_id=payload.get("event_id"),
            stream_id=payload.get("stream_id"),
            stream_position=payload.get("stream_position"),
            global_position=payload.get("global_position"),
            event_type=payload.get("event_type"),
            correlation_id=payload.get("correlation_id"),
            causation_id=payload.get("causation_id"),
            recorded_at=event.recorded_at,
            payload_hash=payload.get("payload_hash")
        )
        self.events.append(audit_event)
    
    def _on_AuditIntegrityCheckRun(self, event: StoredEvent) -> None:
        payload = event.payload
        self.last_integrity_hash = payload.get("integrity_hash")
        self.last_integrity_check_at = datetime.fromisoformat(payload.get("check_timestamp"))
        self.events_verified_count = payload.get("events_verified_count", 0)
    
    # ─── Hash Chain Methods ───────────────────────────────────────────────────
    
    def _compute_event_hash(self, event: AuditEvent) -> str:
        """Compute SHA-256 hash of a single audit event."""
        data = {
            "event_id": event.event_id,
            "stream_id": event.stream_id,
            "stream_position": event.stream_position,
            "event_type": event.event_type,
            "payload_hash": event.payload_hash,
            "recorded_at": event.recorded_at.isoformat()
        }
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()
    
    def _compute_chain_hash(self, previous_hash: Optional[str] = None) -> str:
        """Compute cumulative hash chain."""
        current_hash = previous_hash or "genesis"
        for event in self.events:
            event_hash = self._compute_event_hash(event)
            chain_data = {
                "previous_hash": current_hash,
                "event_hash": event_hash
            }
            current_hash = hashlib.sha256(json.dumps(chain_data, sort_keys=True).encode()).hexdigest()
        return current_hash
    
    def verify_integrity(self) -> Dict[str, Any]:
        """Verify hash chain integrity."""
        computed_hash = self._compute_chain_hash()
        
        if self.last_integrity_hash is None:
            return {
                "chain_valid": True,
                "tamper_detected": False,
                "events_verified": len(self.events),
                "computed_hash": computed_hash
            }
        
        # Compare with last recorded integrity hash
        tamper_detected = computed_hash != self.last_integrity_hash
        
        return {
            "chain_valid": not tamper_detected,
            "tamper_detected": tamper_detected,
            "events_verified": len(self.events),
            "computed_hash": computed_hash,
            "expected_hash": self.last_integrity_hash
        }
    
    # ─── Command Methods ──────────────────────────────────────────────────────
    
    def record_event(self, event: StoredEvent, correlation_id: Optional[str] = None, 
                    causation_id: Optional[str] = None) -> dict:
        """Record an event to the audit ledger."""
        payload_hash = hashlib.sha256(
            json.dumps(event.payload, sort_keys=True).encode()
        ).hexdigest()
        
        return {
            "event_type": "AuditEventRecorded",
            "version": 1,
            "payload": {
                "entity_type": self.entity_type,
                "entity_id": self.entity_id,
                "event_id": event.event_id,
                "stream_id": event.stream_id,
                "stream_position": event.stream_position,
                "global_position": event.global_position,
                "event_type": event.event_type,
                "correlation_id": correlation_id,
                "causation_id": causation_id,
                "payload_hash": payload_hash,
                "recorded_at": event.recorded_at.isoformat()
            }
        }
    
    def run_integrity_check(self) -> dict:
        """Run cryptographic integrity verification."""
        result = self.verify_integrity()
        current_hash = self._compute_chain_hash()
        
        return {
            "event_type": "AuditIntegrityCheckRun",
            "version": 1,
            "payload": {
                "entity_type": self.entity_type,
                "entity_id": self.entity_id,
                "check_timestamp": datetime.utcnow().isoformat(),
                "events_verified_count": len(self.events),
                "integrity_hash": current_hash,
                "previous_hash": self.last_integrity_hash,
                "chain_valid": result["chain_valid"],
                "tamper_detected": result["tamper_detected"]
            }
        }
    
    def get_audit_trail(self, from_position: int = 0, to_position: Optional[int] = None) -> List[Dict]:
        """Get audit trail for a range of events."""
        events = self.events[from_position:to_position] if to_position else self.events[from_position:]
        return [
            {
                "event_id": e.event_id,
                "stream_id": e.stream_id,
                "stream_position": e.stream_position,
                "event_type": e.event_type,
                "correlation_id": e.correlation_id,
                "causation_id": e.causation_id,
                "recorded_at": e.recorded_at.isoformat()
            }
            for e in events
        ]
    
    def get_causal_chain(self, event_id: str) -> List[str]:
        """Trace causal chain back from an event."""
        target = next((e for e in self.events if e.event_id == event_id), None)
        if not target:
            return []
        
        chain = [event_id]
        current_causation = target.causation_id
        
        while current_causation:
            chain.append(current_causation)
            parent = next((e for e in self.events if e.event_id == current_causation), None)
            if parent:
                current_causation = parent.causation_id
            else:
                break
        
        return list(reversed(chain))


class DomainError(Exception):
    """Raised when a business rule invariant is violated."""
    pass
