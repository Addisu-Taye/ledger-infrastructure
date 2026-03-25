"""
AgentSession Aggregate — Gas Town Pattern Implementation

Tracks all actions taken by a specific AI agent instance during a work session.
Enforces: ContextLoaded must be first event; model version tracking; token budget.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Any
from src.events import StoredEvent
from src.store.event_store import EventStore


class AgentSessionState:
    INITIALIZED = "INITIALIZED"
    ACTIVE = "ACTIVE"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass
class AgentContext:
    context_source: str
    event_replay_from_position: int
    context_token_count: int
    model_version: str
    loaded_at: datetime


class AgentSessionAggregate:
    """
    Aggregate boundary: All actions for a single agent session.
    
    Invariants:
    - AgentContextLoaded must be first event (Gas Town pattern)
    - No decision event may be appended before context is loaded
    - Model version is immutable once set
    """
    
    def __init__(self, agent_id: str, session_id: str):
        self.agent_id = agent_id
        self.session_id = session_id
        self.stream_id = f"agent-{agent_id}-{session_id}"
        self.version = 0
        self.state = AgentSessionState.INITIALIZED
        self.context: Optional[AgentContext] = None
        self.model_version: Optional[str] = None
        self.actions: List[Dict] = []
        self.last_action: Optional[str] = None
    
    @classmethod
    async def load(cls, store: EventStore, agent_id: str, session_id: str) -> "AgentSessionAggregate":
        """Reconstruct aggregate state by replaying event stream."""
        agg = cls(agent_id=agent_id, session_id=session_id)
        events = await store.load_stream(f"agent-{agent_id}-{session_id}")
        for event in events:
            agg._apply(event)
        return agg
    
    def _apply(self, event: StoredEvent) -> None:
        """Apply event to aggregate state."""
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position
    
    def _on_AgentContextLoaded(self, event: StoredEvent) -> None:
        payload = event.payload
        self.context = AgentContext(
            context_source=payload.get("context_source"),
            event_replay_from_position=payload.get("event_replay_from_position", 0),
            context_token_count=payload.get("context_token_count", 0),
            model_version=payload.get("model_version"),
            loaded_at=event.recorded_at
        )
        self.model_version = payload.get("model_version")
        self.state = AgentSessionState.ACTIVE
    
    def _on_AgentNodeExecuted(self, event: StoredEvent) -> None:
        payload = event.payload
        self.actions.append({
            "node_name": payload.get("node_name"),
            "sequence": payload.get("node_sequence"),
            "duration_ms": payload.get("duration_ms"),
            "executed_at": event.recorded_at
        })
        self.last_action = payload.get("node_name")
    
    def _on_AgentOutputWritten(self, event: StoredEvent) -> None:
        self.state = AgentSessionState.COMPLETED
    
    def _on_AgentSessionFailed(self, event: StoredEvent) -> None:
        self.state = AgentSessionState.FAILED
    
    # ─── Business Rule Assertions ─────────────────────────────────────────────
    
    def assert_context_loaded(self) -> None:
        """Rule: No action may be taken before context is loaded."""
        if self.context is None:
            raise DomainError(
                f"Agent {self.agent_id} session {self.session_id} has no context loaded. "
                "Call start_agent_session first (Gas Town pattern)."
            )
    
    def assert_model_version_current(self, requested_version: str) -> None:
        """Rule: Agent must use current model version."""
        if self.model_version and self.model_version != requested_version:
            raise DomainError(
                f"Model version mismatch: session has {self.model_version}, requested {requested_version}"
            )
    
    def assert_active(self) -> None:
        """Rule: Actions can only be taken in ACTIVE state."""
        if self.state != AgentSessionState.ACTIVE:
            raise DomainError(f"Session is {self.state}, not ACTIVE")
    
    # ─── Command Methods ──────────────────────────────────────────────────────
    
    def load_context(self, context_source: str, token_count: int, model_version: str) -> dict:
        """Initialize agent session context (must be first event)."""
        if self.version > 0:
            raise DomainError("ContextLoaded must be first event in session stream")
        
        return {
            "event_type": "AgentContextLoaded",
            "version": 1,
            "payload": {
                "agent_id": self.agent_id,
                "session_id": self.session_id,
                "context_source": context_source,
                "context_token_count": token_count,
                "model_version": model_version,
                "loaded_at": datetime.utcnow().isoformat()
            }
        }
    
    def record_node_execution(self, node_name: str, sequence: int, duration_ms: int,
                           llm_called: bool = False, tokens_in: int = 0, 
                           tokens_out: int = 0, cost: float = 0.0) -> dict:
        """Record a node execution event."""
        self.assert_context_loaded()
        self.assert_active()
        
        return {
            "event_type": "AgentNodeExecuted",
            "version": 1,
            "payload": {
                "agent_id": self.agent_id,
                "session_id": self.session_id,
                "node_name": node_name,
                "node_sequence": sequence,
                "duration_ms": duration_ms,
                "llm_called": llm_called,
                "llm_tokens_input": tokens_in if llm_called else None,
                "llm_tokens_output": tokens_out if llm_called else None,
                "llm_cost_usd": cost if llm_called else None,
                "executed_at": datetime.utcnow().isoformat()
            }
        }
    
    def record_output_written(self, events_written: List[Dict], summary: str) -> dict:
        """Record output written event."""
        self.assert_context_loaded()
        
        return {
            "event_type": "AgentOutputWritten",
            "version": 1,
            "payload": {
                "agent_id": self.agent_id,
                "session_id": self.session_id,
                "events_written": events_written,
                "output_summary": summary,
                "written_at": datetime.utcnow().isoformat()
            }
        }


class DomainError(Exception):
    """Raised when a business rule invariant is violated."""
    pass
