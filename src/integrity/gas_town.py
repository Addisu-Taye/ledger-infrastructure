"""
Gas Town Agent Memory Pattern — Crash Recovery & Context Reconstruction

Prevents catastrophic memory loss when AI agents crash mid-session.
An agent that restarts can reconstruct its exact context from the event store
and continue where it left off without repeating completed work.

Required for Final Submission: Phase 4C
"""
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
from src.store.event_store import EventStore
from src.events import StoredEvent
import json


@dataclass
class AgentContext:
    """Reconstructed agent context for crash recovery."""
    agent_id: str
    session_id: str
    context_text: str  # Token-efficient summary for LLM context window
    last_event_position: int
    last_completed_action: Optional[str]
    pending_work: List[Dict[str, Any]]
    session_health_status: str  # "HEALTHY", "NEEDS_RECONCILIATION", "FAILED"
    needs_reconciliation: bool
    model_version: Optional[str]
    total_events: int
    token_count: int


class GasTownReconstructor:
    """
    Reconstructs agent context from event stream after crash.
    
    Key Features:
    - Token-efficient summarization of old events
    - Preserves verbatim: last 3 events, any PENDING/ERROR events
    - Detects partial decisions that need reconciliation
    - Enforces ContextLoaded as first event (Gas Town invariant)
    """
    
    def __init__(self, store: EventStore, token_budget: int = 8000):
        self.store = store
        self.token_budget = token_budget
        # Rough estimate: 1 token ≈ 4 characters
        self.chars_per_token = 4
    
    async def reconstruct_agent_context(
        self,
        agent_id: str,
        session_id: str,
        token_budget: Optional[int] = None,
    ) -> AgentContext:
        """
        Reconstruct agent context from event stream.
        
        CRITICAL: If the agent's last event was a partial decision
        (no corresponding completion event), flags context as NEEDS_RECONCILIATION.
        """
        budget = token_budget or self.token_budget
        stream_id = f"agent-{agent_id}-{session_id}"
        
        # Load full AgentSession stream
        events = await self.store.load_stream(stream_id)
        
        if not events:
            return AgentContext(
                agent_id=agent_id,
                session_id=session_id,
                context_text="",
                last_event_position=0,
                last_completed_action=None,
                pending_work=[],
                session_health_status="FAILED",
                needs_reconciliation=False,
                model_version=None,
                total_events=0,
                token_count=0
            )
        
        # Verify Gas Town invariant: ContextLoaded must be first event
        first_event = events[0]
        if first_event.event_type != "AgentContextLoaded":
            raise DomainError(
                f"Gas Town violation: First event must be AgentContextLoaded, got {first_event.event_type}"
            )
        
        # Extract model version from ContextLoaded
        model_version = first_event.payload.get("model_version")
        
        # Categorize events
        context_events = []  # Events to summarize
        verbatim_events = []  # Last 3 events + any PENDING/ERROR
        pending_actions = []
        last_completed_action = None
        needs_reconciliation = False
        
        for i, event in enumerate(events):
            is_recent = i >= len(events) - 3
            is_pending_or_error = event.event_type.endswith("Pending") or event.event_type.endswith("Error") or event.event_type.endswith("Failed")
            is_partial_decision = event.event_type in ["CreditAnalysisCompleted", "FraudScreeningCompleted", "DecisionGenerated"]
            
            # Check if this event has a corresponding completion
            if is_partial_decision:
                has_completion = any(
                    e.event_type in ["CreditAnalysisApproved", "FraudScreeningApproved", "DecisionApproved"]
                    and e.payload.get("application_id") == event.payload.get("application_id")
                    for e in events[i:]
                )
                if not has_completion:
                    needs_reconciliation = True
                    pending_actions.append({
                        "event_type": event.event_type,
                        "application_id": event.payload.get("application_id"),
                        "position": event.stream_position,
                        "action": "complete_or_rollback"
                    })
            
            if is_recent or is_pending_or_error:
                verbatim_events.append(event)
            else:
                context_events.append(event)
            
            # Track last completed action
            if event.event_type.endswith("Completed") or event.event_type.endswith("Approved") or event.event_type == "AgentOutputWritten":
                last_completed_action = event.event_type
        
        # Build token-efficient context summary
        context_text = self._build_context_summary(
            context_events=context_events,
            verbatim_events=verbatim_events,
            agent_id=agent_id,
            session_id=session_id,
            model_version=model_version,
            budget=budget
        )
        
        # Determine session health
        if needs_reconciliation:
            health_status = "NEEDS_RECONCILIATION"
        elif any(e.event_type.endswith("Failed") for e in events):
            health_status = "FAILED"
        else:
            health_status = "HEALTHY"
        
        # Estimate token count
        token_count = len(context_text) // self.chars_per_token
        
        return AgentContext(
            agent_id=agent_id,
            session_id=session_id,
            context_text=context_text,
            last_event_position=events[-1].stream_position if events else 0,
            last_completed_action=last_completed_action,
            pending_work=pending_actions,
            session_health_status=health_status,
            needs_reconciliation=needs_reconciliation,
            model_version=model_version,
            total_events=len(events),
            token_count=token_count
        )
    
    def _build_context_summary(
        self,
        context_events: List[StoredEvent],
        verbatim_events: List[StoredEvent],
        agent_id: str,
        session_id: str,
        model_version: Optional[str],
        budget: int,
    ) -> str:
        """Build token-efficient context summary."""
        lines = []
        remaining_chars = budget * self.chars_per_token
        
        # Header
        header = f"""=== AGENT SESSION CONTEXT ===
Agent: {agent_id}
Session: {session_id}
Model: {model_version or 'unknown'}
Total Events: {len(context_events) + len(verbatim_events)}
"""
        lines.append(header)
        remaining_chars -= len(header)
        
        # Summarize old events (token-efficient)
        if context_events:
            # Group by event type for efficient summarization
            event_counts: Dict[str, int] = {}
            for event in context_events:
                event_type = event.event_type
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
            
            summary = "\n=== HISTORICAL SUMMARY ===\n"
            for event_type, count in event_counts.items():
                line = f"  {event_type}: {count} events\n"
                if remaining_chars > len(line):
                    summary += line
                    remaining_chars -= len(line)
            lines.append(summary)
        
        # Recent events verbatim (critical for continuity)
        if verbatim_events:
            verbatim = "\n=== RECENT EVENTS (VERBATIM) ===\n"
            lines.append(verbatim)
            remaining_chars -= len(verbatim)
            
            for event in verbatim_events:
                event_summary = f"[{event.event_type}] {json.dumps(event.payload, default=str)[:200]}\n"
                if remaining_chars > len(event_summary):
                    lines.append(event_summary)
                    remaining_chars -= len(event_summary)
        
        # Pending work items
        pending = "\n=== PENDING WORK ===\n"
        lines.append(pending)
        
        return "".join(lines)


class DomainError(Exception):
    """Raised when a business rule invariant is violated."""
    pass


# Convenience function for direct use
async def reconstruct_agent_context(
    store: EventStore,
    agent_id: str,
    session_id: str,
    token_budget: int = 8000,
) -> AgentContext:
    """
    Reconstruct agent context from event stream after crash.
    
    Usage:
        context = await reconstruct_agent_context(store, "agent-001", "session-abc")
        if context.needs_reconciliation:
            # Handle partial state before proceeding
            pass
        # Agent can now continue with context.context_text in its context window
    """
    reconstructor = GasTownReconstructor(store, token_budget)
    return await reconstructor.reconstruct_agent_context(agent_id, session_id, token_budget)
