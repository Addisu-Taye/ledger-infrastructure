# src/events.py
from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
import uuid

@dataclass
class BaseEvent:
    """Base class for all domain events."""
    type: str
    version: int = 1
    payload: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.type:
            raise ValueError("Event type is required")

@dataclass
class StoredEvent:
    """Event as stored in the database with system fields."""
    event_id: str
    stream_id: str
    stream_position: int
    global_position: int
    event_type: str
    event_version: int
    payload: dict
    metadata: dict
    recorded_at: datetime
    
    def with_payload(self, new_payload: dict, version: int) -> "StoredEvent":
        """Create a new StoredEvent with updated payload (for upcasting)."""
        return StoredEvent(
            event_id=self.event_id,
            stream_id=self.stream_id,
            stream_position=self.stream_position,
            global_position=self.global_position,
            event_type=self.event_type,
            event_version=version,
            payload=new_payload,
            metadata=self.metadata,
            recorded_at=self.recorded_at
        )

@dataclass
class StreamMetadata:
    """Metadata about a stream."""
    stream_id: str
    aggregate_type: str
    current_version: int
    created_at: datetime
    archived_at: datetime | None
    metadata: dict