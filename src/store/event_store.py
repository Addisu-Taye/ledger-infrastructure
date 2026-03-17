import asyncpg
import uuid
import json
from typing import Optional, List, AsyncIterator
from src.events import BaseEvent, StoredEvent
from src.store.concurrency import OptimisticConcurrencyError
from src.store.upcaster import UpcasterRegistry

class EventStore:
    def __init__(self, dsn: str, upcaster_registry: UpcasterRegistry):
        self.dsn = dsn
        self.upcaster_registry = upcaster_registry
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self.dsn)

    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> int:
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Check current version for optimistic concurrency
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id
                )
                current_version = row["current_version"] if row else 0

                if expected_version != -1 and current_version != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id, 
                        expected=expected_version, 
                        actual=current_version
                    )

                # 2. Append events
                new_version = current_version
                for event in events:
                    new_version += 1
                    await conn.execute(
                        """
                        INSERT INTO events (stream_id, stream_position, event_type, event_version, payload, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        """,
                        stream_id, new_version, event.type, event.version, 
                        json.dumps(event.payload), json.dumps(event.metadata)
                    )
                    
                    # Outbox Pattern: Write to outbox in same transaction
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3)
                        """,
                        # In real impl, capture generated event_id. Simplified here.
                        uuid.uuid4(), "internal_projection", json.dumps(event.payload)
                    )

                # 3. Update Stream Version
                if not row:
                    await conn.execute(
                        "INSERT INTO event_streams (stream_id, aggregate_type, current_version) VALUES ($1, $2, $3)",
                        stream_id, events[0].aggregate_type, new_version
                    )
                else:
                    await conn.execute(
                        "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                        new_version, stream_id
                    )
                
                return new_version

    async def load_stream(self, stream_id: str, from_position: int = 0) -> List[StoredEvent]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM events WHERE stream_id = $1 AND stream_position >= $2 ORDER BY stream_position",
                stream_id, from_position
            )
            events = []
            for row in rows:
                stored = StoredEvent(
                    event_id=row["event_id"],
                    stream_id=row["stream_id"],
                    stream_position=row["stream_position"],
                    event_type=row["event_type"],
                    event_version=row["event_version"],
                    payload=row["payload"],
                    recorded_at=row["recorded_at"]
                )
                # Phase 4: Auto-upcasting
                events.append(self.upcaster_registry.upcast(stored))
            return events