# src/store/event_store.py
import asyncpg
import json
import uuid
from typing import Optional, List, AsyncIterator
from datetime import datetime
from src.events import BaseEvent, StoredEvent, StreamMetadata
from src.store.concurrency import OptimisticConcurrencyError
from src.store.upcaster import UpcasterRegistry  # Will create in Phase 4

class EventStore:
    """Async event store with optimistic concurrency control."""
    
    def __init__(self, dsn: str, upcaster_registry: Optional[UpcasterRegistry] = None):
        self.dsn = dsn
        self.upcaster_registry = upcaster_registry
        self._pool: Optional[asyncpg.Pool] = None
    
    async def connect(self) -> None:
        """Initialize database connection pool."""
        self._pool = await asyncpg.create_pool(self.dsn)
    
    async def close(self) -> None:
        """Close database connection pool."""
        if self._pool:
            await self._pool.close()
    
    async def append(
        self,
        stream_id: str,
        events: List[BaseEvent],
        expected_version: int,
        correlation_id: Optional[str] = None,
        causation_id: Optional[str] = None,
    ) -> int:
        """
        Atomically appends events to stream_id.
        Raises OptimisticConcurrencyError if stream version != expected_version.
        Writes to outbox in same transaction.
        Returns new stream version.
        """
        if not self._pool:
            await self.connect()
        
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # 1. Check current version for optimistic concurrency
                row = await conn.fetchrow(
                    "SELECT current_version FROM event_streams WHERE stream_id = $1",
                    stream_id
                )
                current_version = row["current_version"] if row else 0
                
                # 2. Enforce expected_version (-1 means new stream)
                if expected_version != -1 and current_version != expected_version:
                    raise OptimisticConcurrencyError(
                        stream_id=stream_id,
                        expected=expected_version,
                        actual=current_version
                    )
                
                # 3. Append events
                new_version = current_version
                aggregate_type = events[0].type if events else "Unknown"
                
                for event in events:
                    new_version += 1
                    event_id = str(uuid.uuid4())
                    
                    # Build metadata
                    metadata = event.metadata.copy()
                    if correlation_id:
                        metadata["correlation_id"] = correlation_id
                    if causation_id:
                        metadata["causation_id"] = causation_id
                    
                    await conn.execute(
                        """
                        INSERT INTO events 
                        (event_id, stream_id, stream_position, event_type, event_version, payload, metadata)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """,
                        event_id, stream_id, new_version, event.type, event.version,
                        json.dumps(event.payload), json.dumps(metadata)
                    )
                    
                    # 4. Outbox pattern: write to outbox in same transaction
                    await conn.execute(
                        """
                        INSERT INTO outbox (event_id, destination, payload)
                        VALUES ($1, $2, $3)
                        """,
                        event_id, "internal_projection", json.dumps(event.payload)
                    )
                
                # 5. Update or create stream metadata
                if not row:
                    await conn.execute(
                        """
                        INSERT INTO event_streams 
                        (stream_id, aggregate_type, current_version)
                        VALUES ($1, $2, $3)
                        """,
                        stream_id, aggregate_type, new_version
                    )
                else:
                    await conn.execute(
                        "UPDATE event_streams SET current_version = $1 WHERE stream_id = $2",
                        new_version, stream_id
                    )
                
                return new_version
    
    async def load_stream(
        self,
        stream_id: str,
        from_position: int = 0,
        to_position: Optional[int] = None,
    ) -> List[StoredEvent]:
        """Load events from a stream, optionally upcasted."""
        if not self._pool:
            await self.connect()
        
        async with self._pool.acquire() as conn:
            if to_position:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events 
                    WHERE stream_id = $1 
                    AND stream_position >= $2 
                    AND stream_position <= $3
                    ORDER BY stream_position
                    """,
                    stream_id, from_position, to_position
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM events 
                    WHERE stream_id = $1 AND stream_position >= $2
                    ORDER BY stream_position
                    """,
                    stream_id, from_position
                )
            
            events = []
            for row in rows:
                stored = StoredEvent(
                    event_id=row["event_id"],
                    stream_id=row["stream_id"],
                    stream_position=row["stream_position"],
                    global_position=row["global_position"],
                    event_type=row["event_type"],
                    event_version=row["event_version"],
                    payload=row["payload"],
                    metadata=row["metadata"],
                    recorded_at=row["recorded_at"]
                )
                # Apply upcasting if registry exists (Phase 4)
                if self.upcaster_registry:
                    stored = self.upcaster_registry.upcast(stored)
                events.append(stored)
            
            return events
    
    async def load_all(
        self,
        from_global_position: int = 0,
        event_types: Optional[List[str]] = None,
        batch_size: int = 500,
    ) -> AsyncIterator[StoredEvent]:
        """Async generator for efficient event replay."""
        if not self._pool:
            await self.connect()
        
        current_position = from_global_position
        
        while True:
            async with self._pool.acquire() as conn:
                if event_types:
                    placeholders = ",".join(f"${i}" for i in range(2, 2 + len(event_types)))
                    query = f"""
                        SELECT * FROM events 
                        WHERE global_position > $1 
                        AND event_type IN ({placeholders})
                        ORDER BY global_position 
                        LIMIT $2
                    """
                    rows = await conn.fetch(query, current_position, *event_types, batch_size)
                else:
                    rows = await conn.fetch(
                        """
                        SELECT * FROM events 
                        WHERE global_position > $1 
                        ORDER BY global_position 
                        LIMIT $2
                        """,
                        current_position, batch_size
                    )
                
                if not rows:
                    break
                
                for row in rows:
                    stored = StoredEvent(
                        event_id=row["event_id"],
                        stream_id=row["stream_id"],
                        stream_position=row["stream_position"],
                        global_position=row["global_position"],
                        event_type=row["event_type"],
                        event_version=row["event_version"],
                        payload=row["payload"],
                        metadata=row["metadata"],
                        recorded_at=row["recorded_at"]
                    )
                    if self.upcaster_registry:
                        stored = self.upcaster_registry.upcast(stored)
                    yield stored
                    current_position = row["global_position"]
                
                if len(rows) < batch_size:
                    break
    
    async def stream_version(self, stream_id: str) -> int:
        """Get current version of a stream."""
        if not self._pool:
            await self.connect()
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT current_version FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            return row["current_version"] if row else 0
    
    async def archive_stream(self, stream_id: str) -> None:
        """Mark a stream as archived."""
        if not self._pool:
            await self.connect()
        
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE event_streams 
                SET archived_at = NOW() 
                WHERE stream_id = $1
                """,
                stream_id
            )
    
    async def get_stream_metadata(self, stream_id: str) -> Optional[StreamMetadata]:
        """Get metadata for a stream."""
        if not self._pool:
            await self.connect()
        
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM event_streams WHERE stream_id = $1",
                stream_id
            )
            if not row:
                return None
            return StreamMetadata(
                stream_id=row["stream_id"],
                aggregate_type=row["aggregate_type"],
                current_version=row["current_version"],
                created_at=row["created_at"],
                archived_at=row["archived_at"],
                metadata=row["metadata"]
            )