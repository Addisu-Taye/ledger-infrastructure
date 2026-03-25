import asyncio
import asyncpg
import json
from typing import List, Optional
from datetime import datetime
from src.store.event_store import EventStore
from src.projections.handlers import Projection
from src.events import StoredEvent

class ProjectionDaemon:
    """Async daemon that processes events and updates projections."""
    
    def __init__(self, store: EventStore, projections: List[Projection]):
        self._store = store
        self._projections = {p.name: p for p in projections}
        self._running = False
        self._last_processed_position = 0
        self._error_count = 0
        self._max_consecutive_errors = 10
    
    async def run_forever(self, poll_interval_ms: int = 100, batch_size: int = 100) -> None:
        """Main daemon loop - runs until stopped."""
        self._running = True
        print(f"✓ ProjectionDaemon started with {len(self._projections)} projections")
        
        while self._running:
            try:
                await self._process_batch(batch_size)
                self._error_count = 0
            except Exception as e:
                self._error_count += 1
                print(f"✗ Daemon error ({self._error_count}/{self._max_consecutive_errors}): {e}")
                if self._error_count >= self._max_consecutive_errors:
                    print("✗ Max errors reached, stopping daemon")
                    break
                await asyncio.sleep(1)
            
            await asyncio.sleep(poll_interval_ms / 1000)
    
    async def _process_batch(self, batch_size: int) -> None:
        """Process a batch of events through all subscribed projections."""
        if not self._store._pool:
            await self._store.connect()
        
        async with self._store._pool.acquire() as conn:
            # Get checkpoints for all projections
            checkpoints = await conn.fetch("""
                SELECT projection_name, last_position 
                FROM projection_checkpoints 
                WHERE projection_name = ANY($1)
            """, list(self._projections.keys()))
            
            # Initialize checkpoints if not exist
            for proj_name in self._projections:
                if not any(c["projection_name"] == proj_name for c in checkpoints):
                    await conn.execute("""
                        INSERT INTO projection_checkpoints (projection_name, last_position)
                        VALUES ($1, 0)
                    """, proj_name)
                    checkpoints.append({"projection_name": proj_name, "last_position": 0})
            
            # Find minimum position to process from
            min_position = min(c["last_position"] for c in checkpoints) if checkpoints else 0
            
            # Load events from min_position
            events = await conn.fetch("""
                SELECT * FROM events 
                WHERE global_position > $1 
                ORDER BY global_position 
                LIMIT $2
            """, min_position, batch_size)
            
            if not events:
                return
            
            # Process each event through subscribed projections
            for row in events:
                event = self._row_to_stored_event(row)
                
                for proj_name, projection in self._projections.items():
                    if event.event_type in projection.subscribed_events:
                        try:
                            await projection.handle(event, conn)
                        except Exception as e:
                            print(f"✗ Projection {proj_name} failed on event {event.event_id}: {e}")
                
                self._last_processed_position = row["global_position"]
            
            # Update all checkpoints
            for proj_name in self._projections:
                await conn.execute("""
                    UPDATE projection_checkpoints 
                    SET last_position = $1, updated_at = NOW()
                    WHERE projection_name = $2
                """, self._last_processed_position, proj_name)
    
    def _row_to_stored_event(self, row) -> StoredEvent:
        """Convert database row to StoredEvent with proper JSON parsing."""
        # Ensure payload and metadata are dicts (asyncpg may return strings)
        payload = row["payload"]
        metadata = row["metadata"]
        
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        
        return StoredEvent(
            event_id=row["event_id"],
            stream_id=row["stream_id"],
            stream_position=row["stream_position"],
            global_position=row["global_position"],
            event_type=row["event_type"],
            event_version=row["event_version"],
            payload=payload,
            metadata=metadata,
            recorded_at=row["recorded_at"]
        )
    
    def get_lag(self) -> dict:
        """Return lag metrics for all projections."""
        return {
            "last_processed_position": self._last_processed_position,
            "projections": {name: "lag_metric_available" for name in self._projections}
        }
    
    def stop(self) -> None:
        """Signal daemon to stop."""
        self._running = False
