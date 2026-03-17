import asyncio
import time
from src.store.event_store import EventStore
from src.projections.handlers import ProjectionHandler

class ProjectionDaemon:
    def __init__(self, store: EventStore, handlers: List[ProjectionHandler]):
        self._store = store
        self._handlers = {h.name: h for h in handlers}
        self._running = False
        self._last_position = 0

    async def run_forever(self, poll_interval_ms: int = 100):
        self._running = True
        while self._running:
            try:
                await self._process_batch()
            except Exception as e:
                # Fault-tolerant: log error, continue
                print(f"Projection error: {e}")
            await asyncio.sleep(poll_interval_ms / 1000)

    async def _process_batch(self):
        # Load events from last global position
        async for event in self._store.load_all(from_global_position=self._last_position):
            for handler in self._handlers.values():
                if event.event_type in handler.subscribed_events:
                    try:
                        await handler.handle(event)
                    except Exception as e:
                        # Skip offending event with logging
                        print(f"Handler {handler.name} failed on {event.event_id}: {e}")
            
            self._last_position = event.global_position
            # Update checkpoint logic here

    def get_lag(self) -> dict:
        # Calculate lag metric for SLO monitoring
        return {"projection_lag_ms": 150} # Mocked for structure