@pytest.fixture
async def event_store():
    """Create fresh event store for each test."""
    dsn = os.getenv("DATABASE_URL", "postgresql://postgres:Pass%4012345@localhost:5433/ledger_infrastructure")
    store = EventStore(dsn, UpcasterRegistry())
    await store.connect()
    
    # Clean up test data with CASCADE to handle FKs
    async with store._pool.acquire() as conn:
        await conn.execute("""
            TRUNCATE TABLE events, event_streams, outbox 
            RESTART IDENTITY CASCADE
        """)
    
    yield store
    await store.close()
