# src/store/concurrency.py
class OptimisticConcurrencyError(Exception):
    """Raised when expected_version does not match actual stream version."""
    
    def __init__(self, stream_id: str, expected: int, actual: int):
        self.stream_id = stream_id
        self.expected_version = expected
        self.actual_version = actual
        super().__init__(
            f"Optimistic concurrency conflict on stream '{stream_id}': "
            f"expected version {expected}, but actual version is {actual}"
        )