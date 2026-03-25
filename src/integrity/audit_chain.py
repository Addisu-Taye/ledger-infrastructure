import hashlib
import json
import uuid
from typing import Optional, List, Any
from datetime import datetime
from dataclasses import dataclass, asdict
import asyncpg

def _json_serializer(obj: Any) -> Any:
    """Custom JSON serializer for non-standard types."""
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, '__dict__'):
        return obj.__dict__
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

@dataclass
class IntegrityCheckResult:
    """Result of cryptographic integrity verification."""
    check_id: str
    check_timestamp: datetime
    last_verified_position: int
    chain_valid: bool
    tamper_detected: bool
    tamper_details: Optional[List[dict]] = None
    next_expected_hash: Optional[str] = None

class CryptographicAuditChain:
    """
    Hash chain verification for tamper detection.
    Each AuditIntegrityCheckRun event includes the hash of all prior events.
    """
    
    def __init__(self, algorithm: str = "sha256"):
        self.algorithm = algorithm
    
    def _compute_hash(self, data: dict) -> str:
        """Compute cryptographic hash of event data with UUID-safe serialization."""
        # Canonical JSON serialization for consistent hashing
        canonical = json.dumps(data, sort_keys=True, separators=(',', ':'), default=_json_serializer)
        hasher = hashlib.new(self.algorithm)
        hasher.update(canonical.encode('utf-8'))
        return hasher.hexdigest()
    
    def _compute_chain_hash(self, events: List[dict], previous_hash: Optional[str] = None) -> str:
        """
        Compute cumulative hash chain.
        H(n) = hash(H(n-1) || event_n_payload || event_n_metadata)
        """
        current_hash = previous_hash or "genesis"
        
        for event in events:
            # Convert row to dict if needed
            if hasattr(event, '_mapping'):
                event = dict(event._mapping)
            
            # Ensure payload/metadata are dicts
            payload = event.get("payload", {})
            metadata = event.get("metadata", {})
            if isinstance(payload, str):
                payload = json.loads(payload)
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            
            # Hash components that affect integrity
            components = {
                "event_id": str(event.get("event_id")),
                "stream_id": event.get("stream_id"),
                "stream_position": event.get("stream_position"),
                "event_type": event.get("event_type"),
                "event_version": event.get("event_version"),
                "payload_hash": self._compute_hash(payload),
                "metadata_hash": self._compute_hash(metadata),
                "recorded_at": event.get("recorded_at").isoformat() if isinstance(event.get("recorded_at"), datetime) else str(event.get("recorded_at")),
                "previous_chain_hash": current_hash
            }
            current_hash = self._compute_hash(components)
        
        return current_hash
    
    async def run_integrity_check(
        self,
        conn: asyncpg.Connection,
        from_position: int = 0,
        to_position: Optional[int] = None,
        previous_check_hash: Optional[str] = None
    ) -> IntegrityCheckResult:
        """
        Verify cryptographic integrity of events in range.
        Returns result with tamper detection status.
        """
        # Load events to verify
        if to_position:
            rows = await conn.fetch("""
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events 
                WHERE global_position >= $1 AND global_position <= $2
                ORDER BY global_position
            """, from_position, to_position)
        else:
            rows = await conn.fetch("""
                SELECT event_id, stream_id, stream_position, global_position,
                       event_type, event_version, payload, metadata, recorded_at
                FROM events 
                WHERE global_position >= $1
                ORDER BY global_position
            """, from_position)
        
        if not rows:
            return IntegrityCheckResult(
                check_id=f"check-{datetime.now().isoformat()}",
                check_timestamp=datetime.now(),
                last_verified_position=from_position - 1,
                chain_valid=True,
                tamper_detected=False,
                next_expected_hash=previous_check_hash
            )
        
        # Compute expected chain hash
        computed_hash = self._compute_chain_hash(rows, previous_check_hash)
        
        # Check for tampering
        tamper_details = []
        tamper_detected = False
        
        # Query for any prior integrity check that covers this range
        prior_check = await conn.fetchrow("""
            SELECT payload
            FROM events 
            WHERE event_type = 'AuditIntegrityCheckRun' 
            ORDER BY recorded_at DESC
            LIMIT 1
        """)
        
        if prior_check:
            prior_payload = prior_check["payload"]
            if isinstance(prior_payload, str):
                prior_payload = json.loads(prior_payload)
            stored_hash = prior_payload.get("next_expected_hash")
            if stored_hash and stored_hash != computed_hash:
                tamper_detected = True
                tamper_details.append({
                    "type": "chain_mismatch",
                    "expected": stored_hash,
                    "computed": computed_hash,
                    "position_range": f"{from_position}-{rows[-1]['global_position']}"
                })
        
        last_position = rows[-1]["global_position"]
        
        return IntegrityCheckResult(
            check_id=f"check-{datetime.now().isoformat()}",
            check_timestamp=datetime.now(),
            last_verified_position=last_position,
            chain_valid=not tamper_detected,
            tamper_detected=tamper_detected,
            tamper_details=tamper_details if tamper_details else None,
            next_expected_hash=computed_hash
        )
    
    async def record_integrity_check(
        self,
        conn: asyncpg.Connection,
        result: IntegrityCheckResult,
        auditor_id: str
    ) -> str:
        """Record the integrity check result as an AuditIntegrityCheckRun event."""
        import uuid as uuid_lib
        from src.events import BaseEvent
        
        event = BaseEvent(
            type="AuditIntegrityCheckRun",
            version=1,
            payload={
                "check_id": result.check_id,
                "check_timestamp": result.check_timestamp.isoformat(),
                "last_verified_position": result.last_verified_position,
                "chain_valid": result.chain_valid,
                "tamper_detected": result.tamper_detected,
                "tamper_details": result.tamper_details,
                "auditor_id": auditor_id,
                "algorithm": self.algorithm,
                "next_expected_hash": result.next_expected_hash
            }
        )
        
        # Append to special audit stream
        audit_stream = "system:audit:integrity"
        expected_version = await conn.fetchval(
            "SELECT current_version FROM event_streams WHERE stream_id = $1",
            audit_stream
        ) or 0
        
        event_id = str(uuid_lib.uuid4())
        
        await conn.execute("""
            INSERT INTO events 
            (event_id, stream_id, stream_position, event_type, event_version, payload, metadata)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
        """, event_id, audit_stream, expected_version + 1, event.type, event.version,
            json.dumps(event.payload, default=_json_serializer), json.dumps(event.metadata, default=_json_serializer))
        
        await conn.execute("""
            INSERT INTO event_streams (stream_id, aggregate_type, current_version)
            VALUES ($1, $2, $3)
            ON CONFLICT (stream_id) DO UPDATE SET current_version = $3
        """, audit_stream, "AuditIntegrityCheck", expected_version + 1)
        
        return event_id
