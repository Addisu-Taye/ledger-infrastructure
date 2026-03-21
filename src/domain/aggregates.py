from src.store.event_store import EventStore
from src.events import StoredEvent
from enum import Enum

class ApplicationState(Enum):
    SUBMITTED = "SUBMITTED"
    AWAITING_ANALYSIS = "AWAITING_ANALYSIS"
    FINAL_APPROVED = "FINAL_APPROVED"
    # ... other states

class LoanApplicationAggregate:
    def __init__(self, application_id: str):
        self.application_id = application_id
        self.state = ApplicationState.SUBMITTED
        self.version = 0
        self.compliance_checks_passed = []

    @classmethod
    async def load(cls, store: EventStore, application_id: str) -> "LoanApplicationAggregate":
        events = await store.load_stream(f"loan-{application_id}")
        agg = cls(application_id=application_id)
        for event in events:
            agg._apply(event)
        return agg

    def _apply(self, event: StoredEvent) -> None:
        handler = getattr(self, f"_on_{event.event_type}", None)
        if handler:
            handler(event)
        self.version = event.stream_position

    def _on_ApplicationSubmitted(self, event: StoredEvent) -> None:
        self.state = ApplicationState.SUBMITTED

    def _on_ComplianceRulePassed(self, event: StoredEvent) -> None:
        self.compliance_checks_passed.append(event.payload["rule_id"])

    def assert_awaiting_credit_analysis(self):
        if self.state != ApplicationState.AWAITING_ANALYSIS:
            raise ValueError("Invalid state transition")

    def assert_compliance_complete(self):
        # Business Rule 5: Compliance dependency
        if not self.compliance_checks_passed:
            raise ValueError("Cannot approve without compliance checks")