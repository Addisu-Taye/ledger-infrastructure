"""
MCP Server for The Ledger — Model Context Protocol Interface

Exposes audit infrastructure to AI agents via standardized tools and resources.
Implements structural CQRS: Tools = Commands (write), Resources = Queries (read).
"""

import asyncio
import json
from typing import Any, Optional
from mcp.server import Server
from mcp.types import Tool, Resource, TextContent, ImageContent, EmbeddedResource
from src.store.event_store import EventStore
from src.store.upcaster import UpcasterRegistry, registry
from src.projections.daemon import ProjectionDaemon
from src.projections.handlers import ApplicationSummaryProjection, AgentPerformanceProjection, ComplianceAuditProjection
from src.mcp.tools import (
    submit_application, record_credit_analysis, record_fraud_screening,
    record_compliance_check, generate_decision, record_human_review,
    start_agent_session, run_integrity_check
)
from src.mcp.resources import (
    get_application_summary, get_compliance_audit, get_audit_trail,
    get_agent_performance, get_ledger_health
)
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize MCP server
server = Server("ledger-infrastructure")

# Global instances (initialized in main)
event_store: Optional[EventStore] = None
projection_daemon: Optional[ProjectionDaemon] = None


@server.list_tools()
async def list_tools() -> list[Tool]:
    """Return available MCP tools (commands)."""
    return [
        Tool(
            name="submit_application",
            description="Submit a new loan application. Preconditions: applicant_id required, requested_amount_usd > 0.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "applicant_id": {"type": "string"},
                    "requested_amount_usd": {"type": "number"},
                    "applicant_data": {"type": "object"}
                },
                "required": ["application_id", "applicant_id", "requested_amount_usd"]
            }
        ),
        Tool(
            name="record_credit_analysis",
            description="Record AI agent credit analysis. Preconditions: application must be in SUBMITTED state.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "risk_tier": {"type": "string", "enum": ["LOW", "MEDIUM", "HIGH"]},
                    "confidence_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "analysis_duration_ms": {"type": "integer"}
                },
                "required": ["application_id", "agent_id", "risk_tier"]
            }
        ),
        Tool(
            name="record_fraud_screening",
            description="Record fraud screening result. Preconditions: credit analysis must be complete.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "fraud_score": {"type": "number", "minimum": 0, "maximum": 1},
                    "flags": {"type": "array", "items": {"type": "string"}},
                    "review_required": {"type": "boolean"}
                },
                "required": ["application_id", "fraud_score"]
            }
        ),
        Tool(
            name="record_compliance_check",
            description="Record compliance rule evaluation. Preconditions: fraud screening must be complete.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "rule_id": {"type": "string"},
                    "rule_version": {"type": "string"},
                    "status": {"type": "string", "enum": ["PASSED", "FAILED"]},
                    "evidence_hash": {"type": "string"},
                    "failure_reason": {"type": "string"},
                    "remediation_required": {"type": "boolean"}
                },
                "required": ["application_id", "rule_id", "status"]
            }
        ),
        Tool(
            name="generate_decision",
            description="Generate final decision recommendation. Preconditions: all compliance checks must be PASSED.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "recommendation": {"type": "string", "enum": ["APPROVE", "DECLINE", "REFER"]},
                    "reason": {"type": "string"},
                    "approved_amount_usd": {"type": "number"}
                },
                "required": ["application_id", "recommendation", "reason"]
            }
        ),
        Tool(
            name="record_human_review",
            description="Record human reviewer decision. Preconditions: decision must be generated.",
            inputSchema={
                "type": "object",
                "properties": {
                    "application_id": {"type": "string"},
                    "reviewer_id": {"type": "string"},
                    "final_decision": {"type": "string", "enum": ["APPROVED", "DECLINED", "REFERRED"]},
                    "notes": {"type": "string"},
                    "override_reason": {"type": "string"}
                },
                "required": ["application_id", "reviewer_id", "final_decision"]
            }
        ),
        Tool(
            name="start_agent_session",
            description="Initialize agent session context (Gas Town pattern). Required before any agent actions.",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {"type": "string"},
                    "agent_id": {"type": "string"},
                    "model_version": {"type": "string"},
                    "context_data": {"type": "object"}
                },
                "required": ["session_id", "agent_id", "model_version"]
            }
        ),
        Tool(
            name="run_integrity_check",
            description="Run cryptographic audit chain verification. Returns tamper detection status.",
            inputSchema={
                "type": "object",
                "properties": {
                    "from_position": {"type": "integer", "minimum": 0},
                    "to_position": {"type": "integer"},
                    "auditor_id": {"type": "string"}
                },
                "required": ["auditor_id"]
            }
        )
    ]


@server.list_resources()
async def list_resources() -> list[Resource]:
    """Return available MCP resources (queries)."""
    return [
        Resource(
            uri="ledger://applications/{application_id}",
            name="Application Summary",
            description="Current state of a loan application (p99 < 50ms). Returns ApplicationSummary projection.",
            mimeType="application/json"
        ),
        Resource(
            uri="ledger://applications/{application_id}/compliance",
            name="Compliance Audit View",
            description="Regulatory compliance state with temporal query support (p99 < 200ms).",
            mimeType="application/json"
        ),
        Resource(
            uri="ledger://applications/{application_id}/audit-trail",
            name="Full Audit Trail",
            description="Complete event stream for an application (p99 < 500ms).",
            mimeType="application/json"
        ),
        Resource(
            uri="ledger://agents/{agent_id}/performance",
            name="Agent Performance Ledger",
            description="Aggregated metrics for an agent/model version (p99 < 50ms).",
            mimeType="application/json"
        ),
        Resource(
            uri="ledger://ledger/health",
            name="Ledger Health",
            description="System health metrics including projection lag (p99 < 10ms).",
            mimeType="application/json"
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent | ImageContent | EmbeddedResource]:
    """Execute an MCP tool (command)."""
    global event_store
    
    if not event_store:
        return [TextContent(type="text", text="Error: EventStore not initialized")]
    
    try:
        if name == "submit_application":
            result = await submit_application(event_store, **arguments)
        elif name == "record_credit_analysis":
            result = await record_credit_analysis(event_store, **arguments)
        elif name == "record_fraud_screening":
            result = await record_fraud_screening(event_store, **arguments)
        elif name == "record_compliance_check":
            result = await record_compliance_check(event_store, **arguments)
        elif name == "generate_decision":
            result = await generate_decision(event_store, **arguments)
        elif name == "record_human_review":
            result = await record_human_review(event_store, **arguments)
        elif name == "start_agent_session":
            result = await start_agent_session(event_store, **arguments)
        elif name == "run_integrity_check":
            result = await run_integrity_check(event_store, **arguments)
        else:
            return [TextContent(type="text", text=f"Error: Unknown tool '{name}'")]
        
        return [TextContent(type="text", text=json.dumps(result, default=str))]
    
    except Exception as e:
        # Structured error response for agent recovery
        return [TextContent(type="text", text=json.dumps({
            "error": type(e).__name__,
            "message": str(e),
            "suggested_action": "retry_with_backoff" if "concurrency" in str(e).lower() else "contact_admin"
        }, default=str))]


@server.read_resource()
async def read_resource(uri: str) -> str:
    """Read an MCP resource (query)."""
    global event_store
    
    if not event_store:
        return json.dumps({"error": "EventStore not initialized"})
    
    try:
        if uri.startswith("ledger://applications/"):
            parts = uri.replace("ledger://applications/", "").split("/")
            application_id = parts[0]
            
            if len(parts) == 1:
                # Application summary
                return await get_application_summary(event_store, application_id)
            elif parts[1] == "compliance":
                # Compliance audit view
                return await get_compliance_audit(event_store, application_id)
            elif parts[1] == "audit-trail":
                # Full event stream
                return await get_audit_trail(event_store, application_id)
        
        elif uri.startswith("ledger://agents/") and uri.endswith("/performance"):
            agent_id = uri.replace("ledger://agents/", "").replace("/performance", "")
            return await get_agent_performance(event_store, agent_id)
        
        elif uri == "ledger://ledger/health":
            return await get_ledger_health(event_store, projection_daemon)
        
        return json.dumps({"error": f"Unknown resource: {uri}"})
    
    except Exception as e:
        return json.dumps({
            "error": type(e).__name__,
            "message": str(e)
        }, default=str)


async def main():
    """Initialize and run the MCP server."""
    global event_store, projection_daemon
    
    # Initialize EventStore
    dsn = os.getenv("DATABASE_URL")
    event_store = EventStore(dsn, registry)
    await event_store.connect()
    
    # Initialize Projection Daemon
    projections = [
        ApplicationSummaryProjection(),
        AgentPerformanceProjection(),
        ComplianceAuditProjection()
    ]
    projection_daemon = ProjectionDaemon(event_store, projections)
    
    # Start daemon in background
    daemon_task = asyncio.create_task(
        projection_daemon.run_forever(poll_interval_ms=100, batch_size=100)
    )
    
    print("✓ Ledger MCP Server initialized")
    print(f"✓ EventStore connected to: {dsn[:50]}...")
    print("✓ Projection Daemon running in background")
    print("✓ 8 Tools available: submit_application, record_credit_analysis, ...")
    print("✓ 5 Resources available: ledger://applications/{id}, ...")
    
    # Run MCP server (stdio transport for local development)
    await server.run()
    
    # Cleanup on shutdown
    daemon_task.cancel()
    await event_store.close()


if __name__ == "__main__":
    asyncio.run(main())
