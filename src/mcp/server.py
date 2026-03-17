from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, Resource
from src.mcp.tools import TOOL_DEFINITIONS
from src.mcp.resources import RESOURCE_DEFINITIONS

app = Server("ledger-infrastructure")

@app.list_tools()
async def list_tools():
    return [Tool(**tool) for tool in TOOL_DEFINITIONS]

@app.call_tool()
async def call_tool(name: str, arguments: dict):
    # Route to specific command handler (e.g., submit_application)
    # Must return structured errors for LLM consumption
    if name == "submit_application":
        try:
            # Logic to call EventStore.append
            return {"content": [{"type": "text", "text": "Application Submitted"}]}
        except Exception as e:
            return {"content": [{"type": "text", "text": f"Error: {str(e)}"}]}

@app.list_resources()
async def list_resources():
    return [Resource(**res) for res in RESOURCE_DEFINITIONS]

@app.read_resource()
async def read_resource(uri: str):
    # Route to Projection Query
    return {"contents": [{"uri": uri, "text": "Projection Data"}]}

async def main():
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())