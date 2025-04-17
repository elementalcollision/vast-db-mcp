from mcp.server.fastmcp import FastMCP

# Import resource and tool modules to ensure decorators run
from .resources import schema       # Registers schema resource
from .resources import table_data   # Registers table_data resource
from .tools import query            # Registers query tool

# Create the FastMCP application instance
mcp_app = FastMCP("VAST DB MCP Server", description="An MCP server to interact with VAST DB.")

# --- Resources and Tools are registered via decorators in imported modules ---
# @mcp_app.resource("vast://schemas") -> Handled in resources/schema.py
# @mcp_app.resource("vast://tables/{table_name}") -> Handled in resources/table_data.py
# @mcp_app.tool("vast_sql_query") -> Handled in tools/query.py


# --- Main Application ---
# The FastMCP instance (`mcp_app`) itself is the ASGI application.
# It will be imported and run by an ASGI server like uvicorn.

# Example of how to run this with uvicorn (e.g., in scripts/run_server.py):
# import uvicorn
# if __name__ == "__main__":
#     uvicorn.run("vast_mcp_server.server:mcp_app", host="0.0.0.0", port=8000, reload=True)
