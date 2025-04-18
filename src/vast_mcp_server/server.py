import logging
from mcp.server.fastmcp import FastMCP
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from starlette.requests import Request # Needed for type hinting in limiter key func
from . import config # Import config

# --- Logging Configuration ---
# Configure basic logging
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
# Optionally set lower level for specific libraries if needed, e.g.:
# logging.getLogger("uvicorn.error").setLevel(logging.WARNING)

logger = logging.getLogger(__name__) # Get logger for this module
logger.info("Initializing VAST DB MCP Server...")

# --- Rate Limiter Configuration ---
# Read rate limit from config
limiter = Limiter(key_func=get_remote_address, default_limits=[config.DEFAULT_RATE_LIMIT])
logger.info("Rate limiter configured with default limit: %s per IP", config.DEFAULT_RATE_LIMIT)

# Import resource and tool modules AFTER limiter is defined
# if decorators need to access it (though middleware handles it globally here)
logger.info("Importing and registering MCP resources and tools...")
from .resources import schema       # Registers schema resource
from .resources import table_data   # Registers table_data resource
from .resources import metadata     # Registers metadata resource
from .tools import query            # Registers query tool
logger.info("MCP resources and tools registered.")

# Create the FastMCP application instance
mcp_app = FastMCP("VAST DB MCP Server", description="An MCP server to interact with VAST DB.")
logger.info("FastMCP application instance created.")

# Add Rate Limiter State and Middleware
mcp_app.state.limiter = limiter
mcp_app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
# Note: FastMCP app is likely a Starlette app, so add_middleware should work
# If FastMCP uses a different underlying framework, this might need adjustment
# mcp_app.add_middleware(SlowAPIMiddleware) # This line causes issues with FastMCP's routing
# Instead, we might need to add limits directly to routes or use a different integration method
# For now, we'll rely on applying limits within handlers if needed, or explore FastMCP-specific middleware
# Since global middleware isn't straightforward, we'll skip adding it for now and rely on potential future handler-level limits.
logger.warning("Global SlowAPI middleware integration skipped due to potential FastMCP conflicts. Consider handler-level limits if needed.")

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
