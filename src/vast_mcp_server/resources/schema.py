import logging
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.resource("vast://schemas")
async def get_vast_schema() -> str:
    """Provides the VAST DB schema as a resource.

    Fetches table names and column definitions from the connected VAST DB.
    """
    logger.info("MCP Resource request received for: vast://schemas")
    try:
        schema_info = await db_ops.get_db_schema()
        logger.debug("Schema resource request successful.")
        return schema_info
    except Exception as e:
        logger.error("Error handling vast://schemas resource request: %s", e, exc_info=True)
        # Return an error message to the client
        return f"Error retrieving VAST DB schema: {e}"
