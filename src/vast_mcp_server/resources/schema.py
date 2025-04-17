import logging
import json
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module
from ..exceptions import VastMcpError # Import base custom error

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.resource("vast://schemas")
async def get_vast_schema() -> str:
    """Provides the VAST DB schema as a resource.

    Fetches table names and column definitions from the connected VAST DB.
    """
    logger.info("MCP Resource request received for: vast://schemas")
    try:
        # get_db_schema now returns string or raises VastMcpError subtypes
        schema_info = await db_ops.get_db_schema()
        logger.debug("Schema resource request successful.")
        return schema_info
    except VastMcpError as e:
        logger.error("Error handling vast://schemas request: %s", e, exc_info=True)
        # Provide a slightly more structured error message
        error_type = type(e).__name__
        return f"ERROR: [{error_type}] {e}"
    except Exception as e:
        # Catch any other unexpected errors
        logger.exception("Unexpected error handling vast://schemas request: %s", e)
        return f"ERROR: [UnexpectedError] An unexpected error occurred: {e}"
