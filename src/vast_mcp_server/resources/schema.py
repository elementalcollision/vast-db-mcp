import logging
import json
from mcp_core.mcp_response import McpResponse, StatusCode # Needed for error responses
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module
from ..exceptions import VastMcpError, DatabaseConnectionError # Import base custom error
from .. import utils # Import the new utils module

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.resource("vast://schemas")
async def get_vast_schema(headers: dict = None) -> McpResponse:
    """Provides the VAST DB schema as a resource.

    Fetches table names and column definitions from the connected VAST DB.
    Requires X-Vast-Access-Key and X-Vast-Secret-Key headers.
    """
    logger.info("MCP Resource request received for: vast://schemas")
    try:
        # Use the utility function
        access_key, secret_key = utils.extract_auth_headers(headers)
    except ValueError as e:
        logger.warning("Authentication header error for vast://schemas: %s", e)
        # Return McpResponse with UNAUTHENTICATED status
        return McpResponse(
            status_code=StatusCode.UNAUTHENTICATED,
            headers={"Content-Type": "text/plain"}, # Keep content type simple for error
            body=f"ERROR: [AuthenticationError] {e}".encode('utf-8')
        )

    try:
        # Pass credentials to db_ops function
        schema_info = await db_ops.get_db_schema(access_key, secret_key)
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
