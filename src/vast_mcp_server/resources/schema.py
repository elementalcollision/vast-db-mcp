import logging
import json
from mcp_core.mcp_response import McpResponse, StatusCode
from mcp_server.fastmcp import Context # Import Context
from ..server import mcp_app, limiter
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, DatabaseConnectionError, SchemaFetchError
from .. import utils, config
from starlette.requests import Request

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.resource("vast://schemas")
@limiter.limit(config.DEFAULT_RATE_LIMIT) # Apply rate limit
async def get_vast_schema(request: Request, headers: dict, ctx: Context) -> McpResponse:
    """Provides the VAST DB schema as a resource.

    Fetches table names and column definitions from the connected VAST DB.
    Authentication is performed by comparing X-Vast-Access-Key and X-Vast-Secret-Key
    headers from the request against the server's configured credentials.

    Args:
        request: The Starlette Request object.
        headers: Request headers containing authentication credentials.
        ctx: The MCP Context, used to access shared resources like the DB connection.

    Returns:
        An McpResponse containing the VAST DB schema as a plain text string or an error.
    """
    logger.info("MCP Resource request received for: vast://schemas from %s", request.client.host)
    try:
        provided_access_key, provided_secret_key = utils.extract_auth_headers(headers)
        if not (provided_access_key == config.VAST_ACCESS_KEY and provided_secret_key == config.VAST_SECRET_KEY):
            logger.warning("Mismatch in provided VAST credentials and server configuration for schema request.")
            return McpResponse(
                status_code=StatusCode.UNAUTHENTICATED,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Provided credentials do not match server configuration."}).encode('utf-8')
            )
    except ValueError as e: # From extract_auth_headers if headers are missing
        logger.warning("Authentication header error for vast://schemas: %s", e)
        return McpResponse(
            status_code=StatusCode.UNAUTHENTICATED,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Authentication error", "details": str(e)}).encode('utf-8')
        )

    # Retrieve the shared VAST DB connection from the application context.
    db_connection = ctx.request_context.lifespan_context.db_connection
    if not db_connection:
        logger.error("Database connection not found in context for schema request.")
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database connection unavailable."}).encode('utf-8')
        )

    try:
        schema_info = await db_ops.get_db_schema(db_connection)
        logger.debug("Schema resource request successful for vast://schemas.")
        return McpResponse(
            status_code=StatusCode.OK,
            headers={"Content-Type": "text/plain; charset=utf-8"},
            body=schema_info.encode('utf-8')
        )
    except DatabaseConnectionError as e:
        logger.error("Database connection error for vast://schemas: %s", e, exc_info=True)
        if "authentication failed" in str(e).lower() or "invalid credentials" in str(e).lower():
            status_code = StatusCode.UNAUTHENTICATED
            error_message = "Database authentication failed."
        else:
            status_code = StatusCode.SERVICE_UNAVAILABLE
            error_message = "Database connection error."
        return McpResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": error_message, "details": str(e)}).encode('utf-8')
        )
    except SchemaFetchError as e: # More specific VAST MCP error
        logger.error("Schema fetch error for vast://schemas: %s", e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR, # Or other appropriate code
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Failed to fetch schema", "details": str(e)}).encode('utf-8')
        )
    except VastMcpError as e: # Catch other VastMcpErrors
        logger.error("VastMcpError handling vast://schemas request: %s", e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR, # Generic for other VAST errors
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "A VAST MCP error occurred", "details": str(e)}).encode('utf-8')
        )
    except Exception as e:
        logger.exception("Unexpected error handling vast://schemas request: %s", e)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "An unexpected server error occurred", "details": str(e)}).encode('utf-8')
        )
