import logging
import json
from urllib.parse import unquote
from mcp_core.mcp_response import McpResponse, StatusCode
from mcp_server.fastmcp import Context # Import Context
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, InvalidInputError, TableDescribeError, DatabaseConnectionError
from .. import utils, config
from ..server import limiter, mcp_app
from starlette.requests import Request

logger = logging.getLogger(__name__)

# --- Resource Handler ---

@mcp_app.resource("vast://metadata/tables/{table_name}")
@limiter.limit(config.DEFAULT_RATE_LIMIT)
async def get_table_metadata_resource(request: Request, table_name: str, headers: dict, ctx: Context) -> McpResponse:
    """
    MCP Resource handler for fetching metadata of a specific VAST DB table.
    Handles URIs like: vast://metadata/tables/{table_name}.

    Authentication is performed by comparing X-Vast-Access-Key and X-Vast-Secret-Key
    headers from the request against the server's configured credentials.

    Args:
        request: The Starlette Request object.
        table_name: The name of the table to fetch metadata for.
        headers: Request headers containing authentication credentials.
        ctx: The MCP Context, used to access shared resources like the DB connection.

    Returns:
        An McpResponse containing the table metadata or an error.
    """
    try:
        provided_access_key, provided_secret_key = utils.extract_auth_headers(headers)
        if not (provided_access_key == config.VAST_ACCESS_KEY and provided_secret_key == config.VAST_SECRET_KEY):
            logger.warning(
                "Mismatch in provided VAST credentials and server configuration for table metadata request: %s",
                table_name
            )
            return McpResponse(
                status_code=StatusCode.UNAUTHENTICATED,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Provided credentials do not match server configuration."}).encode('utf-8')
            )
    except ValueError as e: # From extract_auth_headers if headers are missing
        logger.warning("Authentication header error for table %s: %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.UNAUTHENTICATED,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Authentication error", "details": str(e)}).encode('utf-8')
        )

    # Retrieve the shared VAST DB connection from the application context.
    db_connection = ctx.request_context.lifespan_context.db_connection
    if not db_connection:
        logger.error("Database connection not found in context for table metadata request: %s", table_name)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database connection unavailable."}).encode('utf-8')
        )

    # table_name is now a direct argument, unquote if necessary
    table_name = unquote(table_name)
    logger.info("Handling GET request for metadata of table: %s", table_name)

    try:
        # Pass the connection from context, no longer individual keys
        metadata = await db_ops.get_table_metadata(db_connection, table_name)
        response_body = json.dumps(metadata, indent=2)
        return McpResponse(
            status_code=StatusCode.OK,
            headers={"Content-Type": "application/json"},
            body=response_body.encode('utf-8')
        )
    except ValueError as e: # Typically for config/setup issues before db_ops call
        logger.warning("Value error during metadata fetch for table '%s': %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.BAD_REQUEST, # Or INTERNAL_SERVER_ERROR depending on context
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Configuration or input error", "details": str(e)}).encode('utf-8')
        )
    except InvalidInputError as e:
        logger.warning("Invalid input for table metadata request '%s': %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.BAD_REQUEST,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Invalid input", "details": str(e)}).encode('utf-8')
        )
    except TableDescribeError as e:
        logger.warning("Error describing table '%s': %s", table_name, e)
        if "exist" in str(e).lower() or "no columns" in str(e).lower():
            status_code = StatusCode.NOT_FOUND
            error_message = f"Table '{table_name}' not found or metadata unavailable."
        else:
            status_code = StatusCode.INTERNAL_SERVER_ERROR
            error_message = f"Failed to retrieve metadata for table '{table_name}'."
        return McpResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": error_message, "details": str(e)}).encode('utf-8')
        )
    except DatabaseConnectionError as e:
        logger.error("Database connection error during metadata fetch for table '%s': %s", table_name, e, exc_info=True)
        if "authentication failed" in str(e).lower() or "invalid credentials" in str(e).lower():
            status_code = StatusCode.UNAUTHENTICATED
            error_body = {"error": "Authentication failed with provided credentials.", "details": str(e)}
        else:
            status_code = StatusCode.SERVICE_UNAVAILABLE
            error_body = {"error": "Database connection error", "details": str(e)}
        return McpResponse(
            status_code=status_code,
            headers={"Content-Type": "application/json"},
            body=json.dumps(error_body).encode('utf-8')
        )
    except Exception as e:
        logger.error("Unexpected error fetching metadata for table '%s': %s", table_name, e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "An unexpected server error occurred", "details": str(e)}).encode('utf-8')
        )

# Note: Resource registration is handled by the @mcp_app.resource decorator.