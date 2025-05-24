import logging
import json
import csv
import io
from typing import Optional, List, Dict, Any, Union

from mcp_core.mcp_response import McpResponse, StatusCode
from mcp_server.fastmcp import Context # Import Context
from ..server import mcp_app, limiter
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, InvalidInputError, DatabaseConnectionError, QueryExecutionError
from .. import utils, config
from starlette.requests import Request

logger = logging.getLogger(__name__)

# _format_results has been moved to utils.py as format_data_payload

@mcp_app.resource("vast://tables")
@limiter.limit(config.DEFAULT_RATE_LIMIT)
async def list_vast_tables(request: Request, format: str = "json", headers: dict, ctx: Context) -> McpResponse:
    """Provides a list of available tables in the VAST DB.

    Authentication is performed by comparing X-Vast-Access-Key and X-Vast-Secret-Key
    headers from the request against the server's configured credentials.

    Args:
        request: The Starlette Request object.
        format: The desired output format ("json", "csv", "list"). Defaults to "json".
                "list" is treated as "csv".
        headers: Request headers containing authentication credentials.
        ctx: The MCP Context, used to access shared resources like the DB connection.

    Returns:
        An McpResponse containing the list of table names or an error.
    """
    format_type = format.lower() if format.lower() in ["json", "csv", "list"] else "json"
    if format_type == "list":
        format_type = "csv" # Treat "list" as "csv" for content type and formatting

    logger.info("MCP Resource request: vast://tables?format=%s from %s", format_type, request.client.host)

    try:
        provided_access_key, provided_secret_key = utils.extract_auth_headers(headers)
        if not (provided_access_key == config.VAST_ACCESS_KEY and provided_secret_key == config.VAST_SECRET_KEY):
            logger.warning("Mismatch in provided VAST credentials and server configuration for list_vast_tables.")
            return McpResponse(
                status_code=StatusCode.UNAUTHENTICATED,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Provided credentials do not match server configuration."}).encode('utf-8')
            )
    except ValueError as e: # From extract_auth_headers if headers are missing
        logger.warning("Authentication header error for vast://tables: %s", e)
        return McpResponse(
            status_code=StatusCode.UNAUTHENTICATED,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Authentication error", "details": str(e)}).encode('utf-8')
        )

    # Retrieve the shared VAST DB connection from the application context.
    db_connection = ctx.request_context.lifespan_context.db_connection
    if not db_connection:
        logger.error("Database connection not found in context for list_vast_tables.")
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database connection unavailable."}).encode('utf-8')
        )

    try:
        table_names = await db_ops.list_tables(db_connection)
        logger.debug("Table list request successful, formatting as %s.", format_type)
        
        body_content = utils.format_data_payload(table_names, format_type)
        content_type = "application/json" if format_type == "json" else "text/csv; charset=utf-8"
        
        return McpResponse(
            status_code=StatusCode.OK,
            headers={"Content-Type": content_type},
            body=body_content.encode('utf-8')
        )
    except DatabaseConnectionError as e: # This might still occur if the connection passed from context is bad
        logger.error("Database connection error for vast://tables: %s", e, exc_info=True)
        # The nature of the error might change as we are not establishing connection here.
        # For instance, "authentication failed" might be less likely if initial conn succeeded.
        # However, connection could drop or have other issues.
        return McpResponse(
            status_code=StatusCode.SERVICE_UNAVAILABLE, # Generic for connection issues post-setup
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database operation failed due to connection issue", "details": str(e)}).encode('utf-8')
        )
    except VastMcpError as e: # Catch other VAST specific errors
        logger.error("VastMcpError handling vast://tables: %s", e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "A VAST specific error occurred", "details": str(e)}).encode('utf-8')
        )
    except Exception as e:
        logger.exception("Unexpected error handling vast://tables: %s", e)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "An unexpected server error occurred", "details": str(e)}).encode('utf-8')
        )

@mcp_app.resource("vast://tables/{table_name}")
@limiter.limit(config.DEFAULT_RATE_LIMIT)
async def get_vast_table_sample(request: Request, table_name: str, limit: Optional[int], format: str, headers: dict, ctx: Context) -> McpResponse:
    """Provides a sample of data from a specified VAST DB table.

    Authentication is performed by comparing X-Vast-Access-Key and X-Vast-Secret-Key
    headers from the request against the server's configured credentials.

    Args:
        request: The Starlette Request object.
        table_name: The name of the table to sample.
        limit: The maximum number of rows to return. Defaults to 10.
        format: The desired output format ("csv" or "json"). Defaults to "csv".
        headers: Request headers containing authentication credentials.
        ctx: The MCP Context, used to access shared resources like the DB connection.

    Returns:
        An McpResponse containing the table sample data or an error.
    """
    effective_limit = limit if limit is not None and limit > 0 else 10
    format_type = format.lower() if format.lower() in ["csv", "json"] else "csv"
    logger.info(
        "MCP Resource request: vast://tables/%s?limit=%s&format=%s (effective_limit=%d) from %s",
        table_name, str(limit), format_type, effective_limit, request.client.host
    )

    try:
        provided_access_key, provided_secret_key = utils.extract_auth_headers(headers)
        if not (provided_access_key == config.VAST_ACCESS_KEY and provided_secret_key == config.VAST_SECRET_KEY):
            logger.warning(
                "Mismatch in provided VAST credentials and server configuration for get_vast_table_sample: %s",
                table_name
            )
            return McpResponse(
                status_code=StatusCode.UNAUTHENTICATED,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Provided credentials do not match server configuration."}).encode('utf-8')
            )
    except ValueError as e: # From extract_auth_headers if headers are missing
        logger.warning("Authentication header error for vast://tables/%s: %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.UNAUTHENTICATED,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Authentication error", "details": str(e)}).encode('utf-8')
        )

    # Retrieve the shared VAST DB connection from the application context.
    db_connection = ctx.request_context.lifespan_context.db_connection
    if not db_connection:
        logger.error("Database connection not found in context for get_vast_table_sample: %s", table_name)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database connection unavailable."}).encode('utf-8')
        )

    try:
        result_data = await db_ops.get_table_sample(db_connection, table_name, effective_limit)

        if isinstance(result_data, str): # E.g., "-- No data found --"
            logger.info("Received message from db_ops for table '%s': %s", table_name, result_data)
            return McpResponse(
                status_code=StatusCode.OK, # As per requirement, could be NOT_FOUND too
                headers={"Content-Type": "text/plain; charset=utf-8"},
                body=result_data.encode('utf-8')
            )
        elif isinstance(result_data, list):
            logger.debug("Formatting successful table sample for '%s' as %s.", table_name, format_type)
            body_content = utils.format_data_payload(result_data, format_type)
            content_type = "application/json" if format_type == "json" else "text/csv; charset=utf-8"
            return McpResponse(
                status_code=StatusCode.OK,
                headers={"Content-Type": content_type},
                body=body_content.encode('utf-8')
            )
        else:
            # This case should ideally not be reached if db_ops is consistent
            logger.error("Unexpected data type from db_ops.get_table_sample: %s for table %s", type(result_data), table_name)
            return McpResponse(
                status_code=StatusCode.INTERNAL_SERVER_ERROR,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Received unexpected data format from database operation."}).encode('utf-8')
            )

    except InvalidInputError as e:
        logger.warning("Invalid input for vast://tables/%s: %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.BAD_REQUEST,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Invalid input", "details": str(e)}).encode('utf-8')
        )
    except DatabaseConnectionError as e: # Still possible if connection from context is bad
        logger.error("Database connection error for vast://tables/%s: %s", table_name, e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.SERVICE_UNAVAILABLE,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "Database operation failed due to connection issue", "details": str(e)}).encode('utf-8')
        )
    except QueryExecutionError as e: # Specific VAST error that might indicate table not found
        logger.warning("Query execution error for vast://tables/%s: %s", table_name, e)
        error_str = str(e).lower()
        if "not found" in error_str or "does not exist" in error_str or "no such table" in error_str:
            status = StatusCode.NOT_FOUND
            error_msg = f"Table '{table_name}' not found."
        else:
            status = StatusCode.INTERNAL_SERVER_ERROR
            error_msg = "Query execution failed."
        return McpResponse(
            status_code=status,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": error_msg, "details": str(e)}).encode('utf-8')
        )
    except VastMcpError as e: # Catch other VAST specific errors
        logger.error("VastMcpError handling vast://tables/%s: %s", table_name, e, exc_info=True)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "A VAST specific error occurred", "details": str(e)}).encode('utf-8')
        )
    except Exception as e:
        logger.exception("Unexpected error handling vast://tables/%s: %s", table_name, e)
        return McpResponse(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            headers={"Content-Type": "application/json"},
            body=json.dumps({"error": "An unexpected server error occurred", "details": str(e)}).encode('utf-8')
        )
