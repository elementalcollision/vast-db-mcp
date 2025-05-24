import logging
import json
import csv
import io
from typing import List, Dict, Any
from mcp_server.fastmcp import Context # Import Context
from ..server import mcp_app, limiter
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, InvalidInputError, DatabaseConnectionError
from .. import utils, config
from starlette.requests import Request

# Get logger for this module
logger = logging.getLogger(__name__)

# _format_results and _format_error have been moved to utils.py

@mcp_app.tool()
@limiter.limit(config.DEFAULT_RATE_LIMIT)  # Apply rate limit
async def vast_sql_query(request: Request, sql: str, format: str, headers: dict, ctx: Context) -> str:
    """Executes a SQL query against the VAST database using the shared connection.

    Authentication is performed by comparing X-Vast-Access-Key and X-Vast-Secret-Key
    headers from the request against the server's configured credentials.
    Allowed SQL statement types are controlled by the MCP_ALLOWED_SQL_TYPES environment variable.

    Args:
        request: The Starlette Request object.
        sql: The SQL query to execute.
        format: The desired output format ('csv' or 'json').
        headers: Request headers containing authentication credentials.
        ctx: The MCP Context, used to access the shared DB connection from the lifespan manager.

    Returns:
        A string containing the query results or an error message, formatted as requested.
    """
    sql_snippet = sql[:200] + ("..." if len(sql) > 200 else "")
    format_type = format.lower() if format.lower() in ["csv", "json"] else "csv"
    logger.info(
        "MCP Tool request: vast_sql_query(format='%s', sql='%s') from %s",
        format_type, sql_snippet, request.client.host
    )

    try:
        provided_access_key, provided_secret_key = utils.extract_auth_headers(headers)
        if not (provided_access_key == config.VAST_ACCESS_KEY and provided_secret_key == config.VAST_SECRET_KEY):
            logger.warning("Mismatch in provided VAST credentials and server configuration for vast_sql_query.")
            auth_error = ValueError("Provided credentials do not match server configuration.")
            return utils.format_tool_error_response_body(auth_error, format_type)
    except ValueError as e: # From extract_auth_headers if headers are missing
        logger.warning("Authentication header error for vast_sql_query: %s", e)
        return utils.format_tool_error_response_body(e, format_type)

    # Retrieve the shared VAST DB connection from the application context.
    db_connection = ctx.request_context.lifespan_context.db_connection
    if not db_connection:
        logger.error("Database connection not found in context for vast_sql_query.")
        conn_error = DatabaseConnectionError("Database connection unavailable.")
        return utils.format_tool_error_response_body(conn_error, format_type)

    try:
        result_data = await db_ops.execute_sql_query(db_connection, sql)

        if isinstance(result_data, str):
            # It's an informational message (e.g., "-- No data found --")
            logger.info("Received message from db_ops for SQL query: %s", result_data)
            return result_data
        elif isinstance(result_data, list):
            # Format the list of dicts
            logger.debug("Formatting successful SQL query result as %s.", format_type)
            return utils.format_data_payload(result_data, format_type)
        else:
             # Should not happen
            logger.error("Unexpected data type from db_ops.execute_sql_query: %s", type(result_data))
            # Use the existing error formatter for consistency
            type_error = TypeError("Unexpected internal data format.")
            return utils.format_tool_error_response_body(type_error, format_type)

    except InvalidInputError as e:
        logger.warning("Invalid input for vast_sql_query: %s", e)
        return utils.format_tool_error_response_body(e, format_type)
    except VastMcpError as e:
        logger.error("Database error handling vast_sql_query: %s", e, exc_info=True)
        return utils.format_tool_error_response_body(e, format_type)
    except Exception as e:
        # Catch any other unexpected errors
        logger.exception("Unexpected error handling vast_sql_query: %s", e)
        return utils.format_tool_error_response_body(e, format_type)
