import logging
import json
import csv
import io
from typing import Optional, List, Dict, Any, Union

from mcp_core.mcp_response import McpResponse, StatusCode # Needed for error responses
from ..server import mcp_app
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, InvalidInputError, DatabaseConnectionError
from .. import utils # Import the new utils module

logger = logging.getLogger(__name__)

def _format_results(data: Union[List[Dict[str, Any]], List[str]], format_type: str) -> str:
    """Formats structured data (list of dicts or list of strings) into CSV or JSON string."""
    if not data:
        return "[]" if format_type == "json" else ""

    if format_type == "json":
        try:
            return json.dumps(data, indent=2)
        except TypeError as e:
            logger.error("JSON serialization error: %s", e, exc_info=True)
            return f'{{"error": "Failed to serialize results to JSON: {e}"}}'
    else: # CSV or plain list
        output = io.StringIO()
        if data:
            if isinstance(data[0], dict):
                # List of Dicts -> CSV
                headers = data[0].keys()
                writer = csv.DictWriter(output, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)
            else:
                # Assume List of Strings -> simple newline separated
                for item in data:
                    output.write(str(item) + '\n')
        return output.getvalue()

def _format_error(e: Exception, format_type: str) -> str:
    """Formats an exception into a string, potentially JSON."""
    error_type = type(e).__name__
    message = str(e)
    if format_type == "json":
        error_obj = {"error": {"type": error_type, "message": message}}
        return json.dumps(error_obj)
    else:
        return f"ERROR: [{error_type}] {message}"

@mcp_app.resource("vast://tables")
async def list_vast_tables(format: str = "json", headers: dict = None) -> McpResponse:
    """Provides a list of available tables in the VAST DB.

    Requires X-Vast-Access-Key and X-Vast-Secret-Key headers.
    """
    # Validate format param early
    format_type = format.lower() if format.lower() in ["json", "csv", "list"] else "json"
    if format_type == "list": format_type = "csv" # Treat list as csv internally

    logger.info("MCP Resource request: vast://tables?format=%s", format_type)

    try:
        # Use the utility function
        access_key, secret_key = utils.extract_auth_headers(headers)
    except ValueError as e:
        logger.warning("Authentication header error for vast://tables: %s", e)
        return _format_error_response(e, format_type, StatusCode.UNAUTHENTICATED)

    try:
        table_names = await db_ops.list_tables(access_key, secret_key)
        logger.debug("Table list request successful, formatting as %s.", format_type)
        # Use the same formatter, it handles list of strings for csv/list
        return _format_results(table_names, format_type)

    except VastMcpError as e:
        logger.error("Database error handling vast://tables: %s", e, exc_info=True)
        return _format_error(e, format_type)
    except Exception as e:
        logger.exception("Unexpected error handling vast://tables: %s", e)
        return _format_error(e, format_type)

@mcp_app.resource("vast://tables/{table_name}")
async def get_vast_table_sample(table_name: str, limit: Optional[int] = 10, format: str = "csv") -> str:
    """Provides a sample of data from a specified VAST DB table.

    Args:
        table_name: The name of the table, extracted from the resource path.
        limit: The maximum number of rows to return (default 10), from query param.
        format: The desired output format ('csv' or 'json'), from query param.

    Returns:
        A string containing the sample data in the specified format, or an error message.
    """
    effective_limit = limit if limit is not None and limit > 0 else 10
    format_type = format.lower() if format.lower() in ["csv", "json"] else "csv"
    logger.info(
        "MCP Resource request received for: vast://tables/%s?limit=%d&format=%s (effective_limit=%d)",
        table_name,
        limit if limit is not None else -1,
        format_type,
        effective_limit
    )

    try:
        # db_ops now returns List[Dict] or str (message) or raises VastMcpError
        result_data = await db_ops.get_table_sample(table_name, effective_limit)

        if isinstance(result_data, str):
            # It's an informational message (e.g., "-- No data found --")
            logger.info("Received message from db_ops for table '%s': %s", table_name, result_data)
            # Return message directly, maybe format as JSON if requested?
            # if format_type == "json": return json.dumps({"message": result_data})
            return result_data
        elif isinstance(result_data, list):
            # Format the list of dicts
            logger.debug("Formatting successful table sample for '%s' as %s.", table_name, format_type)
            return _format_results(result_data, format_type)
        else:
            # Should not happen
            logger.error("Unexpected data type from db_ops.get_table_sample: %s", type(result_data))
            raise TypeError("Unexpected internal data format.") # Raise internal error

    except InvalidInputError as e:
        logger.warning("Invalid input for vast://tables/%s: %s", table_name, e)
        return _format_error(e, format_type)
    except VastMcpError as e:
        logger.error("Database error handling vast://tables/%s: %s", table_name, e, exc_info=True)
        return _format_error(e, format_type)
    except Exception as e:
        # Catch any other unexpected errors during processing or formatting
        logger.exception("Unexpected error handling vast://tables/%s: %s", table_name, e)
        return _format_error(e, format_type)
