import logging
import json
import csv
import io
from typing import List, Dict, Any
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module
from ..exceptions import VastMcpError, InvalidInputError # Import relevant custom errors

# Get logger for this module
logger = logging.getLogger(__name__)

# Re-use the formatter from table_data (or move to a shared utils module)
def _format_results(data: List[Dict[str, Any]], format_type: str) -> str:
    """Formats structured data into CSV or JSON string."""
    if not data:
        return "[]" if format_type == "json" else ""

    if format_type == "json":
        try:
            # Use indent for readability, maybe configurable later
            return json.dumps(data, indent=2)
        except TypeError as e:
            logger.error("JSON serialization error: %s", e, exc_info=True)
            return f'{{"error": "Failed to serialize results to JSON: {e}"}}'
    else: # Default to CSV
        output = io.StringIO()
        if data:
            # Use the keys from the first dictionary as header
            headers = data[0].keys()
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()
            writer.writerows(data)
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

@mcp_app.tool()
async def vast_sql_query(sql: str, format: str = "csv") -> str:
    """Executes a read-only (SELECT) SQL query against the VAST database.

    Args:
        sql: The SELECT SQL query to execute.
        format: The desired output format ('csv' or 'json'). Defaults to 'csv'.

    Returns:
        A string containing the query results in the specified format, or an error message.
    """
    sql_snippet = sql[:200] + ("..." if len(sql) > 200 else "")
    format_type = format.lower() if format.lower() in ["csv", "json"] else "csv"
    logger.info(
        "MCP Tool request: vast_sql_query(format='%s', sql='%s')",
        format_type,
        sql_snippet
    )

    try:
        # db_ops now returns List[Dict] or str (message) or raises VastMcpError
        result_data = await db_ops.execute_sql_query(sql)

        if isinstance(result_data, str):
            # It's an informational message (e.g., "-- No data found --")
            logger.info("Received message from db_ops for SQL query: %s", result_data)
            # if format_type == "json": return json.dumps({"message": result_data})
            return result_data
        elif isinstance(result_data, list):
            # Format the list of dicts
            logger.debug("Formatting successful SQL query result as %s.", format_type)
            return _format_results(result_data, format_type)
        else:
             # Should not happen
            logger.error("Unexpected data type from db_ops.execute_sql_query: %s", type(result_data))
            raise TypeError("Unexpected internal data format.")

    except InvalidInputError as e:
        logger.warning("Invalid input for vast_sql_query: %s", e)
        return _format_error(e, format_type)
    except VastMcpError as e:
        logger.error("Database error handling vast_sql_query: %s", e, exc_info=True)
        return _format_error(e, format_type)
    except Exception as e:
        # Catch any other unexpected errors
        logger.exception("Unexpected error handling vast_sql_query: %s", e)
        return _format_error(e, format_type)
