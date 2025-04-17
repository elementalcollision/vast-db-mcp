import logging
import json
import csv
import io
from typing import List, Dict, Any
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module

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
        "MCP Tool request received for: vast_sql_query with format='%s' and SQL: %s",
        format_type,
        sql_snippet
    )

    try:
        # db_ops now returns List[Dict] or str (error/message)
        result_data = await db_ops.execute_sql_query(sql)

        if isinstance(result_data, str):
            # It's an error or informational message from db_ops
            logger.warning("Received message/error from db_ops for SQL query: %s", result_data)
            return result_data
        elif isinstance(result_data, list):
            # Format the list of dicts
            logger.debug("SQL query tool request successful, formatting as %s.", format_type)
            return _format_results(result_data, format_type)
        else:
            # Should not happen
            logger.error("Unexpected data type received from db_ops.execute_sql_query: %s", type(result_data))
            return "Error: Unexpected internal data format received."

    except Exception as e:
        logger.error("Error handling vast_sql_query tool request: %s", e, exc_info=True)
        # Return an error message to the client/model
        err_msg = f"Error executing VAST DB SQL query: {e}"
        # if format_type == "json":
        #     return json.dumps({"error": err_msg})
        return err_msg
