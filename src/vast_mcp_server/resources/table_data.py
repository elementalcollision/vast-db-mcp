import logging
import json
import csv
import io
from typing import Optional, List, Dict, Any

from ..server import mcp_app
from ..vast_integration import db_ops

logger = logging.getLogger(__name__)

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
        # db_ops now returns List[Dict] or str (error/message)
        result_data = await db_ops.get_table_sample(table_name, effective_limit)

        if isinstance(result_data, str):
            # It's an error or informational message from db_ops
            logger.warning("Received message/error from db_ops for table '%s': %s", table_name, result_data)
            return result_data
        elif isinstance(result_data, list):
            # Format the list of dicts
            logger.debug("Table sample resource request successful for table '%s', formatting as %s.", table_name, format_type)
            return _format_results(result_data, format_type)
        else:
            # Should not happen based on db_ops return type hint, but handle defensively
            logger.error("Unexpected data type received from db_ops.get_table_sample: %s", type(result_data))
            return "Error: Unexpected internal data format received."

    except Exception as e:
        logger.error(
            "Error handling vast://tables/%s resource request: %s",
            table_name,
            e,
            exc_info=True
        )
        # Return an error message to the client
        err_msg = f"Error retrieving sample data for table '{table_name}': {e}"
        # Optionally format error as JSON if JSON was requested
        # if format_type == "json":
        #     return json.dumps({"error": err_msg})
        return err_msg
