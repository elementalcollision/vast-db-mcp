import logging
from typing import Optional

from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.resource("vast://tables/{table_name}")
async def get_vast_table_sample(table_name: str, limit: Optional[int] = 10) -> str:
    """Provides a sample of data from a specified VAST DB table.

    Args:
        table_name: The name of the table, extracted from the resource path.
        limit: The maximum number of rows to return (default 10), from query param.

    Returns:
        A string containing the sample data, typically in CSV format.
    """
    effective_limit = limit if limit is not None and limit > 0 else 10
    logger.info(
        "MCP Resource request received for: vast://tables/%s?limit=%d (effective_limit=%d)",
        table_name,
        limit if limit is not None else -1, # Log actual requested limit
        effective_limit
    )

    try:
        sample_data = await db_ops.get_table_sample(table_name, effective_limit)
        logger.debug("Table sample resource request successful for table '%s'.", table_name)
        return sample_data
    except Exception as e:
        logger.error(
            "Error handling vast://tables/%s resource request: %s",
            table_name,
            e,
            exc_info=True
        )
        # Return an error message to the client
        return f"Error retrieving sample data for table '{table_name}': {e}"
