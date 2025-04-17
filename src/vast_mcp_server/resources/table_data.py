from typing import Optional

from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module


@mcp_app.resource("vast://tables/{table_name}")
async def get_vast_table_sample(table_name: str, limit: Optional[int] = 10) -> str:
    """Provides a sample of data from a specified VAST DB table.

    Args:
        table_name: The name of the table, extracted from the resource path.
        limit: The maximum number of rows to return (default 10), from query param.

    Returns:
        A string containing the sample data, typically in CSV format.
    """
    print(f"MCP Resource request received for: vast://tables/{table_name}?limit={limit}")
    effective_limit = limit if limit is not None and limit > 0 else 10
    try:
        sample_data = await db_ops.get_table_sample(table_name, effective_limit)
        return sample_data
    except Exception as e:
        # Log the exception for debugging
        print(f"Error handling vast://tables/{table_name} resource request: {e}")
        # Return an error message to the client
        return f"Error retrieving sample data for table '{table_name}': {e}"
