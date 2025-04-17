from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module


@mcp_app.resource("vast://schemas")
async def get_vast_schema() -> str:
    """Provides the VAST DB schema as a resource.

    Fetches table names and column definitions from the connected VAST DB.
    """
    print("MCP Resource request received for: vast://schemas") # Optional: for debugging
    try:
        schema_info = await db_ops.get_db_schema()
        return schema_info
    except Exception as e:
        # Log the exception for debugging
        print(f"Error handling vast://schemas resource request: {e}")
        # Return an error message to the client
        return f"Error retrieving VAST DB schema: {e}"
