from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module


@mcp_app.tool()
async def vast_sql_query(sql: str) -> str:
    """Executes a read-only (SELECT) SQL query against the VAST database.

    Args:
        sql: The SELECT SQL query to execute.

    Returns:
        A string containing the query results in CSV format, or an error message.
    """
    print(f"MCP Tool request received for: vast_sql_query with SQL: {sql[:100]}...") # Log start, truncate long SQL
    try:
        # Input validation happens within db_ops.execute_sql_query
        results = await db_ops.execute_sql_query(sql)
        return results
    except Exception as e:
        # Log the exception for debugging
        print(f"Error handling vast_sql_query tool request: {e}")
        # Return an error message to the client/model
        return f"Error executing VAST DB SQL query: {e}"
