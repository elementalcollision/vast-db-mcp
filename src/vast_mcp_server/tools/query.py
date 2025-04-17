import logging
from ..server import mcp_app  # Import the FastMCP instance
from ..vast_integration import db_ops  # Import the db operations module

# Get logger for this module
logger = logging.getLogger(__name__)

@mcp_app.tool()
async def vast_sql_query(sql: str) -> str:
    """Executes a read-only (SELECT) SQL query against the VAST database.

    Args:
        sql: The SELECT SQL query to execute.

    Returns:
        A string containing the query results in CSV format, or an error message.
    """
    # Truncate potentially long SQL for logging
    sql_snippet = sql[:200] + ("..." if len(sql) > 200 else "")
    logger.info("MCP Tool request received for: vast_sql_query with SQL: %s", sql_snippet)
    try:
        # Input validation happens within db_ops.execute_sql_query
        results = await db_ops.execute_sql_query(sql)
        logger.debug("vast_sql_query tool request successful.")
        return results
    except Exception as e:
        logger.error("Error handling vast_sql_query tool request: %s", e, exc_info=True)
        # Return an error message to the client/model
        return f"Error executing VAST DB SQL query: {e}"
