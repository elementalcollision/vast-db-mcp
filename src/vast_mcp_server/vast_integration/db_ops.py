import vastdb
import asyncio
import csv
import io
import logging # Import logging
from .. import config  # Import configuration from the parent package

# Get logger for this module
logger = logging.getLogger(__name__)

def create_vast_connection() -> vastdb.api.VastSession:
    """Creates and returns a VAST DB connection session.

    Uses connection details defined in the config module.

    Returns:
        vastdb.api.VastSession: An active VAST DB session.

    Raises:
        Exception: If connection fails (specific exception type may vary based on SDK).
    """
    logger.debug("Attempting to connect to VAST DB at %s", config.VAST_DB_ENDPOINT)
    try:
        # Assuming vastdb.connect uses these parameters.
        # Adjust based on the actual VAST DB SDK's connect function signature.
        conn = vastdb.connect(
            endpoint=config.VAST_DB_ENDPOINT,
            access_key=config.VAST_ACCESS_KEY,
            secret_key=config.VAST_SECRET_KEY,
            # Add any other necessary connection parameters here, e.g., verify_ssl=False
        )
        logger.info("Successfully connected to VAST DB.") # Optional: for debugging
        return conn
    except Exception as e:
        logger.error("Error connecting to VAST DB: %s", e, exc_info=True)
        # Re-raise the exception or handle it more gracefully depending on requirements
        raise


# --- Database Operations ---

def _fetch_schema_sync() -> str:
    """Synchronous helper function to fetch and format schema."""
    logger.debug("Starting synchronous schema fetch.")
    conn = None
    try:
        conn = create_vast_connection()
        if not conn: # Connection might have failed and raised in create_vast_connection
             return "Error: Failed to establish database connection for schema fetch."
        cursor = conn.cursor()

        logger.debug("Executing SHOW TABLES")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables if t]
        logger.info("Found %d tables: %s", len(table_names), table_names)

        schema_parts = []
        for table_name in table_names:
            logger.debug("Describing table: %s", table_name)
            try:
                # Using DESCRIBE or similar command - adjust SQL if needed for VAST DB
                cursor.execute(f"DESCRIBE TABLE {table_name}")
                columns = cursor.fetchall()
                schema_parts.append(f"TABLE: {table_name}")
                col_defs = [f"  - {col[0]} ({col[1]})" for col in columns] # Assuming name, type are first 2
                schema_parts.extend(col_defs)
                schema_parts.append("")
                logger.debug("Successfully described table: %s", table_name)
            except Exception as desc_e:
                logger.warning("Error describing table '%s': %s", table_name, desc_e, exc_info=True)
                schema_parts.append(f"TABLE: {table_name}")
                schema_parts.append(f"  - Error describing table: {desc_e}")
                schema_parts.append("")

        if not schema_parts:
            logger.warning("No tables found or unable to describe any tables.")
            return "-- No tables found or unable to describe tables. --"

        logger.debug("Schema fetch completed successfully.")
        return "\n".join(schema_parts)

    except Exception as e:
        logger.error("Error fetching schema from VAST DB: %s", e, exc_info=True)
        # Return error message instead of raising here, as async wrapper expects string
        return f"Error fetching schema from VAST DB: {e}"
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after schema fetch.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def get_db_schema() -> str:
    """Fetches the database schema asynchronously.

    Returns:
        str: A string representation of the schema.
    """
    logger.info("Received request to fetch DB schema.")
    result = await asyncio.to_thread(_fetch_schema_sync)
    return result

def _fetch_table_sample_sync(table_name: str, limit: int) -> str:
    """Synchronous helper function to fetch and format table sample data."""
    logger.debug("Starting synchronous table sample fetch for table '%s' limit %d.", table_name, limit)
    conn = None
    try:
        conn = create_vast_connection()
        if not conn:
            return "Error: Failed to establish database connection for table sample fetch."
        cursor = conn.cursor()

        # Basic input validation
        if not table_name.isidentifier():
             logger.warning("Invalid table name requested for sample: %s", table_name)
             return f"Error: Invalid table name '{table_name}'."
        if not isinstance(limit, int) or limit <= 0:
            logger.warning("Invalid limit %s provided for table sample, defaulting to 10.", limit)
            limit = 10

        # Execute query
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        logger.debug("Executing query: %s", query)
        cursor.execute(query)

        # Fetch results and column names
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
        logger.info("Fetched %d rows from table '%s' with columns: %s", len(results), table_name, column_names)

        if not results:
            logger.info("No data found in table '%s' for sample.", table_name)
            return f"-- No data found in table '{table_name}' or table does not exist. --"

        # Format results as CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)
        writer.writerows(results)
        csv_output = output.getvalue()
        logger.debug("Formatted sample data for '%s' as CSV string.", table_name)
        return csv_output

    except Exception as e:
        logger.error("Error fetching sample data for table '%s': %s", table_name, e, exc_info=True)
        return f"Error fetching sample data for table '{table_name}' from VAST DB: {e}"
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after table sample fetch.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def get_table_sample(table_name: str, limit: int = 10) -> str:
    """Fetches a sample of data from a table asynchronously.

    Args:
        table_name (str): The name of the table.
        limit (int): The maximum number of rows to fetch.

    Returns:
        str: A string representation of the sample data (e.g., CSV).
    """
    logger.info("Received request for table sample: table='%s', limit=%d", table_name, limit)
    result = await asyncio.to_thread(_fetch_table_sample_sync, table_name, limit)
    return result

def _execute_sql_sync(sql: str) -> str:
    """Synchronous helper function to execute a SQL query and format results."""
    logger.debug("Starting synchronous SQL execution: %s...", sql[:100])
    # Security Check
    clean_sql = sql.strip().upper()
    if not clean_sql.startswith("SELECT"):
        logger.warning("Rejected non-SELECT query: %s...", sql[:100])
        return "Error: Only SELECT queries are currently allowed for safety."

    conn = None
    try:
        conn = create_vast_connection()
        if not conn:
            return "Error: Failed to establish database connection for SQL query execution."
        cursor = conn.cursor()

        logger.debug("Executing validated SQL query.")
        cursor.execute(sql) # Execute original SQL, not uppercased version

        if cursor.description is None:
            logger.info("SQL query executed, but no description/results returned.")
            return "-- Query executed, but no results returned (or not a SELECT query). --"

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        logger.info("SQL query returned %d rows with columns: %s", len(results), column_names)

        if not results:
            logger.info("SQL query returned no rows.")
            return "-- Query executed successfully, but returned no rows. --"

        # Format results as CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)
        writer.writerows(results)
        csv_output = output.getvalue()
        logger.debug("Formatted SQL query results as CSV string.")
        return csv_output

    except Exception as e:
        logger.error("Error executing SQL query: %s", e, exc_info=True)
        return f"Error executing SQL query in VAST DB: {e}"
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after SQL query execution.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def execute_sql_query(sql: str) -> str:
    """Executes a read-only SQL query asynchronously.

    Args:
        sql (str): The SQL query to execute (must be SELECT).

    Returns:
        str: A string representation of the query results (e.g., CSV) or an error message.
    """
    logger.info("Received request to execute SQL query: %s...", sql[:100])
    result = await asyncio.to_thread(_execute_sql_sync, sql)
    return result
