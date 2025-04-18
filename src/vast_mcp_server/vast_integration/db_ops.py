import vastdb
import asyncio
import csv
import io
import logging # Import logging
from .. import config  # Import configuration from the parent package
from typing import List, Dict, Any, Union
# Import custom exceptions
from ..exceptions import (
    DatabaseConnectionError,
    SchemaFetchError,
    TableDescribeError,
    QueryExecutionError,
    InvalidInputError,
    VastMcpError
)

# Get logger for this module
logger = logging.getLogger(__name__)

# Define a type alias for structured results or specific messages
QueryResult = Union[List[Dict[str, Any]], str]
# Define type alias for Schema info (always string for now)
SchemaResult = str

def create_vast_connection() -> vastdb.api.VastSession:
    """Creates and returns a VAST DB connection session.

    Uses connection details defined in the config module.

    Returns:
        vastdb.api.VastSession: An active VAST DB session.

    Raises:
        DatabaseConnectionError: If connection fails.
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
        logger.error("Connection to VAST DB failed: %s", e, exc_info=True)
        # Wrap the original exception
        raise DatabaseConnectionError(f"Failed to connect to VAST DB: {e}", original_exception=e)


# --- Database Operations ---

def _fetch_schema_sync() -> SchemaResult:
    """Synchronous helper function to fetch and format schema.

    Returns:
        str: A string representation of the schema.

    Raises:
        DatabaseConnectionError: If connection fails.
        SchemaFetchError: If unable to fetch schema.
        TableDescribeError: If unable to describe tables.
    """
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

        if not table_names:
             logger.warning("No tables found in database.")
             return "-- No tables found in the database. --"

        schema_parts = []
        describe_errors = []
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
                logger.warning("Error describing table '%s': %s", table_name, desc_e)
                # Store the error to potentially raise later or include in message
                describe_errors.append(f"Error describing table '{table_name}': {desc_e}")
                # Add error indication to the output schema string
                schema_parts.append(f"TABLE: {table_name}")
                schema_parts.append(f"  - !!! Error describing table: {desc_e} !!!")
                schema_parts.append("")
                # Optionally, raise immediately if one failure should stop the whole process:
                # raise TableDescribeError(f"Failed to describe table '{table_name}': {desc_e}", original_exception=desc_e)

        schema_output = "\n".join(schema_parts)
        # If we encountered errors describing *some* tables, we could still return the partial schema
        # or raise a higher-level error. Let's return partial for now, logging indicates issues.
        if describe_errors:
            logger.warning("Finished schema fetch with %d describe errors.", len(describe_errors))

        logger.debug("Schema fetch completed.")
        return schema_output

    except DatabaseConnectionError: # Re-raise specific connection errors
        raise
    except Exception as e:
        logger.error("Generic error during schema fetch: %s", e, exc_info=True)
        raise SchemaFetchError(f"Error fetching schema: {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after schema fetch.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def get_db_schema() -> SchemaResult:
    """Fetches the database schema asynchronously.

    Returns:
        str: A string representation of the schema.
    """
    logger.info("Received request to fetch DB schema.")
    # Let exceptions propagate up to the handler
    return await asyncio.to_thread(_fetch_schema_sync)

def _list_tables_sync() -> List[str]:
    """Synchronous helper to fetch table names.
    Raises: DatabaseConnectionError, QueryExecutionError
    """
    logger.debug("Starting synchronous table list fetch.")
    conn = None
    try:
        conn = create_vast_connection() # Can raise DatabaseConnectionError
        cursor = conn.cursor()
        logger.debug("Executing SHOW TABLES")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables if t]
        logger.info("Found %d tables: %s", len(table_names), table_names)
        return table_names
    except DatabaseConnectionError:
        raise
    except Exception as e:
        logger.error("Error executing SHOW TABLES: %s", e, exc_info=True)
        raise QueryExecutionError(f"Failed to list tables: {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after listing tables.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def list_tables() -> List[str]:
    """Fetches a list of table names asynchronously.
    Raises: DatabaseConnectionError, QueryExecutionError
    """
    logger.info("Received request to list tables.")
    return await asyncio.to_thread(_list_tables_sync)

def _fetch_table_sample_sync(table_name: str, limit: int) -> QueryResult:
    """Synchronous helper function to fetch table sample data as list of dicts or error string.

    Returns:
        Union[List[Dict[str, Any]], str]: List of dicts on success, error/message string otherwise.

    Raises:
        InvalidInputError: If table name is invalid.
        DatabaseConnectionError: If unable to establish database connection.
        QueryExecutionError: If unable to execute query.
    """
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
             raise InvalidInputError(f"Invalid table name '{table_name}'.")
        if not isinstance(limit, int) or limit <= 0:
            logger.warning("Invalid limit %s provided for table sample, defaulting to 10.", limit)
            limit = 10

        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        logger.debug("Executing query: %s", query)
        cursor.execute(query)

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []
        logger.info("Fetched %d rows from table '%s' with columns: %s", len(results), table_name, column_names)

        if not results:
            logger.info("No data found in table '%s' for sample.", table_name)
            return f"-- No data found in table '{table_name}' or table does not exist. --"

        # Convert results to list of dictionaries
        structured_results = [dict(zip(column_names, row)) for row in results]
        logger.debug("Returning %d structured results for '%s'.", len(structured_results), table_name)
        return structured_results

    except InvalidInputError:
        raise # Propagate invalid input errors directly
    except DatabaseConnectionError:
        raise # Propagate connection errors directly
    except Exception as e:
        # Assume other errors are query execution related for this function
        logger.error("Error executing sample query for table '%s': %s", table_name, e, exc_info=True)
        raise QueryExecutionError(f"Failed to execute sample query for table '{table_name}': {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after table sample fetch.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def get_table_sample(table_name: str, limit: int = 10) -> QueryResult:
    """Fetches a sample of data from a table asynchronously.

    Returns:
        Union[List[Dict[str, Any]], str]: List of dicts on success, error/message string otherwise.
    """
    logger.info("Received request for table sample: table='%s', limit=%d", table_name, limit)
    # Let exceptions propagate up
    return await asyncio.to_thread(_fetch_table_sample_sync, table_name, limit)

def _execute_sql_sync(sql: str) -> QueryResult:
    """Synchronous helper function to execute a SQL query and return list of dicts or error string.

    Returns:
        Union[List[Dict[str, Any]], str]: List of dicts on success, error/message string otherwise.

    Raises:
        InvalidInputError: If SQL query is not a SELECT query.
        DatabaseConnectionError: If unable to establish database connection.
        QueryExecutionError: If unable to execute query.
    """
    logger.debug("Starting synchronous SQL execution: %s...", sql[:100])
    clean_sql = sql.strip().upper()
    if not clean_sql.startswith("SELECT"):
        logger.warning("Rejected non-SELECT query: %s...", sql[:100])
        raise InvalidInputError("Only SELECT queries are currently allowed for safety.")

    conn = None
    try:
        conn = create_vast_connection()
        if not conn:
            return "Error: Failed to establish database connection for SQL query execution."
        cursor = conn.cursor()

        logger.debug("Executing validated SQL query.")
        cursor.execute(sql)

        if cursor.description is None:
            logger.info("SQL query executed, but no description/results returned.")
            return "-- Query executed, but no results returned (or not a SELECT query). --"

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        logger.info("SQL query returned %d rows with columns: %s", len(results), column_names)

        if not results:
            logger.info("SQL query returned no rows.")
            return "-- Query executed successfully, but returned no rows. --"

        # Convert results to list of dictionaries
        structured_results = [dict(zip(column_names, row)) for row in results]
        logger.debug("Returning %d structured results for SQL query.", len(structured_results))
        return structured_results

    except InvalidInputError:
        raise # Propagate invalid input errors directly
    except DatabaseConnectionError:
        raise # Propagate connection errors directly
    except Exception as e:
        logger.error("Error executing SQL query: %s", e, exc_info=True)
        raise QueryExecutionError(f"Error executing SQL query: {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after SQL query execution.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def execute_sql_query(sql: str) -> QueryResult:
    """Executes a read-only SQL query asynchronously.

    Returns:
        Union[List[Dict[str, Any]], str]: List of dicts on success, error/message string otherwise.
    """
    logger.info("Received request to execute SQL query: %s...", sql[:100])
    # Let exceptions propagate up
    return await asyncio.to_thread(_execute_sql_sync, sql)
