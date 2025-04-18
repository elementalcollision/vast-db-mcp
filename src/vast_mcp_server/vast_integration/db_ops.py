import vastdb
import asyncio
import csv
import io
import logging # Import logging
import sqlparse # Added for query validation
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
# Define type alias for structured Table Metadata
TableMetadataResult = Dict[str, Any]

def create_vast_connection(access_key: str, secret_key: str) -> vastdb.api.VastSession:
    """Creates and returns a VAST DB connection session using provided credentials.

    Uses the endpoint from config, but requires access/secret keys.

    Args:
        access_key: The VAST DB access key.
        secret_key: The VAST DB secret key.

    Returns:
        vastdb.api.VastSession: An active VAST DB session.

    Raises:
        DatabaseConnectionError: If connection fails.
        ValueError: If keys are missing.
    """
    if not access_key or not secret_key:
        logger.error("Missing VAST DB access key or secret key for connection.")
        raise ValueError("Access key and secret key are required.")

    # Endpoint still comes from config, but keys are passed in
    endpoint = config.VAST_DB_ENDPOINT
    logger.debug("Attempting to connect to VAST DB at %s with provided keys.", endpoint)
    try:
        # Adjust based on the actual VAST DB SDK's connect function signature.
        conn = vastdb.connect(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            # Add any other necessary connection parameters here, e.g., verify_ssl=False
        )
        logger.info("Successfully connected to VAST DB.")
        return conn
    except Exception as e:
        logger.error("Connection to VAST DB failed: %s", e, exc_info=True)
        # Wrap the original exception
        raise DatabaseConnectionError(f"Failed to connect to VAST DB: {e}", original_exception=e)


# --- Database Operations ---

def _fetch_schema_sync(access_key: str, secret_key: str) -> SchemaResult:
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
        conn = create_vast_connection(access_key, secret_key)
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

async def get_db_schema(access_key: str, secret_key: str) -> SchemaResult:
    """Fetches the database schema asynchronously using provided credentials."""
    logger.info("Received request to fetch DB schema.")
    return await asyncio.to_thread(_fetch_schema_sync, access_key, secret_key)

def _list_tables_sync(access_key: str, secret_key: str) -> List[str]:
    """Synchronous helper to fetch table names using provided credentials."""
    logger.debug("Starting synchronous table list fetch.")
    conn = None
    try:
        conn = create_vast_connection(access_key, secret_key) # Use provided keys
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

async def list_tables(access_key: str, secret_key: str) -> List[str]:
    """Fetches a list of table names asynchronously using provided credentials."""
    logger.info("Received request to list tables.")
    return await asyncio.to_thread(_list_tables_sync, access_key, secret_key)

def _get_table_metadata_sync(table_name: str, access_key: str, secret_key: str) -> TableMetadataResult:
    """Synchronous helper to get metadata for a specific table using provided credentials."""
    logger.debug("Starting synchronous metadata fetch for table '%s'.", table_name)
    conn = None
    try:
        # Basic input validation
        if not table_name.isidentifier():
            logger.warning("Invalid table name requested for metadata: %s", table_name)
            raise InvalidInputError(f"Invalid table name '{table_name}'.")

        conn = create_vast_connection(access_key, secret_key) # Use provided keys
        cursor = conn.cursor()

        logger.debug("Describing table: %s", table_name)
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        columns_raw = cursor.fetchall()
        if not columns_raw:
            logger.warning("DESCRIBE TABLE %s returned no columns.", table_name)
            raise TableDescribeError(f"Could not retrieve column information for table '{table_name}' (table might not exist or is empty).")

        # Attempt to parse more details from DESCRIBE output
        # Assuming format: (name, type, nullable, key, default, extra)
        # Adjust indices based on actual VAST DB output if known
        columns_metadata = []
        for col in columns_raw:
            if not col or len(col) < 2:
                continue # Skip malformed rows
            
            col_meta = {
                "name": col[0],
                "type": col[1],
                # Attempt to parse optional fields gracefully
                "is_nullable": col[2] if len(col) > 2 else None, # e.g., 'YES'/'NO' or True/False?
                "key": col[3] if len(col) > 3 else None,       # e.g., 'PRI', 'UNI', 'MUL'
                "default": col[4] if len(col) > 4 else None,   # Default value as string or None
                # "extra": col[5] if len(col) > 5 else None    # e.g., 'auto_increment'
            }
            # Clean up potential None values if desired, or keep them explicitly
            # col_meta = {k: v for k, v in col_meta.items() if v is not None}
            columns_metadata.append(col_meta)

        metadata = {
            "table_name": table_name,
            "columns": columns_metadata
        }
        logger.info("Successfully described table '%s'. Found %d columns.", table_name, len(columns_metadata))
        return metadata

    except InvalidInputError:
        raise # Propagate invalid input directly
    except DatabaseConnectionError:
        raise # Propagate connection errors directly
    except Exception as e:
        # Treat other exceptions during describe as TableDescribeError
        logger.error("Error describing table '%s': %s", table_name, e, exc_info=True)
        raise TableDescribeError(f"Failed to describe table '{table_name}': {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after metadata fetch.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def get_table_metadata(table_name: str, access_key: str, secret_key: str) -> TableMetadataResult:
    """Fetches metadata for a specific table asynchronously using provided credentials."""
    logger.info("Received request for metadata for table: %s", table_name)
    return await asyncio.to_thread(_get_table_metadata_sync, table_name, access_key, secret_key)

def _fetch_table_sample_sync(table_name: str, limit: int, access_key: str, secret_key: str) -> QueryResult:
    """Synchronous helper function to fetch table sample data using provided credentials."""
    logger.debug("Starting synchronous table sample fetch for table '%s' limit %d.", table_name, limit)
    conn = None
    try:
        conn = create_vast_connection(access_key, secret_key) # Use provided keys
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

async def get_table_sample(table_name: str, limit: int, access_key: str, secret_key: str) -> QueryResult:
    """Fetches a sample of data from a table asynchronously using provided credentials."""
    logger.info("Received request for table sample: table='%s', limit=%d", table_name, limit)
    return await asyncio.to_thread(_fetch_table_sample_sync, table_name, limit, access_key, secret_key)

def _execute_sql_sync(sql: str, access_key: str, secret_key: str) -> QueryResult:
    """Synchronous helper function to execute SQL query using provided credentials."""
    logger.debug("Starting synchronous SQL execution: %s...", sql[:100])

    # --- Query Validation using sqlparse ---
    try:
        # Parse the SQL. sqlparse returns a list of statements.
        parsed_statements = sqlparse.parse(sql)

        # Check if parsing produced anything meaningful
        if not parsed_statements:
            logger.warning("SQL parsing resulted in an empty statement list: %s", sql)
            raise InvalidInputError("Invalid or empty SQL query provided.")

        # For now, we only support a single statement per request
        if len(parsed_statements) > 1:
            logger.warning("Rejected multi-statement SQL query: %s...", sql[:100])
            raise InvalidInputError("Multi-statement SQL queries are not allowed.")

        statement = parsed_statements[0]
        statement_type = statement.get_type()

        # Allow only configured statement types
        allowed_types = config.ALLOWED_SQL_TYPES
        if statement_type not in allowed_types:
            logger.warning("Rejected non-allowed query type '%s': %s...", statement_type, sql[:100])
            # Dynamically generate the error message based on configured allowed types
            allowed_str = ", ".join(allowed_types)
            raise InvalidInputError(f"Query type '{statement_type}' is not allowed. Allowed types: {allowed_str}.")

        logger.debug("SQL query validated as type: %s (Allowed: %s)", statement_type, ", ".join(allowed_types))

    except InvalidInputError:
        raise # Re-raise our specific validation errors
    except Exception as parse_e:
        # Catch potential errors during parsing itself
        logger.error("Error parsing SQL query: %s", parse_e, exc_info=True)
        raise InvalidInputError(f"Failed to parse SQL query: {parse_e}")
    # --- End Query Validation ---

    conn = None
    try:
        conn = create_vast_connection(access_key, secret_key) # Use provided keys
        if not conn:
            # This condition might be less likely if create_vast_connection raises reliably
            logger.error("Failed to establish database connection for SQL execution (conn is None).")
            raise DatabaseConnectionError("Failed to establish database connection.")

        cursor = conn.cursor()

        logger.debug("Executing validated SQL query.")
        cursor.execute(sql) # Execute the original, validated SQL

        # Check if the query was meant to return results (e.g., SELECT)
        if cursor.description is None:
            logger.info("SQL query executed, but did not return results (e.g., non-SELECT or empty result).")
            # Check if it was a SELECT that genuinely returned nothing vs. another type (though we block others)
            if statement_type == 'SELECT':
                return "-- Query executed successfully, but returned no rows. --"
            else:
                # This path shouldn't be reached with current validation, but good practice
                return "-- Query executed, but it was not a type that returns rows. --"

        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        logger.info("SQL query returned %d rows with columns: %s", len(results), column_names)

        # Convert results to list of dictionaries
        structured_results = [dict(zip(column_names, row)) for row in results]
        logger.debug("Returning %d structured results for SQL query.", len(structured_results))
        return structured_results

    except InvalidInputError:
        raise # Propagate our own validation errors
    except DatabaseConnectionError:
        raise # Propagate connection errors
    except Exception as e:
        # Catch errors during VAST DB execution (e.g., syntax errors VAST finds)
        logger.error("Error executing SQL query in VAST DB: %s", e, exc_info=True)
        # Map VAST DB execution errors to QueryExecutionError
        raise QueryExecutionError(f"Error executing SQL query: {e}", original_exception=e)
    finally:
        if conn:
            try:
                logger.debug("Closing VAST DB connection after SQL query execution.")
                conn.close()
            except Exception as close_e:
                logger.warning("Error closing VAST DB connection: %s", close_e, exc_info=True)

async def execute_sql_query(sql: str, access_key: str, secret_key: str) -> QueryResult:
    """Executes a SQL query asynchronously using provided credentials."""
    logger.info("Received request to execute SQL query: %s...", sql[:100])
    return await asyncio.to_thread(_execute_sql_sync, sql, access_key, secret_key)
