import vastdb
import asyncio
import csv
import io
from .. import config  # Import configuration from the parent package


def create_vast_connection() -> vastdb.api.VastSession:
    """Creates and returns a VAST DB connection session.

    Uses connection details defined in the config module.

    Returns:
        vastdb.api.VastSession: An active VAST DB session.

    Raises:
        Exception: If connection fails (specific exception type may vary based on SDK).
    """
    try:
        # Assuming vastdb.connect uses these parameters.
        # Adjust based on the actual VAST DB SDK's connect function signature.
        conn = vastdb.connect(
            endpoint=config.VAST_DB_ENDPOINT,
            access_key=config.VAST_ACCESS_KEY,
            secret_key=config.VAST_SECRET_KEY,
            # Add any other necessary connection parameters here, e.g., verify_ssl=False
        )
        print("Successfully connected to VAST DB.") # Optional: for debugging
        return conn
    except Exception as e:
        print(f"Error connecting to VAST DB: {e}")
        # Re-raise the exception or handle it more gracefully depending on requirements
        raise


# --- Database Operations ---

def _fetch_schema_sync() -> str:
    """Synchronous helper function to fetch and format schema."""
    conn = None
    try:
        conn = create_vast_connection()
        cursor = conn.cursor()

        # Fetch table names (adjust SQL if needed for VAST DB)
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        # Assuming fetchall returns a list of tuples, e.g., [('table1',), ('table2',)]
        table_names = [t[0] for t in tables if t]

        schema_parts = []
        for table_name in table_names:
            # Fetch columns for each table (adjust SQL if needed for VAST DB)
            # Using DESCRIBE or similar command
            try:
                cursor.execute(f"DESCRIBE TABLE {table_name}") # Or SHOW COLUMNS FROM table_name
                columns = cursor.fetchall()
                # Assuming fetchall returns list like [('col1', 'type1', ...), ('col2', 'type2', ...)]
                schema_parts.append(f"TABLE: {table_name}")
                col_defs = [f"  - {col[0]} ({col[1]})" for col in columns]
                schema_parts.extend(col_defs)
                schema_parts.append("") # Add spacing
            except Exception as desc_e:
                schema_parts.append(f"TABLE: {table_name}")
                schema_parts.append(f"  - Error describing table: {desc_e}")
                schema_parts.append("")

        if not schema_parts:
            return "-- No tables found or unable to describe tables. --"

        return "\n".join(schema_parts)

    except Exception as e:
        return f"Error fetching schema from VAST DB: {e}"
    finally:
        if conn:
            try:
                conn.close() # Ensure connection is closed
            except Exception as close_e:
                print(f"Error closing VAST DB connection: {close_e}")

async def get_db_schema() -> str:
    """Fetches the database schema asynchronously.

    Returns:
        str: A string representation of the schema.
    """
    # Run the synchronous database operations in a separate thread
    return await asyncio.to_thread(_fetch_schema_sync)

def _fetch_table_sample_sync(table_name: str, limit: int) -> str:
    """Synchronous helper function to fetch and format table sample data."""
    conn = None
    try:
        conn = create_vast_connection()
        cursor = conn.cursor()

        # Basic input validation (consider more robust validation/sanitization)
        if not table_name.isidentifier(): # Simple check against SQL injection
             return f"Error: Invalid table name '{table_name}'."
        if not isinstance(limit, int) or limit <= 0:
            limit = 10 # Default to 10 if limit is invalid

        # Execute query
        # WARNING: Directly formatting table names can be risky if table_name isn't validated thoroughly.
        # Using parameterized queries is generally safer if the DBAPI driver supports them for identifiers.
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        cursor.execute(query)

        # Fetch results and column names
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

        if not results:
            return f"-- No data found in table '{table_name}' or table does not exist. --"

        # Format results as CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)
        writer.writerows(results)
        return output.getvalue()

    except Exception as e:
        # Catch specific VAST DB exceptions if known, otherwise generic Exception
        return f"Error fetching sample data for table '{table_name}' from VAST DB: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception as close_e:
                print(f"Error closing VAST DB connection: {close_e}")

async def get_table_sample(table_name: str, limit: int = 10) -> str:
    """Fetches a sample of data from a table asynchronously.

    Args:
        table_name (str): The name of the table.
        limit (int): The maximum number of rows to fetch.

    Returns:
        str: A string representation of the sample data (e.g., CSV).
    """
    # Run the synchronous database operations in a separate thread
    return await asyncio.to_thread(_fetch_table_sample_sync, table_name, limit)

def _execute_sql_sync(sql: str) -> str:
    """Synchronous helper function to execute a SQL query and format results."""
    # Security Check: Only allow SELECT statements for now
    # Consider making this configurable or using more sophisticated parsing/allow-listing
    if not sql.strip().upper().startswith("SELECT"):
        return "Error: Only SELECT queries are currently allowed for safety."

    conn = None
    try:
        conn = create_vast_connection()
        cursor = conn.cursor()

        # Execute the validated query
        cursor.execute(sql)

        # Check if the query was a SELECT or similar that returns results
        if cursor.description is None:
            # Might be an empty result set or a non-select query that slipped through
            # Or a DDL/DML if the check above was bypassed/modified
            return "-- Query executed, but no results returned (or not a SELECT query). --"

        # Fetch results and column names
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        if not results:
            return "-- Query executed successfully, but returned no rows. --"

        # Format results as CSV string
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(column_names)
        writer.writerows(results)
        return output.getvalue()

    except Exception as e:
        # Catch specific VAST DB/SQL exceptions if possible
        return f"Error executing SQL query in VAST DB: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except Exception as close_e:
                print(f"Error closing VAST DB connection: {close_e}")

async def execute_sql_query(sql: str) -> str:
    """Executes a read-only SQL query asynchronously.

    Args:
        sql (str): The SQL query to execute (must be SELECT).

    Returns:
        str: A string representation of the query results (e.g., CSV) or an error message.
    """
    # Run the synchronous database operations in a separate thread
    return await asyncio.to_thread(_execute_sql_sync, sql)
