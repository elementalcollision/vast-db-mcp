import pytest
from unittest.mock import MagicMock, patch
import csv
import io

# Since we configured pythonpath = ["src"] in pyproject.toml,
# we can import directly from vast_mcp_server
from vast_mcp_server.vast_integration import db_ops
from vast_mcp_server import config

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio


# Helper to create mock cursor/connection for schema tests
def _get_mock_conn_cursor(mocker):
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_create_conn = mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection')
    mock_create_conn.return_value = mock_conn
    return mock_create_conn, mock_conn, mock_cursor


# --- Tests for create_vast_connection --- #

def test_create_vast_connection_success(mocker):
    """Test successful database connection."""
    # Arrange
    mock_connect = mocker.patch('vastdb.connect') # Patch vastdb.connect
    mock_conn_obj = MagicMock()
    mock_connect.return_value = mock_conn_obj

    # Act
    conn = db_ops.create_vast_connection()

    # Assert
    mock_connect.assert_called_once_with(
        endpoint=config.VAST_DB_ENDPOINT,
        access_key=config.VAST_ACCESS_KEY,
        secret_key=config.VAST_SECRET_KEY,
    )
    assert conn is mock_conn_obj

def test_create_vast_connection_failure(mocker):
    """Test database connection failure."""
    # Arrange
    mock_connect = mocker.patch('vastdb.connect')
    test_exception = ConnectionError("Test DB connection failed")
    mock_connect.side_effect = test_exception

    # Act & Assert
    with pytest.raises(ConnectionError, match="Test DB connection failed"):
        db_ops.create_vast_connection()

    mock_connect.assert_called_once()


# --- Tests for get_db_schema --- #

async def test_get_db_schema_success(mocker):
    """Test successful schema fetching with multiple tables."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)

    # Configure cursor mock return values based on expected calls
    def execute_side_effect(sql):
        if sql == "SHOW TABLES":
            mock_cursor.fetchall.return_value = [('table1',), ('table2',)]
        elif sql == "DESCRIBE TABLE table1":
            mock_cursor.fetchall.return_value = [('col1', 'INT', ...), ('col2', 'VARCHAR', ...)]
        elif sql == "DESCRIBE TABLE table2":
            mock_cursor.fetchall.return_value = [('id', 'BIGINT', ...), ('data', 'TEXT', ...)]
        else:
            mock_cursor.fetchall.return_value = [] # Default empty for unexpected calls
        return None # execute itself doesn't return anything

    mock_cursor.execute.side_effect = execute_side_effect

    # Act
    schema_output = await db_ops.get_db_schema()

    # Assert
    expected_output = (
        "TABLE: table1\n"
        "  - col1 (INT)\n"
        "  - col2 (VARCHAR)\n"
        "\n"
        "TABLE: table2\n"
        "  - id (BIGINT)\n"
        "  - data (TEXT)\n"
        "\n"
    )
    assert schema_output == expected_output
    mock_create_conn.assert_called_once()
    assert mock_cursor.execute.call_count == 3 # SHOW TABLES + 2 DESCRIBE
    mock_conn.close.assert_called_once()

async def test_get_db_schema_no_tables(mocker):
    """Test schema fetching when no tables are found."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.execute.side_effect = lambda sql: None # Just need execute to run
    mock_cursor.fetchall.return_value = [] # SHOW TABLES returns empty list

    # Act
    schema_output = await db_ops.get_db_schema()

    # Assert
    assert schema_output == "-- No tables found or unable to describe tables. --"
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    mock_conn.close.assert_called_once()

async def test_get_db_schema_describe_error(mocker):
    """Test handling of an error when describing a specific table."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    describe_exception = Exception("Describe permission denied")

    def execute_side_effect(sql):
        if sql == "SHOW TABLES":
            mock_cursor.fetchall.return_value = [('table1',), ('sensitive_table',)]
        elif sql == "DESCRIBE TABLE table1":
            mock_cursor.fetchall.return_value = [('col1', 'INT', ...)]
        elif sql == "DESCRIBE TABLE sensitive_table":
            raise describe_exception # Error describing this table
        else:
            mock_cursor.fetchall.return_value = []
        return None

    mock_cursor.execute.side_effect = execute_side_effect

    # Act
    schema_output = await db_ops.get_db_schema()

    # Assert
    expected_output = (
        "TABLE: table1\n"
        "  - col1 (INT)\n"
        "\n"
        "TABLE: sensitive_table\n"
        f"  - Error describing table: {describe_exception}\n"
        "\n"
    )
    assert schema_output == expected_output
    mock_create_conn.assert_called_once()
    assert mock_cursor.execute.call_count == 3
    mock_conn.close.assert_called_once()

async def test_get_db_schema_show_tables_error(mocker):
    """Test handling of an error during the initial SHOW TABLES query."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    show_tables_exception = Exception("Cannot list tables")
    mock_cursor.execute.side_effect = show_tables_exception

    # Act
    schema_output = await db_ops.get_db_schema()

    # Assert
    expected_error_msg = f"Error fetching schema from VAST DB: {show_tables_exception}"
    assert schema_output == expected_error_msg
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # Connection might not be closed if exception happens early in try block
    # Depending on exact logic, conn.close() might or might not be called.
    # For robustness, we might not assert close() here, or check specifically.
    # Let's assert it was called, assuming the finally block runs.
    mock_conn.close.assert_called_once()

async def test_get_db_schema_connection_error(mocker):
    """Test handling of a connection error."""
    # Arrange
    conn_exception = ConnectionError("Failed to connect")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act
    schema_output = await db_ops.get_db_schema()

    # Assert
    # The error happens in the sync part, wrapped by the async function
    expected_error_msg = f"Error fetching schema from VAST DB: {conn_exception}"
    assert schema_output == expected_error_msg
    mock_create_conn.assert_called_once()


# --- Tests for get_table_sample --- #

async def test_get_table_sample_success(mocker):
    """Test successful table sample fetching returns list[dict]."""
    # Arrange
    table_name = "my_table"
    limit = 5
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = [('id',), ('value',)]
    mock_cursor.fetchall.return_value = [(1, 'abc'), (2, 'def')]

    # Act
    result = await db_ops.get_table_sample(table_name, limit)

    # Assert
    expected_result = [
        {'id': 1, 'value': 'abc'},
        {'id': 2, 'value': 'def'}
    ]
    assert result == expected_result # Check for list[dict]
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    mock_conn.close.assert_called_once()

async def test_get_table_sample_no_data(mocker):
    """Test table sample fetching when table is empty or not found."""
    # Arrange
    table_name = "empty_table"
    limit = 10
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = [('colA',)] # Has columns but no data
    mock_cursor.fetchall.return_value = [] # No rows

    # Act
    output = await db_ops.get_table_sample(table_name, limit)

    # Assert
    expected_msg = f"-- No data found in table '{table_name}' or table does not exist. --"
    assert output == expected_msg
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    mock_conn.close.assert_called_once()

async def test_get_table_sample_invalid_table_name(mocker):
    """Test handling of invalid table name (potential SQL injection)."""
    # Arrange
    table_name = "invalid-name; DROP TABLES"
    limit = 10
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)

    # Act
    output = await db_ops.get_table_sample(table_name, limit)

    # Assert
    expected_msg = f"Error: Invalid table name '{table_name}'."
    assert output == expected_msg
    mock_create_conn.assert_called_once() # Connection is made before validation
    mock_cursor.execute.assert_not_called() # Execute should not be called
    mock_conn.close.assert_called_once()

@pytest.mark.parametrize("invalid_limit", [-1, 0, "abc", None])
async def test_get_table_sample_invalid_limit_defaults_to_10(mocker, invalid_limit):
    """Test that invalid limit values default to 10."""
    # Arrange
    table_name = "some_table"
    default_limit = 10
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = [(1,)]

    # Act
    result = await db_ops.get_table_sample(table_name, invalid_limit)

    # Assert
    expected_result = [{'id': 1}]
    assert result == expected_result # Check it still processed correctly
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {default_limit}")
    mock_conn.close.assert_called_once()

async def test_get_table_sample_execution_error(mocker):
    """Test handling of error during SQL execution."""
    # Arrange
    table_name = "error_table"
    limit = 10
    sql_exception = Exception("Syntax error near FROM")
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.execute.side_effect = sql_exception

    # Act
    output = await db_ops.get_table_sample(table_name, limit)

    # Assert
    expected_msg = f"Error fetching sample data for table '{table_name}' from VAST DB: {sql_exception}"
    assert output == expected_msg
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    mock_conn.close.assert_called_once()

async def test_get_table_sample_connection_error(mocker):
    """Test handling of connection error for table sample."""
    # Arrange
    table_name = "any_table"
    limit = 10
    conn_exception = ConnectionError("Cannot connect to DB")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act
    output = await db_ops.get_table_sample(table_name, limit)

    # Assert
    expected_msg = f"Error fetching sample data for table '{table_name}' from VAST DB: {conn_exception}"
    assert output == expected_msg
    mock_create_conn.assert_called_once()


# --- Tests for execute_sql_query --- #

async def test_execute_sql_query_success(mocker):
    """Test successful execution of a SELECT query returns list[dict]."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = [('id',), ('name',)]
    mock_cursor.fetchall.return_value = [(1, 'Alice')]

    # Act
    result = await db_ops.execute_sql_query(sql)

    # Assert
    expected_result = [{'id': 1, 'name': 'Alice'}]
    assert result == expected_result # Check for list[dict]
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.close.assert_called_once()

@pytest.mark.parametrize("non_select_sql", [
    "INSERT INTO users (id, name) VALUES (2, 'Bob')",
    "UPDATE users SET name = 'Charlie' WHERE id = 1",
    "DELETE FROM users WHERE id = 1",
    "DROP TABLE users",
    " create table new_t (c int);",
    "-- SELECT * FROM users; \nDROP TABLE users;",
])
async def test_execute_sql_query_rejects_non_select(mocker, non_select_sql):
    """Test that non-SELECT statements are rejected."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)

    # Act
    output = await db_ops.execute_sql_query(non_select_sql)

    # Assert
    assert output == "Error: Only SELECT queries are currently allowed for safety."
    # Non-select check now happens *before* connection
    mock_create_conn.assert_not_called()
    mock_cursor.execute.assert_not_called()
    mock_conn.close.assert_not_called()

async def test_execute_sql_query_empty_result(mocker):
    """Test SELECT query that returns no rows."""
    # Arrange
    sql = "SELECT id FROM users WHERE name = 'NonExistent'"
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = [] # No rows

    # Act
    output = await db_ops.execute_sql_query(sql)

    # Assert
    assert output == "-- Query executed successfully, but returned no rows. --"
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.close.assert_called_once()

async def test_execute_sql_query_no_description(mocker):
    """Test query where cursor.description is None after execute."""
    # Arrange
    sql = "SELECT * FROM some_view" # Example
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.description = None # Simulate no description

    # Act
    output = await db_ops.execute_sql_query(sql)

    # Assert
    expected_msg = "-- Query executed, but no results returned (or not a SELECT query). --"
    assert output == expected_msg
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.close.assert_called_once()

async def test_execute_sql_query_execution_error(mocker):
    """Test handling of an error during SQL execution."""
    # Arrange
    sql = "SELECT bad_col FROM users"
    sql_exception = Exception("Column 'bad_col' not found")
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.execute.side_effect = sql_exception

    # Act
    output = await db_ops.execute_sql_query(sql)

    # Assert
    expected_msg = f"Error executing SQL query in VAST DB: {sql_exception}"
    assert output == expected_msg
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.close.assert_called_once()

async def test_execute_sql_query_connection_error(mocker):
    """Test handling of a connection error."""
    # Arrange
    sql = "SELECT 1"
    conn_exception = ConnectionError("DB connection failed")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act
    output = await db_ops.execute_sql_query(sql)

    # Assert
    expected_msg = f"Error executing SQL query in VAST DB: {conn_exception}"
    assert output == expected_msg
    mock_create_conn.assert_called_once()

    # TODO: Add tests for execute_sql_query 