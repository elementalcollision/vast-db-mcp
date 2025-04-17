import pytest
from unittest.mock import MagicMock, patch
import csv
import io

# Since we configured pythonpath = ["src"] in pyproject.toml,
# we can import directly from vast_mcp_server
from vast_mcp_server.vast_integration import db_ops
from vast_mcp_server import config
# Import custom exceptions to test for them
from vast_mcp_server.exceptions import (
    DatabaseConnectionError,
    SchemaFetchError,
    TableDescribeError,
    QueryExecutionError,
    InvalidInputError
)

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
    """Test database connection failure raises DatabaseConnectionError."""
    # Arrange
    mock_connect = mocker.patch('vastdb.connect')
    original_exception = ConnectionError("Network Error")
    mock_connect.side_effect = original_exception

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        db_ops.create_vast_connection()

    assert "Failed to connect to VAST DB" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
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
    assert schema_output == "-- No tables found in the database. --" # Updated message
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    mock_conn.close.assert_called_once()

async def test_get_db_schema_describe_error_returns_partial(mocker):
    """Test schema fetch still returns partial schema even if describe fails."""
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

    # Assert - Check that the error message is embedded in the output
    expected_output = (
        "TABLE: table1\n"
        "  - col1 (INT)\n"
        "\n"
        "TABLE: sensitive_table\n"
        f"  - !!! Error describing table: {describe_exception} !!!\n"
        "\n"
    )
    assert schema_output == expected_output
    mock_create_conn.assert_called_once()
    assert mock_cursor.execute.call_count == 3
    mock_conn.close.assert_called_once()

async def test_get_db_schema_show_tables_error_raises(mocker):
    """Test error during SHOW TABLES raises SchemaFetchError."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    original_exception = Exception("Cannot list tables")
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(SchemaFetchError) as excinfo:
        await db_ops.get_db_schema()

    assert "Error fetching schema" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    mock_conn.close.assert_called_once()

async def test_get_db_schema_connection_error_raises(mocker):
    """Test connection error raises DatabaseConnectionError."""
    # Arrange
    conn_exception = DatabaseConnectionError("Failed to connect")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.get_db_schema()

    assert excinfo.value is conn_exception
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

async def test_get_table_sample_invalid_table_name_raises(mocker):
    """Test invalid table name raises InvalidInputError."""
    # Arrange
    table_name = "invalid-name;" # Just needs to fail isidentifier
    limit = 10
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)

    # Act & Assert
    with pytest.raises(InvalidInputError) as excinfo:
        await db_ops.get_table_sample(table_name, limit)

    assert f"Invalid table name '{table_name}'" in str(excinfo.value)
    # Validation happens before connection in this case
    mock_create_conn.assert_not_called()
    mock_cursor.execute.assert_not_called()
    mock_conn.close.assert_not_called()

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

async def test_get_table_sample_execution_error_raises(mocker):
    """Test SQL execution error raises QueryExecutionError."""
    # Arrange
    table_name = "error_table"
    limit = 10
    original_exception = Exception("Syntax error")
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(QueryExecutionError) as excinfo:
        await db_ops.get_table_sample(table_name, limit)

    assert f"Failed to execute sample query for table '{table_name}'" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    mock_conn.close.assert_called_once()

async def test_get_table_sample_connection_error_raises(mocker):
    """Test connection error raises DatabaseConnectionError."""
    # Arrange
    table_name = "any_table"
    limit = 10
    conn_exception = DatabaseConnectionError("Cannot connect to DB")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.get_table_sample(table_name, limit)

    assert excinfo.value is conn_exception
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
async def test_execute_sql_query_rejects_non_select_raises(mocker, non_select_sql):
    """Test non-SELECT statements raise InvalidInputError."""
    # Arrange
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)

    # Act & Assert
    with pytest.raises(InvalidInputError) as excinfo:
        await db_ops.execute_sql_query(non_select_sql)

    assert "Only SELECT queries are currently allowed" in str(excinfo.value)
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

async def test_execute_sql_query_execution_error_raises(mocker):
    """Test SQL execution error raises QueryExecutionError."""
    # Arrange
    sql = "SELECT bad_col FROM users"
    original_exception = Exception("Column 'bad_col' not found")
    mock_create_conn, mock_conn, mock_cursor = _get_mock_conn_cursor(mocker)
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(QueryExecutionError) as excinfo:
        await db_ops.execute_sql_query(sql)

    assert "Error executing SQL query" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_create_conn.assert_called_once()
    mock_cursor.execute.assert_called_once_with(sql)
    mock_conn.close.assert_called_once()

async def test_execute_sql_query_connection_error_raises(mocker):
    """Test connection error raises DatabaseConnectionError."""
    # Arrange
    sql = "SELECT 1"
    conn_exception = DatabaseConnectionError("DB connection failed")
    mock_create_conn = mocker.patch(
        'vast_mcp_server.vast_integration.db_ops.create_vast_connection',
        side_effect=conn_exception
    )

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.execute_sql_query(sql)

    assert excinfo.value is conn_exception
    mock_create_conn.assert_called_once()

    # TODO: Add tests for execute_sql_query 