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


# Helper to create mock connection and cursor
def _get_mock_conn_and_cursor():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# --- Tests for create_vast_connection --- #
# These tests are removed as create_vast_connection is removed.

# --- Tests for get_db_schema --- #

async def test_get_db_schema_success(mocker):
    """Test successful schema fetching with multiple tables."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()

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
    schema_output = await db_ops.get_db_schema(mock_conn)

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
    assert mock_cursor.execute.call_count == 3 # SHOW TABLES + 2 DESCRIBE
    # mock_conn.close() is no longer called by db_ops

async def test_get_db_schema_no_tables(mocker):
    """Test schema fetching when no tables are found."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.execute.side_effect = lambda sql: None # Just need execute to run
    mock_cursor.fetchall.return_value = [] # SHOW TABLES returns empty list

    # Act
    schema_output = await db_ops.get_db_schema(mock_conn)

    # Assert
    assert schema_output == "-- No tables found in the database. --"
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # mock_conn.close() is no longer called by db_ops

async def test_get_db_schema_describe_error_returns_partial(mocker):
    """Test schema fetch still returns partial schema even if describe fails."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
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
    schema_output = await db_ops.get_db_schema(mock_conn)

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
    assert mock_cursor.execute.call_count == 3
    # mock_conn.close() is no longer called by db_ops

async def test_get_db_schema_show_tables_error_raises(mocker):
    """Test error during SHOW TABLES raises SchemaFetchError."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    original_exception = Exception("Cannot list tables")
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(SchemaFetchError) as excinfo:
        await db_ops.get_db_schema(mock_conn)

    assert "Error fetching schema" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # mock_conn.close() is no longer called by db_ops

async def test_get_db_schema_invalid_connection_raises(mocker):
    """Test invalid connection (e.g., None) raises DatabaseConnectionError."""
    # Arrange
    mock_conn = None # Simulate a bad connection from context

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.get_db_schema(mock_conn)
    assert "Provided database connection is invalid" in str(excinfo.value)


# --- Tests for get_table_sample --- #

async def test_get_table_sample_success(mocker):
    """Test successful table sample fetching returns list[dict]."""
    # Arrange
    table_name = "my_table"
    limit = 5
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = [('id',), ('value',)]
    mock_cursor.fetchall.return_value = [(1, 'abc'), (2, 'def')]

    # Act
    result = await db_ops.get_table_sample(mock_conn, table_name, limit)

    # Assert
    expected_result = [
        {'id': 1, 'value': 'abc'},
        {'id': 2, 'value': 'def'}
    ]
    assert result == expected_result
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    # mock_conn.close() is no longer called by db_ops

async def test_get_table_sample_no_data(mocker):
    """Test table sample fetching when table is empty or not found."""
    # Arrange
    table_name = "empty_table"
    limit = 10
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = [('colA',)]
    mock_cursor.fetchall.return_value = []

    # Act
    output = await db_ops.get_table_sample(mock_conn, table_name, limit)

    # Assert
    expected_msg = f"-- No data found in table '{table_name}' or table does not exist. --"
    assert output == expected_msg
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    # mock_conn.close() is no longer called by db_ops

async def test_get_table_sample_invalid_table_name_raises(mocker):
    """Test invalid table name raises InvalidInputError."""
    # Arrange
    table_name = "invalid-name;"
    limit = 10
    mock_conn, mock_cursor = _get_mock_conn_and_cursor() # conn is not used by validation

    # Act & Assert
    with pytest.raises(InvalidInputError) as excinfo:
        await db_ops.get_table_sample(mock_conn, table_name, limit)

    assert f"Invalid table name '{table_name}'" in str(excinfo.value)
    mock_cursor.execute.assert_not_called() # execute should not be called

@pytest.mark.parametrize("invalid_limit", [-1, 0, "abc", None])
async def test_get_table_sample_invalid_limit_defaults_to_10(mocker, invalid_limit):
    """Test that invalid limit values default to 10."""
    # Arrange
    table_name = "some_table"
    default_limit = 10
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = [(1,)]

    # Act
    result = await db_ops.get_table_sample(mock_conn, table_name, invalid_limit)

    # Assert
    expected_result = [{'id': 1}]
    assert result == expected_result
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {default_limit}")
    # mock_conn.close() is no longer called by db_ops

async def test_get_table_sample_execution_error_raises(mocker):
    """Test SQL execution error raises QueryExecutionError."""
    # Arrange
    table_name = "error_table"
    limit = 10
    original_exception = Exception("Syntax error")
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(QueryExecutionError) as excinfo:
        await db_ops.get_table_sample(mock_conn, table_name, limit)

    assert f"Failed to execute sample query for table '{table_name}'" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_cursor.execute.assert_called_once_with(f"SELECT * FROM {table_name} LIMIT {limit}")
    # mock_conn.close() is no longer called by db_ops

async def test_get_table_sample_invalid_connection_raises(mocker):
    """Test invalid connection (e.g., None) raises DatabaseConnectionError."""
    # Arrange
    table_name = "any_table"
    limit = 10
    mock_conn = None # Simulate a bad connection

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.get_table_sample(mock_conn, table_name, limit)
    assert "Provided database connection is invalid" in str(excinfo.value)


# --- Tests for list_tables --- #

async def test_list_tables_success(mocker):
    """Test successful fetching of table list."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.fetchall.return_value = [('table1',), ('table_two',), (None,), ('table3',)]

    # Act
    result = await db_ops.list_tables(mock_conn)

    # Assert
    expected_result = ['table1', 'table_two', 'table3']
    assert result == expected_result
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # mock_conn.close() is no longer called by db_ops

async def test_list_tables_empty(mocker):
    """Test fetching table list when database has no tables."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.fetchall.return_value = []

    # Act
    result = await db_ops.list_tables(mock_conn)

    # Assert
    assert result == []
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # mock_conn.close() is no longer called by db_ops

async def test_list_tables_execution_error_raises(mocker):
    """Test error during SHOW TABLES raises QueryExecutionError."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    original_exception = Exception("SHOW TABLES not allowed")
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(QueryExecutionError) as excinfo:
        await db_ops.list_tables(mock_conn)

    assert "Failed to list tables" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_cursor.execute.assert_called_once_with("SHOW TABLES")
    # mock_conn.close() is no longer called by db_ops

async def test_list_tables_invalid_connection_raises(mocker):
    """Test invalid connection (e.g., None) raises DatabaseConnectionError."""
    # Arrange
    mock_conn = None # Simulate a bad connection

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.list_tables(mock_conn)
    assert "Provided database connection is invalid" in str(excinfo.value)


# --- Tests for execute_sql_query --- #

async def test_execute_sql_query_success(mocker):
    """Test successful execution of a SELECT query returns list[dict]."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = [('id',), ('name',)]
    mock_cursor.fetchall.return_value = [(1, 'Alice')]

    # Act
    result = await db_ops.execute_sql_query(mock_conn, sql)

    # Assert
    expected_result = [{'id': 1, 'name': 'Alice'}]
    assert result == expected_result
    mock_cursor.execute.assert_called_once_with(sql)
    # mock_conn.close() is no longer called by db_ops

@pytest.mark.parametrize("non_allowed_sql, statement_type", [
    ("INSERT INTO users (id, name) VALUES (2, 'Bob')", "INSERT"),
    ("UPDATE users SET name = 'Charlie' WHERE id = 1", "UPDATE"),
    ("DELETE FROM users WHERE id = 1", "DELETE"),
    ("DROP TABLE users", "DROP"),
    ("CREATE TABLE new_t (c int);", "CREATE"),
    ("-- SELECT * FROM users;\nDROP TABLE users;", "DROP"), # Assuming sqlparse handles comments
])
async def test_execute_sql_query_rejects_non_allowed_type_raises(mocker, non_allowed_sql, statement_type):
    """Test non-allowed SQL statements raise InvalidInputError with dynamic message."""
    # Arrange
    mock_conn, mock_cursor = _get_mock_conn_and_cursor() # conn not used by validation
    
    # Temporarily modify ALLOWED_SQL_TYPES in config for this test if needed,
    # or ensure it's set to something like ['SELECT'] for this test to be meaningful.
    # For this example, we assume config.ALLOWED_SQL_TYPES does NOT include `statement_type`.
    # If it does, this test would need adjustment or a different `statement_type`.
    original_allowed_types = config.ALLOWED_SQL_TYPES
    config.ALLOWED_SQL_TYPES = ['SELECT'] # Ensure only SELECT is allowed for this test case

    allowed_str = ", ".join(config.ALLOWED_SQL_TYPES)
    expected_msg_regex = f"Query type '{statement_type}' is not allowed. Allowed types: {allowed_str}."

    # Act & Assert
    with pytest.raises(InvalidInputError, match=expected_msg_regex):
        await db_ops.execute_sql_query(mock_conn, non_allowed_sql)

    mock_cursor.execute.assert_not_called()
    # Restore original ALLOWED_SQL_TYPES if changed
    config.ALLOWED_SQL_TYPES = original_allowed_types


async def test_execute_sql_query_empty_result(mocker):
    """Test SELECT query that returns no rows."""
    # Arrange
    sql = "SELECT id FROM users WHERE name = 'NonExistent'"
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = [('id',)]
    mock_cursor.fetchall.return_value = []

    # Act
    output = await db_ops.execute_sql_query(mock_conn, sql)

    # Assert
    assert output == "-- Query executed successfully, but returned no rows. --"
    mock_cursor.execute.assert_called_once_with(sql)
    # mock_conn.close() is no longer called by db_ops

async def test_execute_sql_query_no_description_select(mocker):
    """Test SELECT query where cursor.description is None after execute (should be empty result)."""
    # Arrange
    sql = "SELECT * FROM some_view" # Example of a SELECT
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = None # Simulate no description (e.g. empty view or specific DB behavior)
    
    # We need to mock the statement type for this specific test path
    mocker.patch('sqlparse.parse', return_value=[MagicMock(get_type=lambda: 'SELECT')])


    # Act
    output = await db_ops.execute_sql_query(mock_conn, sql)

    # Assert
    # If it's a SELECT and description is None, it means 0 columns were selected or 0 rows.
    # The current logic in db_ops.py returns "-- Query executed successfully, but returned no rows. --"
    # if cursor.description is None BUT statement_type is 'SELECT'
    assert output == "-- Query executed successfully, but returned no rows. --"
    mock_cursor.execute.assert_called_once_with(sql)
    # mock_conn.close() is no longer called by db_ops

async def test_execute_sql_query_no_description_non_select(mocker):
    """Test non-SELECT query where cursor.description is None (e.g. DDL)."""
    # Arrange
    # For this test, assume 'CREATE' is an allowed type for a moment
    sql = "CREATE TABLE my_new_table (id INT)"
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.description = None

    # Mock sqlparse to control the statement type for this test
    mock_statement = MagicMock()
    mock_statement.get_type.return_value = 'CREATE' 
    mocker.patch('sqlparse.parse', return_value=[mock_statement])
    
    original_allowed_types = config.ALLOWED_SQL_TYPES
    config.ALLOWED_SQL_TYPES = ['CREATE'] # Temporarily allow CREATE for this test

    # Act
    output = await db_ops.execute_sql_query(mock_conn, sql)

    # Assert
    # If it's not a SELECT and description is None, it means it's a DDL/DML that doesn't return rows.
    expected_msg = "-- Query executed, but it was not a type that returns rows. --"
    assert output == expected_msg
    mock_cursor.execute.assert_called_once_with(sql)
    
    config.ALLOWED_SQL_TYPES = original_allowed_types # Restore


async def test_execute_sql_query_execution_error_raises(mocker):
    """Test SQL execution error raises QueryExecutionError."""
    # Arrange
    sql = "SELECT bad_col FROM users"
    original_exception = Exception("Column 'bad_col' not found")
    mock_conn, mock_cursor = _get_mock_conn_and_cursor()
    mock_cursor.execute.side_effect = original_exception

    # Act & Assert
    with pytest.raises(QueryExecutionError) as excinfo:
        await db_ops.execute_sql_query(mock_conn, sql)

    assert "Error executing SQL query" in str(excinfo.value)
    assert excinfo.value.original_exception is original_exception
    mock_cursor.execute.assert_called_once_with(sql)
    # mock_conn.close() is no longer called by db_ops

async def test_execute_sql_query_invalid_connection_raises(mocker):
    """Test invalid connection (e.g., None) raises DatabaseConnectionError."""
    # Arrange
    sql = "SELECT 1"
    mock_conn = None # Simulate a bad connection

    # Act & Assert
    with pytest.raises(DatabaseConnectionError) as excinfo:
        await db_ops.execute_sql_query(mock_conn, sql)
    assert "Provided database connection is invalid" in str(excinfo.value)
    # The TODO for adding tests for execute_sql_query can be removed as this covers a connection case