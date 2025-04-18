import pytest
import json
from unittest.mock import patch

# Import handler to test
from vast_mcp_server.tools import query
from vast_mcp_server import config # Import config to allow mocking
# Import custom exceptions to mock them being raised
from vast_mcp_server.exceptions import (
    QueryExecutionError,
    InvalidInputError,
    DatabaseConnectionError
)

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# Standard mock headers for successful calls
MOCK_HEADERS = {
    'X-Vast-Access-Key': 'test-access-key',
    'X-Vast-Secret-Key': 'test-secret-key'
}

# --- Integration-Style Tests for tools/query.py --- #
# These tests mock the db_ops layer but test the tool handler logic, #
# including configuration checks.

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_integration_success(mock_execute_sql):
    """Test successful SQL query execution (default CSV)."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    # Pass mock headers
    result = await query.vast_sql_query(sql=sql, headers=MOCK_HEADERS)

    # Assert
    assert result == expected_csv
    # Check db_ops called with keys
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@pytest.mark.asyncio
async def test_vast_sql_query_integration_missing_headers():
    """Test query fails with auth error if headers are missing."""
    # Arrange
    sql = "SELECT 1"

    # Act
    result_none = await query.vast_sql_query(sql=sql, headers=None)
    result_empty = await query.vast_sql_query(sql=sql, headers={})
    result_partial = await query.vast_sql_query(sql=sql, headers={'X-Vast-Access-Key': 'key'})

    # Assert
    assert result_none.startswith("ERROR: [AuthenticationError] Authentication headers are missing")
    assert result_empty.startswith("ERROR: [AuthenticationError] Missing required authentication headers")
    assert result_partial.startswith("ERROR: [AuthenticationError] Missing required authentication headers: X-Vast-Secret-Key")

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_integration_auth_fail_db(mock_execute_sql):
    """Test query fails with auth error if db_ops indicates auth failure."""
    # Arrange
    sql = "SELECT * FROM secrets"
    error_msg = "Authentication failed for user's key"
    db_exception = DatabaseConnectionError(error_msg)
    mock_execute_sql.side_effect = db_exception

    # Act
    result = await query.vast_sql_query(sql=sql, headers=MOCK_HEADERS)

    # Assert
    assert result == "ERROR: [AuthenticationError] Authentication failed with provided credentials."
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@pytest.mark.parametrize("sql, allowed_types, should_pass", [
    ("SELECT * FROM t", ["SELECT"], True),
    ("SELECT * FROM t", ["SELECT", "INSERT"], True),
    ("INSERT INTO t VALUES (1)", ["SELECT"], False),
    ("INSERT INTO t VALUES (1)", ["SELECT", "INSERT"], True),
    ("UPDATE t SET c=1", ["SELECT"], False),
    ("UPDATE t SET c=1", ["SELECT", "UPDATE"], True),
    ("DELETE FROM t", ["SELECT"], False),
    ("DELETE FROM t", ["SELECT", "DELETE"], True),
    ("CREATE TABLE t (id INT)", ["SELECT", "INSERT"], False),
    ("CREATE TABLE t (id INT)", ["SELECT", "DDL"], True), # Assuming sqlparse uses DDL
])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_integration_type_validation(mock_execute_sql, mocker, sql, allowed_types, should_pass):
    """Test query type validation respects config.ALLOWED_SQL_TYPES."""
    # Arrange
    mocker.patch.object(config, 'ALLOWED_SQL_TYPES', allowed_types)
    db_result = [{'id': 1}]
    mock_execute_sql.return_value = db_result

    # Act
    if should_pass:
        # Pass mock headers
        result = await query.vast_sql_query(sql=sql, headers=MOCK_HEADERS)
        assert "ERROR:" not in result
        # Check db_ops called with keys
        mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])
    else:
        # Pass mock headers (db_ops won't be called anyway)
        result = await query.vast_sql_query(sql=sql, headers=MOCK_HEADERS)
        assert result.startswith("ERROR: [InvalidInputError]")
        assert "not allowed. Allowed types:" in result
        mock_execute_sql.assert_not_called()

@pytest.mark.parametrize("format_type, expected_prefix, is_json", [
    ("csv", "ERROR: [QueryExecutionError]", False),
    ("json", '{"error": {"type": "QueryExecutionError", "message":', True)
])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_integration_db_exception(mock_execute_sql, format_type, expected_prefix, is_json):
    """Test formatted error messages on DB exceptions (non-auth, integration style)."""
    # Arrange
    sql = "SELECT bad_col FROM t"
    db_exception = QueryExecutionError("Column not found")
    mock_execute_sql.side_effect = db_exception

    # Act
    # Pass mock headers
    result = await query.vast_sql_query(sql=sql, format=format_type, headers=MOCK_HEADERS)

    # Assert
    # Check db_ops called with keys
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])
    assert str(result).startswith(expected_prefix)
    if is_json:
        try: json.loads(result)
        except json.JSONDecodeError: pytest.fail("Invalid JSON error response")
    # Ensure original error message is present (and not the simplified auth one)
    assert str(db_exception) in str(result)
    assert "Authentication failed" not in str(result)

# --- Existing Unit Tests (Can be kept or refactored) ---
# The tests below focus more on the formatting logic within the handler itself.

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_csv_default(mock_execute_sql):
    """Test default CSV format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql, headers=MOCK_HEADERS)

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_csv_explicit(mock_execute_sql):
    """Test explicit CSV format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql, format="csv", headers=MOCK_HEADERS)

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_json(mock_execute_sql):
    """Test JSON format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_json = json.dumps(db_result, indent=2)

    # Act
    result = await query.vast_sql_query(sql=sql, format="json", headers=MOCK_HEADERS)

    # Assert
    assert result == expected_json
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_invalid(mock_execute_sql):
    """Test invalid format defaults to CSV."""
    # Arrange
    sql = "SELECT id FROM t"
    db_result = [{'id': 1}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id\r\n1\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql, format="yaml", headers=MOCK_HEADERS) # Invalid format

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_db_message_string(mock_execute_sql):
    """Test that DB message strings (like no data) are passed through."""
    # Arrange
    sql = "SELECT * FROM empty_table"
    db_message = "-- Query executed successfully, but returned no rows. --"
    mock_execute_sql.return_value = db_message

    # Act
    result_csv = await query.vast_sql_query(sql=sql, format="csv", headers=MOCK_HEADERS)
    # result_json = await query.vast_sql_query(sql=sql, format="json")

    # Assert
    assert result_csv == db_message
    # assert json.loads(result_json) == {"message": db_message} # Optional check
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])

@pytest.mark.parametrize("format_type, expected_prefix, is_json", [
    ("csv", "ERROR: [QueryExecutionError]", False),
    ("json", '{"error": {"type": "QueryExecutionError", "message":', True)
])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_db_exception(mock_execute_sql, format_type, expected_prefix, is_json):
    """Test formatted error messages on DB exceptions."""
    # Arrange
    sql = "SELECT bad_col FROM t"
    db_exception = QueryExecutionError("Column not found")
    mock_execute_sql.side_effect = db_exception

    # Act
    result = await query.vast_sql_query(sql=sql, format=format_type, headers=MOCK_HEADERS)

    # Assert
    mock_execute_sql.assert_called_once_with(sql, MOCK_HEADERS['X-Vast-Access-Key'], MOCK_HEADERS['X-Vast-Secret-Key'])
    assert str(result).startswith(expected_prefix)
    if is_json:
        try: json.loads(result) # Check valid JSON
        except json.JSONDecodeError: pytest.fail("Invalid JSON error response")
    assert str(db_exception) in str(result)

@pytest.mark.parametrize("format_type, expected_prefix, is_json", [
    ("csv", "ERROR: [InvalidInputError]", False),
    ("json", '{"error": {"type": "InvalidInputError", "message":', True)
])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_invalid_input_exception(mock_execute_sql, format_type, expected_prefix, is_json):
    """Test formatted error for InvalidInputError (e.g., non-SELECT)."""
    # Arrange
    sql = "DROP TABLE users"
    db_exception = InvalidInputError("Only SELECT queries are currently allowed")
    mock_execute_sql.side_effect = db_exception

    # Act
    result = await query.vast_sql_query(sql=sql, format=format_type, headers=MOCK_HEADERS)

    # Assert
    mock_execute_sql.assert_called_once_with(sql)
    assert str(result).startswith(expected_prefix)
    if is_json:
        try: json.loads(result) # Check valid JSON
        except json.JSONDecodeError: pytest.fail("Invalid JSON error response")
    assert str(db_exception) in str(result)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_exception(mock_execute_sql):
    """Test error handling within the handler itself."""
    # Arrange
    sql = "SELECT 1"
    handler_exception = TypeError("Cannot format this")
    mock_execute_sql.return_value = [{'a': 1}] # DB call succeeds

    # Patch the internal _format_results to raise an error
    with patch('vast_mcp_server.tools.query._format_results', side_effect=handler_exception):
        # Act
        result = await query.vast_sql_query(sql=sql, format="csv")

    # Assert
    expected_error_msg = f"ERROR: [{type(handler_exception).__name__}] {handler_exception}"
    assert result == expected_error_msg
    mock_execute_sql.assert_called_once_with(sql) 