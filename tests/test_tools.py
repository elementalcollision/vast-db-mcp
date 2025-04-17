import pytest
import json
from unittest.mock import patch

# Import handler to test
from vast_mcp_server.tools import query
# Import custom exceptions to mock them being raised
from vast_mcp_server.exceptions import (
    QueryExecutionError,
    InvalidInputError,
    DatabaseConnectionError
)

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# --- Tests for tools/query.py --- #

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_csv_default(mock_execute_sql):
    """Test default CSV format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql)

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_csv_explicit(mock_execute_sql):
    """Test explicit CSV format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql, format="csv")

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_json(mock_execute_sql):
    """Test JSON format output."""
    # Arrange
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_json = json.dumps(db_result, indent=2)

    # Act
    result = await query.vast_sql_query(sql=sql, format="json")

    # Assert
    assert result == expected_json
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_format_invalid(mock_execute_sql):
    """Test invalid format defaults to CSV."""
    # Arrange
    sql = "SELECT id FROM t"
    db_result = [{'id': 1}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id\r\n1\r\n"

    # Act
    result = await query.vast_sql_query(sql=sql, format="yaml") # Invalid format

    # Assert
    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_db_message_string(mock_execute_sql):
    """Test that DB message strings (like no data) are passed through."""
    # Arrange
    sql = "SELECT * FROM empty_table"
    db_message = "-- Query executed successfully, but returned no rows. --"
    mock_execute_sql.return_value = db_message

    # Act
    result_csv = await query.vast_sql_query(sql=sql, format="csv")
    # result_json = await query.vast_sql_query(sql=sql, format="json")

    # Assert
    assert result_csv == db_message
    # assert json.loads(result_json) == {"message": db_message} # Optional check
    mock_execute_sql.assert_called_once_with(sql)

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
    result = await query.vast_sql_query(sql=sql, format=format_type)

    # Assert
    mock_execute_sql.assert_called_once_with(sql)
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
    result = await query.vast_sql_query(sql=sql, format=format_type)

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