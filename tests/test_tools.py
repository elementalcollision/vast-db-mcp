import pytest
import json
from unittest.mock import patch

# Import handler to test
from vast_mcp_server.tools import query

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
async def test_vast_sql_query_db_error_string(mock_execute_sql):
    """Test that DB error strings are passed through directly."""
    # Arrange
    sql = "SELECT * FROM non_existent_table"
    db_error_msg = "Error executing SQL query in VAST DB: Table not found"
    mock_execute_sql.return_value = db_error_msg

    # Act
    result_csv = await query.vast_sql_query(sql=sql, format="csv")
    result_json = await query.vast_sql_query(sql=sql, format="json")

    # Assert
    assert result_csv == db_error_msg
    assert result_json == db_error_msg
    assert mock_execute_sql.call_count == 2
    mock_execute_sql.assert_called_with(sql)

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
    expected_error_msg = f"Error executing VAST DB SQL query: {handler_exception}"
    assert result == expected_error_msg
    mock_execute_sql.assert_called_once_with(sql) 