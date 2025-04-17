import pytest
from unittest.mock import patch

# Import handler to test
from vast_mcp_server.tools import query

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# --- Tests for tools/query.py --- #

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_success(mock_execute_sql):
    """Test successful call to SQL query tool handler."""
    # Arrange
    sql = "SELECT name FROM users WHERE id = 1"
    expected_result = "name\r\nAlice\r\n"
    mock_execute_sql.return_value = expected_result

    # Act
    result = await query.vast_sql_query(sql=sql)

    # Assert
    assert result == expected_result
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_db_error(mock_execute_sql):
    """Test SQL query tool when db_ops raises an error."""
    # Arrange
    sql = "SELECT * FROM non_existent_table"
    db_exception = Exception("Table not found")
    mock_execute_sql.side_effect = db_exception

    # Act
    result = await query.vast_sql_query(sql=sql)

    # Assert
    expected_error_msg = f"Error executing VAST DB SQL query: {db_exception}"
    assert result == expected_error_msg
    mock_execute_sql.assert_called_once_with(sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_non_select_error(mock_execute_sql):
    """Test SQL query tool when db_ops returns non-SELECT error message."""
    # Arrange
    sql = "INSERT INTO data VALUES (1)"
    # Simulate the error message returned by db_ops for non-SELECT
    non_select_error_msg = "Error: Only SELECT queries are currently allowed for safety."
    mock_execute_sql.return_value = non_select_error_msg

    # Act
    result = await query.vast_sql_query(sql=sql)

    # Assert
    # The handler should just return the error message from db_ops
    assert result == non_select_error_msg
    mock_execute_sql.assert_called_once_with(sql) 