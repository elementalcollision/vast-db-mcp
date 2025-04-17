import pytest
from unittest.mock import patch, MagicMock

# Import handlers to test
from vast_mcp_server.resources import schema
from vast_mcp_server.resources import table_data

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# --- Tests for resources/schema.py --- #

@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_vast_schema_success(mock_get_db_schema):
    """Test successful call to schema resource handler."""
    # Arrange
    expected_schema = "TABLE: table1\n  - col1 (INT)\n\n"
    mock_get_db_schema.return_value = expected_schema

    # Act
    result = await schema.get_vast_schema()

    # Assert
    assert result == expected_schema
    mock_get_db_schema.assert_called_once()

@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_vast_schema_error(mock_get_db_schema):
    """Test error handling in schema resource handler."""
    # Arrange
    test_exception = Exception("DB schema fetch failed")
    mock_get_db_schema.side_effect = test_exception

    # Act
    result = await schema.get_vast_schema()

    # Assert
    expected_error_msg = f"Error retrieving VAST DB schema: {test_exception}"
    assert result == expected_error_msg
    mock_get_db_schema.assert_called_once()

# --- Tests for resources/table_data.py --- #

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_success_with_limit(mock_get_table_sample):
    """Test successful call to table sample handler with explicit limit."""
    # Arrange
    table_name = "users"
    limit = 5
    expected_data = "id,name\r\n1,Alice\r\n"
    mock_get_table_sample.return_value = expected_data

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit)

    # Assert
    assert result == expected_data
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_success_default_limit(mock_get_table_sample):
    """Test successful call to table sample handler with default limit."""
    # Arrange
    table_name = "products"
    default_limit = 10 # Default limit defined in the handler
    expected_data = "id,price\r\n1,9.99\r\n"
    mock_get_table_sample.return_value = expected_data

    # Act
    # Call without providing the limit parameter
    result = await table_data.get_vast_table_sample(table_name=table_name)

    # Assert
    assert result == expected_data
    # Verify it was called with the default limit
    mock_get_table_sample.assert_called_once_with(table_name, default_limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_success_invalid_limit(mock_get_table_sample):
    """Test successful call to table sample handler with invalid limit (should default)."""
    # Arrange
    table_name = "orders"
    invalid_limit = -5
    default_limit = 10
    expected_data = "id,status\r\n1,shipped\r\n"
    mock_get_table_sample.return_value = expected_data

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=invalid_limit)

    # Assert
    assert result == expected_data
    # Verify it was called with the default limit, not the invalid one
    mock_get_table_sample.assert_called_once_with(table_name, default_limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_error(mock_get_table_sample):
    """Test error handling in table sample handler."""
    # Arrange
    table_name = "logs"
    limit = 20
    test_exception = Exception("DB table sample fetch failed")
    mock_get_table_sample.side_effect = test_exception

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit)

    # Assert
    expected_error_msg = f"Error retrieving sample data for table '{table_name}': {test_exception}"
    assert result == expected_error_msg
    mock_get_table_sample.assert_called_once_with(table_name, limit) 