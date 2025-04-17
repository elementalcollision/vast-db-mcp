import pytest
import json
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
async def test_get_vast_table_sample_format_csv_default(mock_get_table_sample):
    """Test CSV format is returned by default."""
    # Arrange
    table_name = "users"
    limit = 2
    db_result = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    mock_get_table_sample.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n2,Bob\r\n"

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit)

    # Assert
    assert result == expected_csv
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_format_csv_explicit(mock_get_table_sample):
    """Test CSV format is returned when specified."""
    # Arrange
    table_name = "users"
    limit = 2
    db_result = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    mock_get_table_sample.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n2,Bob\r\n"

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="csv")

    # Assert
    assert result == expected_csv
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_format_json(mock_get_table_sample):
    """Test JSON format is returned when specified."""
    # Arrange
    table_name = "users"
    limit = 2
    db_result = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    mock_get_table_sample.return_value = db_result
    # Use json.dumps to ensure exact formatting match (including indentation)
    expected_json = json.dumps(db_result, indent=2)

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="json")

    # Assert
    assert result == expected_json
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_format_invalid(mock_get_table_sample):
    """Test invalid format defaults to CSV."""
    # Arrange
    table_name = "users"
    limit = 2
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_get_table_sample.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n"

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="xml") # Invalid format

    # Assert
    assert result == expected_csv
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_db_error_string(mock_get_table_sample):
    """Test that DB error strings are passed through directly."""
    # Arrange
    table_name = "logs"
    limit = 20
    db_error_msg = "Error fetching sample data for table 'logs' from VAST DB: Some DB Error"
    mock_get_table_sample.return_value = db_error_msg

    # Act
    # Test both formats, error string should be the same
    result_csv = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="csv")
    result_json = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="json")

    # Assert
    assert result_csv == db_error_msg
    assert result_json == db_error_msg
    # Called twice because we called the handler twice
    assert mock_get_table_sample.call_count == 2
    mock_get_table_sample.assert_called_with(table_name, limit)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_handler_exception(mock_get_table_sample):
    """Test error handling within the handler itself (after db call)."""
    # Arrange
    table_name = "data"
    limit = 5
    handler_exception = Exception("Something broke during formatting")

    # Mock db_ops call to succeed
    mock_get_table_sample.return_value = [{'a': 1}]

    # Patch the internal _format_results to raise an error
    with patch('vast_mcp_server.resources.table_data._format_results', side_effect=handler_exception):
        # Act
        result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="csv")

    # Assert
    # The handler should catch its own exception and return a formatted error string
    expected_error_msg = f"Error retrieving sample data for table '{table_name}': {handler_exception}"
    assert result == expected_error_msg
    mock_get_table_sample.assert_called_once_with(table_name, limit) 