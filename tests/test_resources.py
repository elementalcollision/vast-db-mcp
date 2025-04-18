import pytest
import json
from unittest.mock import patch, MagicMock
import httpx # Added for ASGI testing

# Import the app instance
from vast_mcp_server.server import mcp_app
# Import resource handlers directly (optional, can test through app)
# from vast_mcp_server.resources import schema
# from vast_mcp_server.resources import table_data
# from vast_mcp_server.resources import metadata # Import new handler
from vast_mcp_server.exceptions import (
    DatabaseConnectionError,
    SchemaFetchError,
    TableDescribeError, # Added
    QueryExecutionError,
    InvalidInputError,
    VastMcpError
)

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# --- Fixture for Async Test Client ---
@pytest.fixture
async def client():
    async with httpx.AsyncClient(app=mcp_app, base_url="http://test") as client:
        yield client

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
async def test_get_vast_schema_db_error(mock_get_db_schema):
    """Test formatted error message on DB error from db_ops."""
    # Arrange
    db_exception = SchemaFetchError("Underlying DB error")
    mock_get_db_schema.side_effect = db_exception

    # Act
    result = await schema.get_vast_schema()

    # Assert
    expected_error_msg = f"ERROR: [SchemaFetchError] {db_exception}"
    assert result == expected_error_msg
    mock_get_db_schema.assert_called_once()

@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema', side_effect=Exception("Unexpected!"))
async def test_get_vast_schema_unexpected_error(mock_get_db_schema):
    """Test formatted error message on unexpected error."""
    # Arrange (exception set in patch)

    # Act
    result = await schema.get_vast_schema()

    # Assert
    assert result.startswith("ERROR: [UnexpectedError] An unexpected error occurred:")
    mock_get_db_schema.assert_called_once()

# --- Tests for resources/table_data.py: list_vast_tables --- #

@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_vast_tables_success_json_default(mock_list_tables):
    """Test successful table listing returns JSON by default."""
    # Arrange
    db_result = ["table1", "table2"]
    mock_list_tables.return_value = db_result
    expected_json = json.dumps(db_result, indent=2)

    # Act
    result = await table_data.list_vast_tables()

    # Assert
    assert result == expected_json
    mock_list_tables.assert_called_once()

@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_vast_tables_success_csv_list(mock_list_tables):
    """Test successful table listing returns list format when format=csv."""
    # Arrange
    db_result = ["table1", "table2"]
    mock_list_tables.return_value = db_result
    expected_list_str = "table1\ntable2\n"

    # Act
    result = await table_data.list_vast_tables(format="csv")

    # Assert
    assert result == expected_list_str
    mock_list_tables.assert_called_once()

@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_vast_tables_empty(mock_list_tables):
    """Test empty table list returns empty JSON array."""
    # Arrange
    db_result = []
    mock_list_tables.return_value = db_result
    expected_json = "[]"

    # Act
    result = await table_data.list_vast_tables()

    # Assert
    assert result == expected_json
    mock_list_tables.assert_called_once()

@pytest.mark.parametrize("format_type, expected_prefix, is_json", [
    ("json", '{"error": {"type": "QueryExecutionError", "message":', True),
    ("csv", "ERROR: [QueryExecutionError]", False),
])
@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_vast_tables_db_error(mock_list_tables, format_type, expected_prefix, is_json):
    """Test formatted error message on DB exception when listing tables."""
    # Arrange
    db_exception = QueryExecutionError("Cannot show tables")
    mock_list_tables.side_effect = db_exception

    # Act
    result = await table_data.list_vast_tables(format=format_type)

    # Assert
    mock_list_tables.assert_called_once()
    assert str(result).startswith(expected_prefix)
    if is_json:
        try: json.loads(result)
        except json.JSONDecodeError: pytest.fail("Invalid JSON error response")
    assert str(db_exception) in str(result)

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
async def test_get_vast_table_sample_db_message_string(mock_get_table_sample):
    """Test that DB message strings (like no data) are passed through."""
    # Arrange
    table_name = "empty_table"
    limit = 10
    db_message = "-- No data found in table 'empty_table' or table does not exist. --"
    mock_get_table_sample.return_value = db_message

    # Act
    result_csv = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="csv")
    # Check if JSON format wraps the message (optional behavior)
    # result_json = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format="json")

    # Assert
    assert result_csv == db_message
    # assert json.loads(result_json) == {"message": db_message}
    mock_get_table_sample.assert_called_once_with(table_name, limit)

@pytest.mark.parametrize("format_type, expected_prefix, is_json", [
    ("csv", "ERROR: [QueryExecutionError]", False),
    ("json", '{"error": {"type": "QueryExecutionError", "message":', True)
])
@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_db_exception(mock_get_table_sample, format_type, expected_prefix, is_json):
    """Test formatted error messages on DB exceptions."""
    # Arrange
    table_name = "bad_table"
    limit = 5
    db_exception = QueryExecutionError("DB query failed")
    mock_get_table_sample.side_effect = db_exception

    # Act
    result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format=format_type)

    # Assert
    mock_get_table_sample.assert_called_once_with(table_name, limit)
    assert str(result).startswith(expected_prefix)
    if is_json:
        try: json.loads(result) # Check if valid JSON
        except json.JSONDecodeError: pytest.fail("Invalid JSON error response")
    assert str(db_exception) in str(result)

@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_vast_table_sample_handler_exception(mock_get_table_sample):
    """Test error handling within the handler itself (after db call)."""
    # Arrange
    table_name = "data"
    limit = 5
    format_type = "csv"
    handler_exception = ValueError("Bad formatting logic")
    mock_get_table_sample.return_value = [{'a': 1}] # DB call succeeds

    # Patch the internal _format_results
    with patch('vast_mcp_server.resources.table_data._format_results', side_effect=handler_exception):
        # Act
        result = await table_data.get_vast_table_sample(table_name=table_name, limit=limit, format=format_type)

    # Assert
    expected_error_msg = f"ERROR: [{type(handler_exception).__name__}] {handler_exception}"
    assert result == expected_error_msg
    mock_get_table_sample.assert_called_once_with(table_name, limit)

# --- Integration Tests for resources/metadata.py ---

@pytest.mark.asyncio
async def test_get_table_metadata_success(client, mocker):
    """Test successful metadata fetch via vast://metadata/tables/{table_name}"""
    # Arrange
    table_name = "test_table"
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Simulate DESCRIBE TABLE results
    mock_cursor.fetchall.return_value = [
        ('id', 'INTEGER', 'nullable', 'pk'), # Example tuple format
        ('name', 'VARCHAR(100)', 'not nullable', None),
        ('ts', 'TIMESTAMP', None, None)
    ]

    # Patch db_ops.create_vast_connection
    mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection', return_value=mock_conn)

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}")

    # Assert
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    expected_data = {
        "table_name": table_name,
        "columns": [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR(100)"},
            {"name": "ts", "type": "TIMESTAMP"}
        ]
    }
    assert response.json() == expected_data
    mock_cursor.execute.assert_called_once_with(f"DESCRIBE TABLE {table_name}")
    mock_conn.close.assert_called_once() # Check connection closed

@pytest.mark.asyncio
async def test_get_table_metadata_not_found(client, mocker):
    """Test 404 when DESCRIBE fails indicating table not found."""
    # Arrange
    table_name = "nonexistent_table"
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Simulate DESCRIBE failing because table doesn't exist
    describe_error = Exception("Table 'nonexistent_table' does not exist") # Example VAST error
    mock_cursor.execute.side_effect = TableDescribeError(str(describe_error), original_exception=describe_error)

    # Patch db_ops.create_vast_connection
    mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection', return_value=mock_conn)

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}")

    # Assert
    assert response.status_code == 404 # NOT_FOUND maps to 404
    assert response.headers['content-type'] == "application/json"
    assert response.json()["error"] == f"Table '{table_name}' not found or metadata unavailable."
    mock_cursor.execute.assert_called_once_with(f"DESCRIBE TABLE {table_name}")
    mock_conn.close.assert_called_once()

@pytest.mark.asyncio
async def test_get_table_metadata_connection_error(client, mocker):
    """Test 503 on database connection error."""
    # Arrange
    table_name = "any_table"
    # Patch create_vast_connection to raise an error
    connection_error = DatabaseConnectionError("Cannot connect to DB")
    mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection', side_effect=connection_error)

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}")

    # Assert
    assert response.status_code == 503 # SERVICE_UNAVAILABLE maps to 503
    assert response.headers['content-type'] == "application/json"
    assert response.json() == {"error": "Database connection error"}

@pytest.mark.asyncio
async def test_get_table_metadata_invalid_input(client, mocker):
    """Test 400 for invalid table name format."""
    # Arrange
    table_name = "invalid-table-name!"
    # Mock connection just to ensure it's not called
    mock_create_connection = mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection')

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}")

    # Assert
    assert response.status_code == 400 # BAD_REQUEST maps to 400
    assert response.headers['content-type'] == "application/json"
    assert "Invalid input: Invalid table name" in response.json()["error"]
    mock_create_connection.assert_not_called() # DB connection shouldn't be attempted

@pytest.mark.asyncio
async def test_get_table_metadata_other_describe_error(client, mocker):
    """Test 500 for unexpected errors during DESCRIBE."""
    # Arrange
    table_name = "problem_table"
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Simulate DESCRIBE failing for some other reason
    describe_error = Exception("Some internal VAST error")
    mock_cursor.execute.side_effect = TableDescribeError(str(describe_error), original_exception=describe_error)

    # Patch db_ops.create_vast_connection
    mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection', return_value=mock_conn)

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}")

    # Assert
    assert response.status_code == 500 # INTERNAL_SERVER_ERROR maps to 500
    assert response.headers['content-type'] == "application/json"
    assert response.json()["error"] == f"Failed to retrieve metadata for table '{table_name}'."
    assert str(describe_error) in response.json()["details"]
    mock_cursor.execute.assert_called_once_with(f"DESCRIBE TABLE {table_name}")
    mock_conn.close.assert_called_once()

@pytest.mark.asyncio
async def test_get_table_metadata_invalid_uri_format(client):
    """Test 400 for incorrect URI path format."""
    # Arrange (No mocking needed as URI parsing happens before DB interaction)

    # Act
    response = await client.get("/vast/metadata/wrong/{table_name}") # Invalid path

    # Assert
    assert response.status_code == 400
    assert response.headers['content-type'] == "application/json"
    assert "Invalid URI format" in response.json()["error"]

# --- End Tests for resources/metadata.py ---

# --- Existing Tests for resources/schema.py and table_data.py ---
# (These existing unit tests can remain, they test the handler logic directly)

# ... (Paste existing tests from the original file here) ...

# (Need to ensure the original tests are copied back in)

# Example (replace with actual original tests):
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_vast_schema_success_original(mock_get_db_schema):
    """Test successful call to schema resource handler."""
    # Arrange
    expected_schema = "TABLE: table1\n  - col1 (INT)\n\n"
    mock_get_db_schema.return_value = expected_schema
    # Act
    result = await schema.get_vast_schema() # Assumes schema.py has this function
    # Assert
    assert result == expected_schema
    mock_get_db_schema.assert_called_once()

# ... (Add all other original tests back) ... 