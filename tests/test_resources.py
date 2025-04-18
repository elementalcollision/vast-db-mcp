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
    # Define standard mock headers for successful auth
    default_headers = {
        'X-Vast-Access-Key': 'test-access-key',
        'X-Vast-Secret-Key': 'test-secret-key'
    }
    async with httpx.AsyncClient(app=mcp_app, base_url="http://test", headers=default_headers) as client:
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

# --- Integration Tests for resources/schema.py ---

@pytest.mark.asyncio
async def test_get_schema_integration_success(client, mocker):
    """Test successful schema fetch via vast://schemas integration."""
    # Arrange
    expected_schema_str = "TABLE: t1\n - c1 (INT)\nTABLE: t2\n - c2 (STR)"
    # Patch the underlying db_ops function called by the handler
    mock_get_db_schema = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
    mock_get_db_schema.return_value = expected_schema_str

    # Act
    response = await client.get("/vast/schemas") # Headers are passed by client fixture

    # Assert
    assert response.status_code == 200
    assert response.headers['content-type'] == "text/plain; charset=utf-8"
    assert response.text == expected_schema_str
    # Check db_ops was called with credentials from default_headers
    mock_get_db_schema.assert_called_once_with('test-access-key', 'test-secret-key')

@pytest.mark.asyncio
async def test_get_schema_integration_missing_headers(client):
    """Test schema fetch fails with 401 if headers are missing."""
    # Arrange (Client fixture has default headers, override them)
    # Act
    response = await client.get("/vast/schemas", headers={}) # Send request with empty headers

    # Assert
    assert response.status_code == 401 # UNAUTHENTICATED
    assert "Missing required authentication headers" in response.text

@pytest.mark.asyncio
async def test_get_schema_integration_auth_fail_db(client, mocker):
    """Test schema fetch fails with 401 if DB connection indicates auth failure."""
    # Arrange
    error_msg = "Authentication failed for user"
    db_exception = DatabaseConnectionError(error_msg)
    mock_get_db_schema = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
    mock_get_db_schema.side_effect = db_exception

    # Act
    response = await client.get("/vast/schemas") # Uses default valid mock headers

    # Assert
    assert response.status_code == 401 # UNAUTHENTICATED
    assert "Authentication failed with provided credentials" in response.text
    mock_get_db_schema.assert_called_once_with('test-access-key', 'test-secret-key')

@pytest.mark.asyncio
async def test_get_schema_integration_db_error(client, mocker):
    """Test schema fetch DB error (non-auth) via vast://schemas integration."""
    # Arrange
    error_msg = "Some other connection problem"
    db_exception = DatabaseConnectionError(error_msg)
    mock_get_db_schema = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
    mock_get_db_schema.side_effect = db_exception

    # Act
    response = await client.get("/vast/schemas")

    # Assert
    assert response.status_code == 503 # SERVICE_UNAVAILABLE
    assert f"ERROR: [DatabaseConnectionError] Database connection error: {error_msg}" == response.text
    mock_get_db_schema.assert_called_once_with('test-access-key', 'test-secret-key')

@pytest.mark.asyncio
async def test_get_schema_integration_unexpected_error(client, mocker):
    """Test schema fetch unexpected error via vast://schemas integration."""
    # Arrange
    error_msg = "Something broke"
    generic_exception = Exception(error_msg)
    mock_get_db_schema = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
    mock_get_db_schema.side_effect = generic_exception

    # Act
    response = await client.get("/vast/schemas")

    # Assert
    assert response.status_code == 500 # INTERNAL_SERVER_ERROR
    assert response.text.startswith("ERROR: [UnexpectedError] An unexpected error occurred:")
    assert error_msg in response.text
    mock_get_db_schema.assert_called_once_with('test-access-key', 'test-secret-key')

# --- Integration Tests for resources/table_data.py --- #

@pytest.mark.asyncio
async def test_list_tables_integration_success_json(client, mocker):
    """Test successful table list fetch via vast://tables (JSON)."""
    # Arrange
    table_list = ["users", "orders"]
    mock_list_tables = mocker.patch('vast_mcp_server.vast_integration.db_ops.list_tables')
    mock_list_tables.return_value = table_list

    # Act
    response = await client.get("/vast/tables?format=json") # Explicit JSON
    response_default = await client.get("/vast/tables") # Default JSON

    # Assert
    expected_json = json.dumps(table_list, indent=2)
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    assert response.text == expected_json
    assert response_default.status_code == 200
    assert response_default.text == expected_json

    # Check db_ops call includes keys
    mock_list_tables.assert_called_with('test-access-key', 'test-secret-key')
    assert mock_list_tables.call_count == 2

@pytest.mark.asyncio
async def test_list_tables_integration_missing_headers(client):
    """Test list tables fails with 401 if headers are missing."""
    # Act
    response = await client.get("/vast/tables", headers={}) # Override default headers
    # Assert
    assert response.status_code == 401
    assert "Missing required authentication headers" in response.json()["error"]["message"]

@pytest.mark.asyncio
async def test_list_tables_integration_success_csv(client, mocker):
    """Test successful table list fetch via vast://tables (CSV/list)."""
    # Arrange
    table_list = ["users", "orders"]
    mock_list_tables = mocker.patch('vast_mcp_server.vast_integration.db_ops.list_tables')
    mock_list_tables.return_value = table_list

    # Act
    response_csv = await client.get("/vast/tables?format=csv")
    response_list = await client.get("/vast/tables?format=list")

    # Assert
    expected_text = "users\norders\n"
    assert response_csv.status_code == 200
    assert response_csv.headers['content-type'] == "text/plain; charset=utf-8"
    assert response_csv.text == expected_text

    assert response_list.status_code == 200
    assert response_list.headers['content-type'] == "text/plain; charset=utf-8"
    assert response_list.text == expected_text

    assert mock_list_tables.call_count == 2

@pytest.mark.asyncio
async def test_list_tables_integration_db_error(client, mocker):
    """Test table list DB error via vast://tables integration."""
    # Arrange
    error_msg = "Cannot list tables"
    db_exception = QueryExecutionError(error_msg)
    mock_list_tables = mocker.patch('vast_mcp_server.vast_integration.db_ops.list_tables')
    mock_list_tables.side_effect = db_exception

    # Act
    response_json = await client.get("/vast/tables?format=json")
    response_csv = await client.get("/vast/tables?format=csv")

    # Assert JSON
    assert response_json.status_code == 200 # Handler returns 200 with error
    assert response_json.headers['content-type'] == "application/json"
    expected_json_error = {"error": {"type": "QueryExecutionError", "message": error_msg}}
    assert response_json.json() == expected_json_error

    # Assert CSV
    assert response_csv.status_code == 200 # Handler returns 200 with error
    assert response_csv.headers['content-type'] == "text/plain; charset=utf-8"
    expected_csv_error = f"ERROR: [QueryExecutionError] {error_msg}"
    assert response_csv.text == expected_csv_error

    assert mock_list_tables.call_count == 2

@pytest.mark.asyncio
async def test_get_table_sample_integration_success_csv(client, mocker):
    """Test successful table sample fetch via vast://tables/{t} (CSV)."""
    # Arrange
    table_name = "users"
    limit = 5
    db_data = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
    mock_get_table_sample = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
    mock_get_table_sample.return_value = db_data

    # Act
    response_csv = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=csv")
    response_default = await client.get(f"/vast/tables/{table_name}?limit={limit}") # Default CSV

    # Assert
    expected_csv = "id,name\r\n1,A\r\n2,B\r\n"
    assert response_csv.status_code == 200
    assert response_csv.headers['content-type'] == "text/csv; charset=utf-8"
    assert response_csv.text == expected_csv
    assert response_default.status_code == 200
    assert response_default.text == expected_csv

    # Check db_ops call includes keys
    mock_get_table_sample.assert_called_with(table_name, limit, 'test-access-key', 'test-secret-key')
    assert mock_get_table_sample.call_count == 2

@pytest.mark.asyncio
async def test_get_table_sample_integration_missing_headers(client):
    """Test get table sample fails with 401 if headers are missing."""
    # Act
    response = await client.get(f"/vast/tables/some_table", headers={}) # Override default headers
    # Assert
    assert response.status_code == 401
    assert "Missing required authentication headers" in response.text # Default is text/plain error

@pytest.mark.asyncio
async def test_get_table_sample_integration_success_json(client, mocker):
    """Test successful table sample fetch via vast://tables/{t} (JSON)."""
    # Arrange
    table_name = "users"
    limit = 3
    db_data = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
    mock_get_table_sample = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
    mock_get_table_sample.return_value = db_data

    # Act
    response = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")

    # Assert
    expected_json = json.dumps(db_data, indent=2)
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    assert response.text == expected_json
    mock_get_table_sample.assert_called_once_with(table_name, limit, 'test-access-key', 'test-secret-key')

@pytest.mark.asyncio
async def test_get_table_sample_integration_db_error(client, mocker):
    """Test table sample DB error via vast://tables/{t} integration."""
    # Arrange
    table_name = "bad_data_table"
    limit = 10
    error_msg = "Failed to query table"
    db_exception = QueryExecutionError(error_msg)
    mock_get_table_sample = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
    mock_get_table_sample.side_effect = db_exception

    # Act
    response_json = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")
    response_csv = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=csv")

    # Assert JSON
    assert response_json.status_code == 200 # Handler returns 200 with error
    assert response_json.headers['content-type'] == "application/json"
    expected_json_error = {"error": {"type": "QueryExecutionError", "message": error_msg}}
    assert response_json.json() == expected_json_error

    # Assert CSV
    assert response_csv.status_code == 200 # Handler returns 200 with error
    assert response_csv.headers['content-type'] == "text/plain; charset=utf-8"
    expected_csv_error = f"ERROR: [QueryExecutionError] {error_msg}"
    assert response_csv.text == expected_csv_error

    mock_get_table_sample.assert_called_with(table_name, limit, 'test-access-key', 'test-secret-key')
    assert mock_get_table_sample.call_count == 2

@pytest.mark.asyncio
async def test_get_table_sample_integration_invalid_input(client, mocker):
    """Test table sample invalid input via vast://tables/{t} integration."""
    # Arrange
    table_name = "invalid-name!"
    limit = 10
    error_msg = f"Invalid table name '{table_name}'."
    db_exception = InvalidInputError(error_msg)
    mock_get_table_sample = mocker.patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
    mock_get_table_sample.side_effect = db_exception

    # Act
    response_json = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")
    response_csv = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=csv")

    # Assert JSON (Should be 400 based on InvalidInputError? Handler returns 200 now)
    assert response_json.status_code == 200
    assert response_json.headers['content-type'] == "application/json"
    expected_json_error = {"error": {"type": "InvalidInputError", "message": error_msg}}
    assert response_json.json() == expected_json_error

    # Assert CSV
    assert response_csv.status_code == 200
    assert response_csv.headers['content-type'] == "text/plain; charset=utf-8"
    expected_csv_error = f"ERROR: [InvalidInputError] {error_msg}"
    assert response_csv.text == expected_csv_error

    # Check db_ops was called because validation happens there
    mock_get_table_sample.assert_called_with(table_name, limit, 'test-access-key', 'test-secret-key')
    assert mock_get_table_sample.call_count == 2

# --- End Integration Tests for resources/table_data.py --- #

# --- Integration Tests for resources/metadata.py (Update existing tests) ---

@pytest.mark.asyncio
async def test_get_table_metadata_success(client, mocker):
    """Test successful enhanced metadata fetch."""
    # Arrange
    table_name = "test_table"
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    # Simulate DESCRIBE TABLE results with richer info
    mock_cursor.fetchall.return_value = [
        ('id', 'INTEGER', 'NO', 'PRI', None, 'auto_increment'), # Not nullable, primary key
        ('name', 'VARCHAR(100)', 'YES', '', '''Unknown''', ''), # Nullable, no key, default value 'Unknown'
        ('ts', 'TIMESTAMP', 'YES', None, None, None) # Nullable, missing key/default/extra info from DB
    ]

    # Patch create_vast_connection as it's called by _get_table_metadata_sync
    mock_create_conn = mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection')
    mock_create_conn.return_value = mock_conn

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}") # Uses default headers

    # Assert
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    expected_data = {
        "table_name": table_name,
        "columns": [
            {
                "name": "id",
                "type": "INTEGER",
                "is_nullable": "NO",
                "key": "PRI",
                "default": None,
                # "extra": "auto_increment" # We didn't parse index 5 yet
            },
            {
                "name": "name",
                "type": "VARCHAR(100)",
                "is_nullable": "YES",
                "key": "",
                "default": "'Unknown'",
                # "extra": ""
            },
            {
                "name": "ts",
                "type": "TIMESTAMP",
                "is_nullable": "YES",
                "key": None, # Parsed as None because index 3 was missing
                "default": None, # Parsed as None because index 4 was missing
                # "extra": None
            }
        ]
    }
    assert response.json() == expected_data
    # Check create_vast_connection was called correctly
    mock_create_conn.assert_called_once_with('test-access-key', 'test-secret-key')
    # Check DESCRIBE was executed
    mock_cursor.execute.assert_called_once_with(f"DESCRIBE TABLE {table_name}")
    mock_conn.close.assert_called_once()

@pytest.mark.asyncio
async def test_get_table_metadata_missing_headers(client):
    """Test get metadata fails with 401 if headers are missing."""
    # Act
    response = await client.get(f"/vast/metadata/tables/any_table", headers={})
    # Assert
    assert response.status_code == 401
    assert "Missing required authentication headers" in response.json()["error"]

@pytest.mark.asyncio
async def test_get_table_metadata_auth_fail_db(client, mocker):
    """Test get metadata fails with 401 if DB connection indicates auth failure."""
    # Arrange
    table_name = "auth_fail_table"
    error_msg = "Authentication failed"
    db_exception = DatabaseConnectionError(error_msg)
    # Patch create_vast_connection as it's called first
    mock_create_conn = mocker.patch('vast_mcp_server.vast_integration.db_ops.create_vast_connection')
    mock_create_conn.side_effect = db_exception

    # Act
    response = await client.get(f"/vast/metadata/tables/{table_name}") # Uses default headers

    # Assert
    assert response.status_code == 401
    assert response.json() == {"error": "Authentication failed with provided credentials."}
    mock_create_conn.assert_called_once_with('test-access-key', 'test-secret-key')


# ... (Update other metadata tests: connection error (non-auth), not found, invalid input, etc.) ...
# Ensure mocks target appropriate functions (create_vast_connection or get_table_metadata)
# Ensure calls are checked for correct keys being passed.

# --- Unit Tests (Can be removed or kept) ---
# If keeping, they no longer represent the full request flow.

# ... (Remove or adjust existing unit tests) ...


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