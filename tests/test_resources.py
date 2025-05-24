import pytest
import json
from unittest.mock import patch, MagicMock, ANY
import httpx

from vast_mcp_server.server import mcp_app
from vast_mcp_server import config as app_config # For patching
from vast_mcp_server.exceptions import (
    DatabaseConnectionError,
    SchemaFetchError,
    TableDescribeError,
    QueryExecutionError,
    InvalidInputError
)

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# --- Fixture for Async Test Client ---
@pytest.fixture
async def client():
    # These headers will be used for successful authentication matching the patched config
    default_headers = {
        'X-Vast-Access-Key': 'config_access_key',
        'X-Vast-Secret-Key': 'config_secret_key'
    }
    async with httpx.AsyncClient(app=mcp_app, base_url="http://test", headers=default_headers) as client:
        yield client

# --- Integration Tests for resources/schema.py ---

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_schema_integration_success(mock_db_op_get_db_schema, client, mocker):
    """Test successful schema fetch via vast://schemas integration."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')

    expected_schema_str = "TABLE: t1\n - c1 (INT)\nTABLE: t2\n - c2 (STR)"
    mock_db_op_get_db_schema.return_value = expected_schema_str

    response = await client.get("/vast/schemas")

    assert response.status_code == 200
    assert response.headers['content-type'] == "text/plain; charset=utf-8"
    assert response.text == expected_schema_str
    mock_db_op_get_db_schema.assert_called_once_with(ANY) # ANY for the db_connection object

@pytest.mark.asyncio
async def test_get_schema_integration_missing_headers(client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    response = await client.get("/vast/schemas", headers={})
    assert response.status_code == 401
    assert response.json()["error"] == "Authentication error"
    assert "Missing required authentication headers" in response.json()["details"]

@pytest.mark.asyncio
async def test_get_schema_integration_mismatched_credentials(client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'actual_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'actual_secret')
    # Client sends 'config_access_key', 'config_secret_key' by default
    response = await client.get("/vast/schemas")
    assert response.status_code == 401
    assert response.json()["error"] == "Provided credentials do not match server configuration."

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_schema_integration_db_connection_error_auth(mock_db_op_get_db_schema, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    db_exception = DatabaseConnectionError("Authentication failed for user")
    mock_db_op_get_db_schema.side_effect = db_exception
    response = await client.get("/vast/schemas")
    assert response.status_code == 401 # This maps to UNAUTHENTICATED
    assert "Database authentication failed" in response.json()["error"]
    mock_db_op_get_db_schema.assert_called_once_with(ANY)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_schema_integration_db_connection_error_service(mock_db_op_get_db_schema, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    db_exception = DatabaseConnectionError("Some other connection problem")
    mock_db_op_get_db_schema.side_effect = db_exception
    response = await client.get("/vast/schemas")
    assert response.status_code == 503 # SERVICE_UNAVAILABLE
    assert "Database connection error" in response.json()["error"]
    mock_db_op_get_db_schema.assert_called_once_with(ANY)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_schema_integration_schema_fetch_error(mock_db_op_get_db_schema, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    db_exception = SchemaFetchError("Failed to fetch schema details")
    mock_db_op_get_db_schema.side_effect = db_exception
    response = await client.get("/vast/schemas")
    assert response.status_code == 500 # INTERNAL_SERVER_ERROR
    assert "Failed to fetch schema" in response.json()["error"]
    mock_db_op_get_db_schema.assert_called_once_with(ANY)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_db_schema')
async def test_get_schema_integration_unexpected_error(mock_db_op_get_db_schema, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    generic_exception = Exception("Something broke")
    mock_db_op_get_db_schema.side_effect = generic_exception
    response = await client.get("/vast/schemas")
    assert response.status_code == 500
    assert "An unexpected server error occurred" in response.json()["error"]
    mock_db_op_get_db_schema.assert_called_once_with(ANY)


# --- Integration Tests for resources/table_data.py: list_vast_tables ---

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_tables_integration_success_json(mock_db_op_list_tables, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_list = ["users", "orders"]
    mock_db_op_list_tables.return_value = table_list
    response = await client.get("/vast/tables?format=json")
    expected_json = json.dumps(table_list, indent=2)
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    assert response.text == expected_json
    mock_db_op_list_tables.assert_called_once_with(ANY)

@pytest.mark.asyncio
async def test_list_tables_integration_mismatched_credentials(client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'actual_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'actual_secret')
    response = await client.get("/vast/tables")
    assert response.status_code == 401
    assert response.json()["error"] == "Provided credentials do not match server configuration."

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_tables_integration_success_csv(mock_db_op_list_tables, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_list = ["users", "orders"]
    mock_db_op_list_tables.return_value = table_list
    response_csv = await client.get("/vast/tables?format=csv")
    expected_text = "users\norders\n"
    assert response_csv.status_code == 200
    assert response_csv.headers['content-type'] == "text/csv; charset=utf-8"
    assert response_csv.text == expected_text
    mock_db_op_list_tables.assert_called_once_with(ANY)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.list_tables')
async def test_list_tables_integration_db_error(mock_db_op_list_tables, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    db_exception = QueryExecutionError("Cannot list tables")
    mock_db_op_list_tables.side_effect = db_exception
    response_json = await client.get("/vast/tables?format=json")
    assert response_json.status_code == 500 # QueryExecutionError is an internal error
    assert "A VAST specific error occurred" in response_json.json()["error"] # Standardized
    assert "Cannot list tables" in response_json.json()["details"]
    mock_db_op_list_tables.assert_called_once_with(ANY)

# --- Integration Tests for resources/table_data.py: get_vast_table_sample ---

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_table_sample_integration_success_csv(mock_db_op_get_sample, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "users"
    limit = 5
    db_data = [{'id': 1, 'name': 'A'}, {'id': 2, 'name': 'B'}]
    mock_db_op_get_sample.return_value = db_data
    response_csv = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=csv")
    expected_csv = "id,name\r\n1,A\r\n2,B\r\n"
    assert response_csv.status_code == 200
    assert response_csv.headers['content-type'] == "text/csv; charset=utf-8"
    assert response_csv.text == expected_csv
    mock_db_op_get_sample.assert_called_once_with(ANY, table_name, limit)

@pytest.mark.asyncio
async def test_get_table_sample_integration_mismatched_credentials(client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'actual_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'actual_secret')
    response = await client.get("/vast/tables/any_table?limit=1&format=csv")
    assert response.status_code == 401
    assert response.json()["error"] == "Provided credentials do not match server configuration."


@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_table_sample_integration_db_error(mock_db_op_get_sample, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "bad_data_table"
    limit = 10
    db_exception = QueryExecutionError("Failed to query table")
    mock_db_op_get_sample.side_effect = db_exception
    response_json = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")
    assert response_json.status_code == 500 # QueryExecutionError is an internal error
    assert "Query execution failed" in response_json.json()["error"] # Specific message for QueryExecutionError
    assert "Failed to query table" in response_json.json()["details"]
    mock_db_op_get_sample.assert_called_once_with(ANY, table_name, limit)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_table_sample_integration_not_found(mock_db_op_get_sample, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "non_existent_table"
    limit = 10
    # Simulate the specific error message that leads to NOT_FOUND
    db_exception = QueryExecutionError(f"Table '{table_name}' does not exist.")
    mock_db_op_get_sample.side_effect = db_exception
    response_json = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")
    assert response_json.status_code == 404
    assert f"Table '{table_name}' not found" in response_json.json()["error"]
    mock_db_op_get_sample.assert_called_once_with(ANY, table_name, limit)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_sample')
async def test_get_table_sample_integration_invalid_input(mock_db_op_get_sample, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "invalid-name!" # This validation is now within db_ops
    limit = 10
    db_exception = InvalidInputError(f"Invalid table name '{table_name}'.")
    mock_db_op_get_sample.side_effect = db_exception
    response_json = await client.get(f"/vast/tables/{table_name}?limit={limit}&format=json")
    assert response_json.status_code == 400 # InvalidInputError maps to BAD_REQUEST
    assert "Invalid input" in response_json.json()["error"]
    assert db_exception.args[0] in response_json.json()["details"]
    mock_db_op_get_sample.assert_called_once_with(ANY, table_name, limit)


# --- Integration Tests for resources/metadata.py ---

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_metadata')
async def test_get_table_metadata_success(mock_db_op_get_metadata, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "test_table"
    expected_metadata = {
        "table_name": table_name,
        "columns": [{"name": "id", "type": "INTEGER"}]
    }
    mock_db_op_get_metadata.return_value = expected_metadata
    response = await client.get(f"/vast/metadata/tables/{table_name}")
    assert response.status_code == 200
    assert response.headers['content-type'] == "application/json"
    assert response.json() == expected_metadata
    mock_db_op_get_metadata.assert_called_once_with(ANY, table_name)

@pytest.mark.asyncio
async def test_get_table_metadata_mismatched_credentials(client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'actual_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'actual_secret')
    response = await client.get("/vast/metadata/tables/any_table")
    assert response.status_code == 401
    assert response.json()["error"] == "Provided credentials do not match server configuration."

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_metadata')
async def test_get_table_metadata_db_connection_error_auth(mock_db_op_get_metadata, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "auth_fail_table"
    db_exception = DatabaseConnectionError("authentication failed") # Message indicating auth issue
    mock_db_op_get_metadata.side_effect = db_exception
    response = await client.get(f"/vast/metadata/tables/{table_name}")
    assert response.status_code == 401
    assert "Authentication failed with provided credentials" in response.json()["error"]
    mock_db_op_get_metadata.assert_called_once_with(ANY, table_name)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_metadata')
async def test_get_table_metadata_table_describe_error_not_found(mock_db_op_get_metadata, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "unknown_table"
    # Simulate the specific error message that leads to NOT_FOUND
    db_exception = TableDescribeError(f"Table '{table_name}' does not exist.")
    mock_db_op_get_metadata.side_effect = db_exception
    response = await client.get(f"/vast/metadata/tables/{table_name}")
    assert response.status_code == 404
    assert f"Table '{table_name}' not found or metadata unavailable" in response.json()["error"]
    mock_db_op_get_metadata.assert_called_once_with(ANY, table_name)

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.get_table_metadata')
async def test_get_table_metadata_table_describe_error_internal(mock_db_op_get_metadata, client, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'config_access_key')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'config_secret_key')
    table_name = "problem_table"
    db_exception = TableDescribeError("Some other describe failure.")
    mock_db_op_get_metadata.side_effect = db_exception
    response = await client.get(f"/vast/metadata/tables/{table_name}")
    assert response.status_code == 500
    assert f"Failed to retrieve metadata for table '{table_name}'" in response.json()["error"]
    mock_db_op_get_metadata.assert_called_once_with(ANY, table_name)

# Removed old unit tests that called handlers directly
# ...