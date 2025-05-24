import pytest
import json
from unittest.mock import patch, MagicMock

from vast_mcp_server.tools import query
from vast_mcp_server import config as app_config # Renamed for clarity
from vast_mcp_server import utils as app_utils  # For format_tool_error_response_body
from vast_mcp_server.exceptions import (
    QueryExecutionError,
    InvalidInputError,
    DatabaseConnectionError
)
from mcp_server.fastmcp import Context # Assuming this is the correct path
from starlette.requests import Request # For mocking request object

# Mark all tests in this file as asyncio
pytestmark = pytest.mark.asyncio

# Standard mock headers for successful calls matching mocked config
CONFIG_MATCHING_HEADERS = {
    'X-Vast-Access-Key': 'test-config-access-key',
    'X-Vast-Secret-Key': 'test-config-secret-key'
}

MISMATCHED_HEADERS = {
    'X-Vast-Access-Key': 'wrong-access-key',
    'X-Vast-Secret-Key': 'wrong-secret-key'
}

def _get_mock_context_and_db_conn():
    mock_db_conn = MagicMock()
    mock_lifespan_ctx = MagicMock()
    mock_lifespan_ctx.db_connection = mock_db_conn
    mock_request_ctx = MagicMock()
    mock_request_ctx.lifespan_context = mock_lifespan_ctx
    mock_mcp_context = MagicMock(spec=Context)
    mock_mcp_context.request_context = mock_request_ctx
    mock_starlette_request = MagicMock(spec=Request)
    mock_starlette_request.client = MagicMock() # For rate limiter key_func
    mock_starlette_request.client.host = "127.0.0.1"
    return mock_starlette_request, mock_mcp_context, mock_db_conn

# --- Handler-Style Tests for tools/query.py --- #

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_success(mock_execute_sql, mocker):
    """Test successful SQL query execution (default CSV)."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()

    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id,name\r\n1,Alice\r\n" # Assuming default format is csv from _format_results

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)

    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)

@pytest.mark.asyncio
async def test_vast_sql_query_handler_missing_headers(mocker):
    """Test query fails with auth error if headers are missing."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'any-key') # Config needs to exist
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'any-secret')
    mock_req, mock_ctx, _ = _get_mock_context_and_db_conn()
    sql = "SELECT 1"

    result_none = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=None, ctx=mock_ctx)
    result_empty = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers={}, ctx=mock_ctx)
    result_partial = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers={'X-Vast-Access-Key': 'key'}, ctx=mock_ctx)
    
    auth_error_missing = ValueError("Authentication headers are missing.")
    auth_error_partial_secret = ValueError("Missing required authentication headers: X-Vast-Secret-Key.")
    auth_error_partial_access = ValueError("Missing required authentication headers: X-Vast-Access-Key.")


    assert result_none == app_utils.format_tool_error_response_body(auth_error_missing, "csv")
    # The exact message can vary slightly based on which key is checked first or if both are missing.
    # utils.extract_auth_headers raises "Missing required authentication headers: X-Vast-Access-Key, X-Vast-Secret-Key."
    # if both are missing when headers dict is empty.
    if 'X-Vast-Access-Key' in app_utils.format_tool_error_response_body(ValueError("Missing required authentication headers: X-Vast-Access-Key, X-Vast-Secret-Key."),"csv"):
         assert result_empty == app_utils.format_tool_error_response_body(ValueError("Missing required authentication headers: X-Vast-Access-Key, X-Vast-Secret-Key."), "csv")
    else:
         assert result_empty == app_utils.format_tool_error_response_body(auth_error_partial_access, "csv")

    assert result_partial == app_utils.format_tool_error_response_body(auth_error_partial_secret, "csv")


@pytest.mark.asyncio
async def test_vast_sql_query_handler_mismatched_credentials(mocker):
    """Test query fails if header credentials do not match config."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', 'actual-key-from-config')
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', 'actual-secret-from-config')
    mock_req, mock_ctx, _ = _get_mock_context_and_db_conn()
    sql = "SELECT 1"

    # MISMATCHED_HEADERS has 'wrong-access-key', 'wrong-secret-key'
    result = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=MISMATCHED_HEADERS, ctx=mock_ctx)
    
    expected_error = ValueError("Provided credentials do not match server configuration.")
    assert result == app_utils.format_tool_error_response_body(expected_error, "csv")

@pytest.mark.asyncio
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_db_connection_unavailable(mock_execute_sql, mocker):
    """Test query fails if DB connection is not available in context."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, _ = _get_mock_context_and_db_conn()
    mock_ctx.request_context.lifespan_context.db_connection = None # Simulate no DB conn
    sql = "SELECT 1"

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)
    
    conn_error = DatabaseConnectionError("Database connection unavailable.")
    assert result == app_utils.format_tool_error_response_body(conn_error, "csv")
    mock_execute_sql.assert_not_called()


@pytest.mark.parametrize("sql, allowed_types_config, should_pass", [
    ("SELECT * FROM t", ["SELECT"], True),
    ("INSERT INTO t VALUES (1)", ["SELECT"], False),
    ("INSERT INTO t VALUES (1)", ["SELECT", "INSERT"], True),
])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_type_validation(mock_execute_sql, mocker, sql, allowed_types_config, should_pass):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mocker.patch.object(app_config, 'ALLOWED_SQL_TYPES', allowed_types_config)
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()
    
    db_result = [{'id': 1}] # Dummy result for success cases
    mock_execute_sql.return_value = db_result

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)

    if should_pass:
        assert "ERROR:" not in result
        mock_execute_sql.assert_called_once_with(mock_db_conn, sql)
    else:
        assert result.startswith("ERROR: [InvalidInputError]")
        assert "not allowed. Allowed types:" in result
        mock_execute_sql.assert_not_called()

@pytest.mark.parametrize("format_type", ["csv", "json"])
@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_db_exception(mock_execute_sql, mocker, format_type):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()

    sql = "SELECT bad_col FROM t"
    db_exception = QueryExecutionError("Column not found")
    mock_execute_sql.side_effect = db_exception

    result = await query.vast_sql_query(request=mock_req, sql=sql, format=format_type, headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)
    
    expected_error_str = app_utils.format_tool_error_response_body(db_exception, format_type)
    assert result == expected_error_str
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)


@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_format_success_json(mock_execute_sql, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()
    sql = "SELECT id, name FROM users WHERE id = 1"
    db_result = [{'id': 1, 'name': 'Alice'}]
    mock_execute_sql.return_value = db_result
    expected_json_str = json.dumps(db_result, indent=2)

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="json", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)

    assert result == expected_json_str
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_format_invalid_defaults_csv(mock_execute_sql, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()
    sql = "SELECT id FROM t"
    db_result = [{'id': 1}]
    mock_execute_sql.return_value = db_result
    expected_csv = "id\r\n1\r\n"

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="yaml", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)

    assert result == expected_csv
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_db_message_string(mock_execute_sql, mocker):
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()
    sql = "SELECT * FROM empty_table"
    db_message = "-- Query executed successfully, but returned no rows. --"
    mock_execute_sql.return_value = db_message

    result_csv = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)
    
    assert result_csv == db_message
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)

@patch('vast_mcp_server.vast_integration.db_ops.execute_sql_query')
async def test_vast_sql_query_handler_unexpected_type_error(mock_execute_sql, mocker):
    """Test error handling for unexpected data type from db_ops."""
    mocker.patch.object(app_config, 'VAST_ACCESS_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Access-Key'])
    mocker.patch.object(app_config, 'VAST_SECRET_KEY', CONFIG_MATCHING_HEADERS['X-Vast-Secret-Key'])
    mock_req, mock_ctx, mock_db_conn = _get_mock_context_and_db_conn()
    sql = "SELECT 1"
    mock_execute_sql.return_value = 123 # Not a list or string

    result = await query.vast_sql_query(request=mock_req, sql=sql, format="csv", headers=CONFIG_MATCHING_HEADERS, ctx=mock_ctx)

    expected_error = TypeError("Unexpected internal data format.")
    assert result == app_utils.format_tool_error_response_body(expected_error, "csv")
    mock_execute_sql.assert_called_once_with(mock_db_conn, sql)