import logging
import json
import csv
import io
from typing import List, Dict, Any, Union

logger = logging.getLogger(__name__)

# --- Authentication Helper ---

def extract_auth_headers(headers: dict) -> tuple[str, str]:
    """Extracts VAST access and secret keys from request headers.

    This function is used by resource/tool handlers to get client-provided credentials,
    which are then compared against the server's configured credentials for authentication.

    Args:
        headers: The dictionary of request headers. Expected to contain
                 'X-Vast-Access-Key' and 'X-Vast-Secret-Key'.

    Returns:
        A tuple containing (access_key, secret_key).

    Raises:
        ValueError: If the `headers` dictionary is None, or if either
                    'X-Vast-Access-Key' or 'X-Vast-Secret-Key' is missing.
    """
    if headers is None:
        logger.warning("Attempted auth header extraction with None headers.")
        raise ValueError("Authentication headers are missing.")

    # Header keys are case-insensitive in HTTP, but dict keys might be sensitive.
    # Normalize keys to lower case for lookup.
    lower_headers = {k.lower(): v for k, v in headers.items()}

    access_key = lower_headers.get('x-vast-access-key')
    secret_key = lower_headers.get('x-vast-secret-key')

    if not access_key or not secret_key:
        missing = []
        if not access_key: missing.append('X-Vast-Access-Key')
        if not secret_key: missing.append('X-Vast-Secret-Key')
        logger.warning("Missing required authentication headers: %s", ", ".join(missing))
        raise ValueError(f"Missing required authentication headers: {', '.join(missing)}.")

    # Optionally log the found access key *without* the secret key for debugging
    logger.debug("Successfully extracted VAST authentication headers (Access Key: %s).", access_key)
    return access_key, secret_key

# --- Data Formatting Helper ---

def format_data_payload(data: Union[List[Dict[str, Any]], List[str]], format_type: str) -> str:
    """Formats structured data into a string payload, supporting JSON and CSV.

    Used by resource handlers and tools to serialize successful data responses.

    Args:
        data: The data to format, typically a list of dictionaries (for JSON/CSV)
              or a list of strings (for CSV, where each string is a row).
        format_type: The target format. Supported values are "json" and "csv".
                     If an unsupported format is provided, it defaults to JSON.

    Returns:
        A string representing the formatted data.
        - For "json": A JSON string with an indent of 2. Empty list results in "[]".
        - For "csv": A CSV formatted string. Empty list results in an empty string.
                     If `data` is a list of strings, each string is treated as a row.
    """
    if not data: # Handles empty list for both JSON and CSV/list
        return "[]" if format_type == "json" else ""

    if format_type == "json":
        # JSON serialization errors should be caught by the caller's main handler if they occur.
        return json.dumps(data, indent=2)
    elif format_type == "csv":
        output = io.StringIO()
        if data: # Ensure data is not empty before trying to access data[0] or iterate
            if isinstance(data[0], dict):
                # List of Dicts -> CSV
                headers = data[0].keys()
                writer = csv.DictWriter(output, fieldnames=headers)
                writer.writeheader()
                writer.writerows(data)
            else:
                # Assume List of Strings -> simple newline separated
                for item in data:
                    output.write(str(item) + '\n')
        return output.getvalue()
    else:
        logger.warning("Unsupported format_type '%s' in format_data_payload. Defaulting to JSON.", format_type)
        return json.dumps(data, indent=2) # Or raise error, or return as is

# --- Error Formatting Helper for Tools ---

def format_tool_error_response_body(e: Exception, format_type: str) -> str:
    """Formats an exception into a string for tool error responses.

    Tool handlers in MCP typically return strings directly for both success and error.
    This function standardizes the error string format.

    Args:
        e: The exception instance.
        format_type: The target format for the error string. Supported values are:
                     - "json": Returns a JSON string: `{"error": {"type": "ExceptionType", "message": "..."}}`.
                     - Other (e.g., "csv", "text"): Returns a plain text string: `ERROR: [ExceptionType] Message`.

    Returns:
        A string representing the formatted error.
    """
    error_type = type(e).__name__
    message = str(e)
    if format_type == "json":
        error_obj = {"error": {"type": error_type, "message": message}}
        return json.dumps(error_obj)
    else: # Default to plain text for CSV or other non-JSON formats
        return f"ERROR: [{error_type}] {message}"

# --- Other potential utils can go here ---