# VAST DB MCP Server

This project implements a Model Context Protocol (MCP) server designed to act as an interface between AI agents/LLMs and a VAST Data database.

## Project Goal

To provide a secure and structured way for AI models to query information (schema, data samples) and execute read-only queries against a VAST DB instance using the MCP standard.

## Core Technology

*   **Python:** >=3.9
*   **MCP SDK:** [modelcontextprotocol/python-sdk](https://github.com/modelcontextprotocol/python-sdk) (`mcp-sdk`)
*   **VAST DB SDK:** [vast-data/vastdb_sdk](https://github.com/vast-data/vastdb_sdk) (`vastdb`)
*   **ASGI Server:** `uvicorn`
*   **MCP Implementation:** `FastMCP` from the `mcp-sdk`
*   **Configuration:** `python-dotenv`
*   **Testing:** `pytest`, `pytest-asyncio`, `pytest-mock`

## Project Structure

```
/
├── sdk/                     # Cloned SDKs (ignored by git)
│   ├── python-sdk/
│   └── vastdb_sdk/
├── src/
│   └── vast_mcp_server/     # Main Python package
│       ├── __init__.py
│       ├── server.py          # FastMCP application setup
│       ├── config.py          # Loads connection details from .env
│       ├── resources/         # MCP Resource handlers
│       │   ├── __init__.py
│       │   ├── schema.py      # Handler for vast://schemas
│       │   ├── table_data.py  # Handler for vast://tables/{table_name}
│       │   └── metadata.py    # Handler for vast://metadata/tables/{table_name}
│       ├── tools/             # MCP Tool handlers
│       │   ├── __init__.py
│       │   └── query.py       # Handler for vast_sql_query tool
│       └── vast_integration/  # VAST DB interaction logic
│           ├── __init__.py
│           └── db_ops.py      # Connection & query execution (async wrappers)
├── tests/                   # Pytest unit/integration tests
│   ├── __init__.py
│   ├── test_db_ops.py     # Tests for VAST DB interaction logic
│   ├── test_resources.py  # Tests for MCP resource handlers
│   └── test_tools.py      # Tests for MCP tool handlers
├── scripts/
│   └── run_server.py        # Script to start the server via uvicorn
├── .gitignore
├── .env.example             # Example environment file
├── README.md                # This file
└── pyproject.toml           # Project metadata and dependencies
```

## Implemented MCP Features

*   **Resource: Database Schema**
    *   **URI:** `vast://schemas`
    *   **Description:** Returns a formatted string describing all discovered tables and their columns (name and type). Partial results may be returned if describing some tables fails.
    *   **Error Handling:** Returns `ERROR: [ErrorType] Message` on failure (e.g., connection error, general schema fetch error).
*   **Resource: List Tables**
    *   **URI:** `vast://tables?format=FMT`
    *   **Description:** Returns a list of available table names.
    *   **Parameters:**
        *   `format` (string, optional, default: `json`): Output format (`json` or `csv`/'list'). `csv` or `list` returns a newline-separated string.
    *   **Format:** JSON array of strings, or a newline-separated list.
    *   **Error Handling:** Returns formatted error string (CSV: `ERROR: [ErrorType] Message`, JSON: `{\"error\": {\"type\": \"ErrorType\", \"message\": \"Message\"}}`).
*   **Resource: Table Metadata**
    *   **URI:** `vast://metadata/tables/{table_name}`
    *   **Description:** Returns detailed metadata for a specific table, including column names and types.
    *   **Format:** JSON object containing `table_name` (string) and `columns` (list of objects, each with `name` and `type` keys).
    *   **Example Response:**
        ```json
        {
          "table_name": "my_table",
          "columns": [
            {
              "name": "id",
              "type": "INTEGER"
            },
            {
              "name": "data_column",
              "type": "VARCHAR"
            },
            {
              "name": "timestamp",
              "type": "TIMESTAMP"
            }
          ]
        }
        ```
    *   **Error Handling:** Returns a JSON object with an `error` key on failure (e.g., connection error, table not found, describe error). Returns `NOT_FOUND` status code if the table doesn't exist.
*   **Resource: Table Sample Data**
    *   **URI:** `vast://tables/{table_name}?limit=N&format=FMT`
    *   **Description:** Returns a sample of data from the specified `table_name`.
    *   **Parameters:**
        *   `limit` (integer, optional, default: 10): Maximum number of rows.
        *   `format` (string, optional, default: `csv`): Output format (`csv` or `json`).
    *   **Format:** CSV or JSON string (array of objects), including header row for CSV.
    *   **Error Handling:** Returns formatted error string (CSV: `ERROR: [ErrorType] Message`, JSON: `{"error": {"type": "ErrorType", "message": "Message"}}`) on failure (e.g., connection error, query error, invalid table name).
*   **Tool: SQL Query Executor**
    *   **Name:** `vast_sql_query`
    *   **Arguments:**
        *   `sql` (string, required): The SQL query to execute.
        *   `format` (string, optional, default: `csv`): Output format (`csv` or `json`).
    *   **Description:** Executes the provided SQL query against VAST DB.
    *   **Format:** Returns results as a CSV or JSON string (array of objects) or an error message.
    *   **Safety:** Currently restricted to only allow `SELECT` statements (raises `InvalidInputError` otherwise).
    *   **Error Handling:** Returns formatted error string (CSV: `ERROR: [ErrorType] Message`, JSON: `{"error": {"type": "ErrorType", "message": "Message"}}`) on failure (e.g., connection error, query error).

## How to Run

1.  **Clone the repository** (if you haven't already).
2.  **Set up Environment:** Copy `.env.example` to `.env` and fill in your VAST DB endpoint, access key, and secret key.
    ```bash
    cp .env.example .env
    # Edit .env with your details
    ```
3.  **Install Dependencies:** Using a virtual environment is recommended.
    ```