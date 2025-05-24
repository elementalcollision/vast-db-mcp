import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
# Useful for local development without setting system-wide env vars
load_dotenv()

# VAST DB Connection Details
# These credentials are used by the server's lifespan manager to establish the primary
# connection to the VAST DB at application startup.
# They also serve as the reference against which incoming API request headers
# (X-Vast-Access-Key, X-Vast-Secret-Key) are validated for authentication.
VAST_DB_ENDPOINT = os.getenv("VAST_DB_ENDPOINT", "http://<your-vast-endpoint>") # Replace with your VAST endpoint or set via .env
VAST_ACCESS_KEY = os.getenv("VAST_ACCESS_KEY", "<your-access-key>")         # Replace or set via .env
VAST_SECRET_KEY = os.getenv("VAST_SECRET_KEY", "<your-secret-key>")         # Replace or set via .env

# --- MCP Server Specific Configuration ---

# Allowed SQL Statement Types (comma-separated list)
# Defaults to only allowing SELECT statements.
# Examples: "SELECT", "SELECT,INSERT,UPDATE", "SELECT,INSERT,UPDATE,DDL"
_allowed_types_str = os.getenv("MCP_ALLOWED_SQL_TYPES", "SELECT")
ALLOWED_SQL_TYPES = [stmt_type.strip().upper() for stmt_type in _allowed_types_str.split(',') if stmt_type.strip()]

# Default Rate Limit (slowapi format string)
DEFAULT_RATE_LIMIT = os.getenv("MCP_DEFAULT_RATE_LIMIT", "10/minute")

# --- Optional Configuration ---
# Add other configuration variables as needed, e.g.:
# DEFAULT_QUERY_LIMIT = 100
