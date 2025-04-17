"""Custom exception classes for the VAST MCP Server."""

class VastMcpError(Exception):
    """Base class for exceptions in this application."""
    def __init__(self, message: str, original_exception: Exception | None = None):
        super().__init__(message)
        self.original_exception = original_exception

class DatabaseConnectionError(VastMcpError):
    """Raised when unable to connect to the VAST database."""
    pass

class SchemaFetchError(VastMcpError):
    """Raised when fetching schema information fails."""
    pass

class TableDescribeError(SchemaFetchError):
    """Raised when describing a specific table fails."""
    pass

class QueryExecutionError(VastMcpError):
    """Raised when executing a SQL query fails."""
    pass

class InvalidInputError(VastMcpError):
    """Raised for invalid user/client input (e.g., invalid table name, non-SELECT query)."""
    pass 