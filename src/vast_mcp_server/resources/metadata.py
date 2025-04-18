import logging
import json
from urllib.parse import urlparse, unquote
from mcp_core.resource import Resource
from mcp_core.mcp_response import McpResponse, StatusCode
from ..vast_integration import db_ops
from ..exceptions import VastMcpError, InvalidInputError, TableDescribeError, DatabaseConnectionError

logger = logging.getLogger(__name__)

class TableMetadataResource(Resource):
    """
    MCP Resource handler for fetching metadata of a specific VAST DB table.
    Handles URIs like: vast://metadata/tables/{table_name}
    """

    async def can_handle(self, uri: str) -> bool:
        """Checks if this resource can handle the given URI."""
        parsed_uri = urlparse(uri)
        # Check scheme, netloc, and path structure
        return (
            parsed_uri.scheme == "vast" and
            parsed_uri.netloc == "metadata" and
            parsed_uri.path.startswith("/tables/") and
            len(parsed_uri.path.strip('/').split('/')) == 2 # e.g., 'tables/my_table'
        )

    async def get(self, uri: str, headers: dict = None) -> McpResponse:
        """Handles GET requests for table metadata."""
        parsed_uri = urlparse(uri)
        path_parts = parsed_uri.path.strip('/').split('/') # Should be ['tables', 'table_name']

        if len(path_parts) != 2 or path_parts[0] != 'tables':
            logger.warning("Invalid URI path format for TableMetadataResource: %s", uri)
            return McpResponse(
                status_code=StatusCode.BAD_REQUEST,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Invalid URI format. Expected vast://metadata/tables/{table_name}"}).encode()
            )

        table_name = unquote(path_parts[1])
        logger.info("Handling GET request for metadata of table: %s", table_name)

        try:
            metadata = await db_ops.get_table_metadata(table_name)
            response_body = json.dumps(metadata, indent=2)
            return McpResponse(
                status_code=StatusCode.OK,
                headers={"Content-Type": "application/json"},
                body=response_body.encode('utf-8')
            )
        except InvalidInputError as e:
            logger.warning("Invalid input for table metadata request '%s': %s", table_name, e)
            return McpResponse(
                status_code=StatusCode.BAD_REQUEST,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": f"Invalid input: {e}"}).encode('utf-8')
            )
        except TableDescribeError as e:
            # This often indicates the table doesn't exist or access issues
            logger.warning("Error describing table '%s': %s", table_name, e)
            # Distinguish between Not Found and other describe errors
            if "exist" in str(e).lower() or "no columns" in str(e).lower():
                 status_code = StatusCode.NOT_FOUND
                 error_message = f"Table '{table_name}' not found or metadata unavailable."
            else:
                 # Treat other describe failures as internal errors
                 status_code = StatusCode.INTERNAL_SERVER_ERROR
                 error_message = f"Failed to retrieve metadata for table '{table_name}'."

            return McpResponse(
                status_code=status_code,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": error_message, "details": str(e)}).encode('utf-8')
            )
        except DatabaseConnectionError as e:
            logger.error("Database connection error during metadata fetch for table '%s': %s", table_name, e, exc_info=True)
            return McpResponse(
                status_code=StatusCode.SERVICE_UNAVAILABLE, # Or Internal Server Error
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "Database connection error"}).encode('utf-8') # Avoid exposing detailed DB error externally
            )
        except Exception as e:
            # Catch-all for unexpected errors
            logger.error("Unexpected error fetching metadata for table '%s': %s", table_name, e, exc_info=True)
            return McpResponse(
                status_code=StatusCode.INTERNAL_SERVER_ERROR,
                headers={"Content-Type": "application/json"},
                body=json.dumps({"error": "An unexpected server error occurred."}).encode('utf-8')
            )

# Note: Resource registration is likely handled elsewhere (e.g., in server setup or a resource registry)
# Ensure this class is imported and registered where needed. 