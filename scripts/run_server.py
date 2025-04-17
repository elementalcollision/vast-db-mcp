import uvicorn
import os
import sys

# Add the src directory to the Python path
# This allows importing vast_mcp_server directly
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

if __name__ == "__main__":
    # Run the ASGI app hosted in vast_mcp_server.server
    # Uvicorn will look for the `mcp_app` variable in that module
    uvicorn.run(
        "src.vast_mcp_server.server:mcp_app",
        host="0.0.0.0",  # Listen on all available network interfaces
        port=8088,       # Port for the server (changed from 8000)
        reload=True,     # Automatically reload server on code changes
        reload_dirs=["src/vast_mcp_server"], # Directories to watch for reloading
    )
