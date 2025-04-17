import os
from dotenv import load_dotenv

# Load environment variables from a .env file if it exists
# Useful for local development without setting system-wide env vars
load_dotenv()

# VAST DB Connection Details
VAST_DB_ENDPOINT = os.getenv("VAST_DB_ENDPOINT", "http://<your-vast-endpoint>") # Replace with your VAST endpoint or set via .env
VAST_ACCESS_KEY = os.getenv("VAST_ACCESS_KEY", "<your-access-key>")         # Replace or set via .env
VAST_SECRET_KEY = os.getenv("VAST_SECRET_KEY", "<your-secret-key>")         # Replace or set via .env

# --- Optional Configuration ---
# Add other configuration variables as needed, e.g.:
# DEFAULT_QUERY_LIMIT = 100
