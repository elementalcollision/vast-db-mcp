import logging

logger = logging.getLogger(__name__)

# --- Authentication Helper ---

def extract_auth_headers(headers: dict) -> tuple[str, str]:
    """Extracts VAST access and secret keys from headers.

    Args:
        headers: The dictionary of request headers.

    Returns:
        A tuple containing (access_key, secret_key).

    Raises:
        ValueError: If headers are missing or keys are not found.
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

# --- Other potential utils can go here --- 