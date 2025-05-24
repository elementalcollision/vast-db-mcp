import logging
from dataclasses import dataclass
from typing import AsyncIterator

import vastdb # Assuming vastdb.api.VastSession is the correct type
from mcp_server.fastmcp import FastMCP # For type hinting server, adjust if path is different

from . import config # For VAST_DB_ENDPOINT, VAST_ACCESS_KEY, VAST_SECRET_KEY

logger = logging.getLogger(__name__)

@dataclass
class LifespanAppContext:
    db_connection: vastdb.api.VastSession # Adjust if vastdb.api.VastSession is not the actual type

async def app_lifespan(server: FastMCP) -> AsyncIterator[LifespanAppContext]:
    """
    Manages the VAST DB connection lifecycle for the application.

    This asynchronous context manager is executed by FastMCP during application
    startup and shutdown. It's responsible for setting up and tearing down
    resources that are shared across the entire application, such as the
    VAST DB connection pool. The yielded context (`LifespanAppContext`)
    is made available to request handlers.
    """
    conn = None
    logger.info("Initializing VAST DB connection...")
    try:
        # Create the VAST DB connection.
        # vastdb.connect() is synchronous, but it's called only once at startup,
        # so its impact on the event loop during application initialization is acceptable.
        conn = vastdb.connect(
            endpoint=config.VAST_DB_ENDPOINT,
            access_key=config.VAST_ACCESS_KEY,
            secret_key=config.VAST_SECRET_KEY
        )
        logger.info("VAST DB connection established.")
        yield LifespanAppContext(db_connection=conn)
    except Exception as e:
        logger.error("Failed to initialize VAST DB connection: %s", e, exc_info=True)
        # Optionally re-raise or handle to prevent server startup if DB is critical
        raise
    finally:
        if conn:
            logger.info("Closing VAST DB connection...")
            try:
                if hasattr(conn, 'close') and callable(conn.close):
                    conn.close() # Assuming close is synchronous
                    logger.info("VAST DB connection closed.")
                else:
                    logger.warning("VAST DB connection object does not have a callable 'close' method.")
            except Exception as e:
                logger.error("Error closing VAST DB connection: %s", e, exc_info=True)
        else:
            logger.info("No VAST DB connection to close (was not established).")
