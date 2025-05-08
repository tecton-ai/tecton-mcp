"""
Service utilities for embedding and vector database functionality.
"""

import os

def set_dev_mode():
    """
    Set the dev mode for testing and local development. In dev mode, you
    don't need to connect to tecton.
    """
    os.environ["TECTON_SKIP_OBJECT_VALIDATION"] = "true"
    os.environ["TECTON_OFFLINE_RETRIEVAL_COMPUTE_MODE"] = "rift"
    os.environ["TECTON_BATCH_COMPUTE_MODE"] = "rift"
    os.environ["TECTON_FORCE_FUNCTION_SERIALIZATION"] = "false"
    os.environ["DUCKDB_EXTENSION_REPO"] = "" 