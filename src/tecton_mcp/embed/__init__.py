"""
Simplified embedding and vector database module for Tecton MCP.
"""

from .vector_db import VectorDB
from .service import set_dev_mode

__all__ = ["VectorDB", "set_dev_mode"] 