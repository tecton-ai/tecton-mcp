"""
FastMCP server implementation for Tecton.
"""


import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, List, Callable, Any, Dict, Set
from importlib.metadata import version, PackageNotFoundError

from tecton_mcp.tools.api_reference_tools import get_full_sdk_reference
from tecton_mcp.tools.example_code_snippet_tools import load_example_code_snippet_index
from tecton_mcp.tools.documentation_tools import load_documentation_index
from tecton_mcp.embed.meta import get_embedding_model
from tecton_mcp.utils.sdk_introspector import get_sdk_definitions
from tecton._internals.sdk_decorators import sdk_public_method

# Set up JSON logging
class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }
        # Add extra fields if they exist
        if hasattr(record, 'extra'):
            log_data.update(record.extra)
        return json.dumps(log_data)

# Configure logger
logger = logging.getLogger("tecton_mcp.mcp_server")
logger.setLevel(logging.INFO)

# Remove any existing handlers to prevent duplicate logs
logger.handlers.clear()

# Add JSON handler
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

# Prevent propagation to root logger to avoid duplicate logs
logger.propagate = False


if os.environ.get("MCP_DEBUG"):
    logger.info("Debug mode is enabled")
    import debugpy
    debugpy.listen(("localhost", 5678))

# Add the src directory to the Python path
src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from mcp.server.fastmcp import Context, FastMCP

try:
    __version__ = version("tecton_mcp")
except PackageNotFoundError:
    __version__ = "unknown"

logger.info(f"Tecton MCP Server version: {__version__}")

INSTRUCTIONS = """
Tecton MCP Server provides a set of tools to help you with Tecton.

Use the tools to:
- get examples of how to build features with Tecton.
- get the API reference for Tecton.


The user must be logged into a Tecton account to use the tools (using `tecton login [url])`
The tools will work in the workspace that the user has currently selected (it can be changed using `tecton workspace select [name]`)
"""


@dataclass
class AppContext:
    """Application context for Tecton MCP server."""
    # Add any shared resources here
    pass


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    """Manage application lifecycle with type-safe context."""
    # Initialize on startup
    try:
        # Dynamic registration moved outside lifespan
        yield AppContext()
    finally:
        # Cleanup on shutdown
        pass


# Pass lifespan to server
mcp = FastMCP("Tecton", lifespan=app_lifespan, instructions=INSTRUCTIONS)

logger.info("Tecton MCP Server initializing...")

query_example_code_snippet_index = load_example_code_snippet_index()
logger.info(f"Loaded example code snippet index using embedding model: {get_embedding_model()}")

query_documentation_index = load_documentation_index()
logger.info(f"Loaded documentation index using embedding model: {get_embedding_model()}")


@mcp.tool()
@sdk_public_method
def query_example_code_snippet_index_tool(query, ctx: Context) -> str:
    """
    Finds relevant Tecton code examples using a vector database.
    It is always helpful to query the examples retriever before generating Tecton code.

    Input query examples:
    - "examples of an Entity"
    - "examples of a KinesisConfig"
    - "examples of a KafkaConfig"
    - "examples of a batch feature view"
    - "examples of a count distinct aggregation feature view"
    - "examples of a percentile aggregation feature view"
    - "examples of a stream feature view"
    - "examples of an aggregation stream feature view"
    - "examples of a realtime feature view"
    - "examples of a realtime feature view that transforms data from another feature view"
    - "examples of a fraud feature"
    - "examples of a recsys case"
    - "examples of a test"

    The output will be a collection of python code examples that use Tecton to implement features, ranked by relevance.
    """
    ctx.info(f"Received query: {query}")
    return query_example_code_snippet_index(query=query)

@mcp.tool()
@sdk_public_method
def query_documentation_index_tool(query, ctx: Context) -> str:
    """
    Retrieves and formats Tecton documentation snippets based on a query.
    Each snippet includes the TECTON DOCUMENTATION URL (Source URL), 
    the section header, and the relevant text chunk.

    Tell the user what documentation URL they can open up to get more information.

    Input query examples:
    - "How do I unit test a Feature View?"
    - "What are Entities in Tecton?"
    - "Explain Batch Feature Views."
    - "How to connect to a Kafka data source?"
    - "Show me how to construct training data."
    - "Tutorial for building realtime features."
    - "How does `tecton apply` work?"
    - "Information about Tecton data types."
    - "What is a Feature Service?"
    - "Scaling the online feature server."
    - "Monitoring materialization jobs."
    """
    ctx.info(f"Received query: {query}")
    return query_documentation_index(query=query)

@mcp.tool()
@sdk_public_method
def get_full_tecton_sdk_reference_tool(ctx: Context) -> str:    
    """Fetches the full Tecton SDK reference. 
    Use this only if you need to get the full SDK reference for all classes/functions.
    If you care only about a subset, use the `query_tecton_sdk_reference_tool` tool instead.
    """

    try:
        return get_full_sdk_reference()
    except Exception as e:
        ctx.error(f"[Static Debug] Error calling get_full_sdk_reference: {e}")
        return f"Error: {e}"


# --- Register dynamic tools here ---
@sdk_public_method
def query_tecton_sdk_reference_tool(class_names: List[str], ctx: Context) -> str:
    """The docstring will be generated dynamically based on the available classes/functions in the _create_dynamic_sdk_reference_tool function below."""
    ctx.info(f"Fetching Tecton SDK reference for: {class_names}")
    # Directly call the function from api_reference_tools
    return get_full_sdk_reference(filter_list=class_names)

# Helper function to create the dynamic tool
def _create_dynamic_sdk_reference_tool() -> tuple[Callable, str, str]:
    """Fetches SDK definitions and creates the tool registration details."""
    logger.info("Fetching Tecton SDK definitions for dynamic tool registration...")
    _details, all_defs = get_sdk_definitions() # Use _details as details are not needed here
    logger.info(f"Found {len(all_defs)} Tecton SDK definitions.")

    # Construct dynamic docstring
    available_classes_str = ", ".join(sorted(all_defs))
    dynamic_docstring = f"""Fetches the Tecton SDK reference for a specific list of classes/functions.

**IMPORTANT:** The `class_names` list **MUST** only contain names from the 'Available classes/functions' list below.
Providing any names *not* in this list will result in an error or empty output.

Use this tool when you need information about specific Tecton components from the allowed list.

Output Format:
- Starts with a bulleted list of the found public classes/functions matching the query.
- Followed by details for each item, including:
    - Type (Class/Function)
    - Name
    - Recommended import path (e.g., `tecton` or `tecton.types`)
    - The definition header (e.g., `class FeatureView(...)` or `def batch_feature_view(...)`)
    - The full docstring.

Available classes/functions:
{available_classes_str}
"""

    return query_tecton_sdk_reference_tool, "query_tecton_sdk_reference_tool", dynamic_docstring


tool_func, tool_name, tool_description = _create_dynamic_sdk_reference_tool()
# Try positional arguments based on the error message
mcp.add_tool(
    tool_func, 
    name=tool_name, 
    description=tool_description
)
# --- End of dynamic tool registration ---

logger.info("Tecton MCP Server initialized")

