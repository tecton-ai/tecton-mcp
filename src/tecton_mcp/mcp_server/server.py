"""
FastMCP server implementation for Tecton.
"""


import json
import logging
import os
import sys
import re
import subprocess
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator, List, Callable, Any, Dict, Set, Optional
from importlib.metadata import version, PackageNotFoundError

from tecton_mcp.tools.api_reference_tools import get_full_sdk_reference
from tecton_mcp.tools.example_code_snippet_tools import load_example_code_snippet_index
from tecton_mcp.tools.documentation_tools import load_documentation_index
from tecton_mcp.tools.feature_service_tool_library import (
    register_tecton_feature_service_as_tools,
)
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

import tecton
from tecton._internals.utils import cluster_url

# Only register FeatureServices as tools if TECTON_API_KEY is set
if os.environ.get("TECTON_API_KEY"):
    current_workspace = tecton.get_current_workspace()
    tecton_cluster_url = cluster_url()
    register_tecton_feature_service_as_tools(current_workspace, mcp, tecton_cluster_url)
    logger.info("FeatureServices registered as tools")
else:
    logger.warning("No TECTON_API_KEY found - FeatureServices will not be registered as tools")

logger.info("Tecton MCP Server initialized")

if os.environ.get("MCP_SMOKE_TEST"):
    logger.info("MCP_SMOKE_TEST is set. Exiting after initialization.")
    raise SystemExit(0)

def _get_local_tecton_version() -> Optional[str]:
    """Get the Tecton version from the local workspace environment (not the MCP server environment)."""
    try:
        # Try to get version via tecton version command
        result = subprocess.run(
            ["tecton", "version", "--output", "json"], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
        if result.returncode == 0:
            try:
                version_info = json.loads(result.stdout)
                return version_info.get("tecton_sdk_version")
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Fallback: try to get version via Python import in a subprocess
        result = subprocess.run([
            sys.executable, "-c", 
            "import tecton; print(getattr(tecton, '__version__', 'unknown'))"
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            return result.stdout.strip()
            
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        pass
    
    return None

def _get_mcp_server_tecton_version() -> Optional[str]:
    """Get the Tecton version used by the MCP server."""
    try:
        import tecton
        return getattr(tecton, "__version__", None)
    except ImportError:
        return None

def _parse_version(version_str: str) -> Optional[tuple]:
    """Parse version string into comparable tuple (major, minor, patch)."""
    if not version_str:
        return None
    
    # Extract major.minor.patch from version strings like "1.1.0", "1.2.0b12", "1.0.3rc1"
    match = re.match(r"^(\d+)\.(\d+)\.(\d+)", version_str)
    if match:
        return (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return None

def _get_workspace_info() -> Dict[str, Any]:
    """Get current workspace and cluster information."""
    try:
        import tecton
        from tecton._internals.utils import cluster_url
        
        workspace = tecton.get_current_workspace()
        cluster = cluster_url()
        
        return {
            "workspace": workspace,
            "cluster": cluster,
            "logged_in": True
        }
    except Exception as e:
        return {
            "workspace": None,
            "cluster": None, 
            "logged_in": False,
            "error": str(e)
        }

@mcp.tool()
@sdk_public_method
def diagnose_tecton_environment_tool(ctx: Context) -> str:
    """
    Diagnoses the Tecton environment and checks for version mismatches between 
    the MCP server and local workspace/cluster environment.
    
    This tool helps identify configuration issues such as:
    - Version mismatches between MCP server and local environment
    - Authentication/login status
    - Current workspace and cluster information
    - Recommendations for fixing configuration issues
    
    Use this tool when users report issues or when you want to verify the 
    environment is properly configured.
    """
    ctx.info("Running Tecton environment diagnostics...")
    
    # Get version information
    mcp_version = _get_mcp_server_tecton_version()
    local_version = _get_local_tecton_version()
    workspace_info = _get_workspace_info()
    
    # Parse versions for comparison
    mcp_parsed = _parse_version(mcp_version) if mcp_version else None
    local_parsed = _parse_version(local_version) if local_version else None
    
    # Build diagnostic report
    report = []
    report.append("=== Tecton Environment Diagnostics ===\n")
    
    # Version information
    report.append("📋 VERSION INFORMATION:")
    report.append(f"• MCP Server Tecton Version: {mcp_version or 'Unknown'}")
    report.append(f"• Local Environment Tecton Version: {local_version or 'Unable to detect'}")
    
    # Version compatibility check
    version_status = "✅ OK"
    version_advice = ""
    
    if not mcp_version:
        version_status = "❌ ERROR"
        version_advice = "MCP server Tecton version could not be determined"
    elif not local_version:
        version_status = "⚠️  WARNING"
        version_advice = "Local Tecton version could not be detected. Ensure Tecton is installed and accessible."
    elif mcp_version != local_version:
        if mcp_parsed and local_parsed:
            # Check if major.minor versions match (patch differences are usually OK)
            if mcp_parsed[:2] != local_parsed[:2]:
                version_status = "❌ MISMATCH"
                version_advice = f"Version mismatch detected! MCP server uses {mcp_version}, but local environment uses {local_version}."
            else:
                version_status = "⚠️  MINOR DIFF"
                version_advice = f"Minor version difference detected (MCP: {mcp_version}, Local: {local_version}). This should be OK but may cause minor inconsistencies."
        else:
            version_status = "⚠️  DIFFERENT"
            version_advice = f"Version difference detected (MCP: {mcp_version}, Local: {local_version})"
    
    report.append(f"• Version Status: {version_status}")
    if version_advice:
        report.append(f"• Version Analysis: {version_advice}")
    
    # Workspace and authentication information
    report.append("\n🔑 AUTHENTICATION & WORKSPACE:")
    if workspace_info["logged_in"]:
        report.append(f"• Authentication Status: ✅ Logged in")
        report.append(f"• Current Workspace: {workspace_info['workspace']}")
        report.append(f"• Cluster URL: {workspace_info['cluster']}")
    else:
        report.append(f"• Authentication Status: ❌ Not logged in or error")
        if workspace_info.get("error"):
            report.append(f"• Error: {workspace_info['error']}")
    
    # Recommendations section
    report.append("\n💡 RECOMMENDATIONS:")
    
    if version_status.startswith("❌"):
        report.append("🔧 VERSION MISMATCH DETECTED:")
        report.append("   To fix version mismatches, follow these steps:")
        report.append("   1. Update your MCP server configuration to match your local Tecton version")
        report.append(f"   2. Edit your pyproject.toml to pin Tecton to version {local_version}:")
        report.append("      ```toml")
        report.append("      dependencies = [")
        report.append(f'          "tecton=={local_version}"')
        report.append("      ]")
        report.append("      ```")
        report.append("   3. Remove the existing lock file: `rm uv.lock`")
        report.append("   4. Test the configuration: `MCP_SMOKE_TEST=1 uv run mcp run src/tecton_mcp/mcp_server/server.py`")
        report.append("   5. Restart Cursor to load the updated MCP server")
        report.append("   📖 For detailed instructions, see the 'How to Use Specific Tecton SDK Version' section in the README")
    
    if not workspace_info["logged_in"]:
        report.append("🔧 AUTHENTICATION REQUIRED:")
        report.append("   • Run `tecton login yourcluster.tecton.ai` to authenticate")
        report.append("   • Verify your workspace with `tecton workspace list`")
        report.append("   • Select appropriate workspace with `tecton workspace select <name>`")
    
    if version_status == "✅ OK" and workspace_info["logged_in"]:
        report.append("✅ Environment looks good! No issues detected.")
        report.append("   • Versions are compatible")
        report.append("   • Authentication is working")
        report.append("   • Ready for feature development")
    
    report.append("\n🆘 TROUBLESHOOTING:")
    report.append("   • If issues persist, run MCP server in diagnostics mode:")
    report.append("     `uv run mcp dev src/tecton_mcp/mcp_server/server.py`")
    report.append("   • Check the README troubleshooting section for more help")
    
    return "\n".join(report)

