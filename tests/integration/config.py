"""Configuration for integration tests."""

import os
from dataclasses import dataclass
from typing import Optional

@dataclass
class TestConfig:
    """Configuration for integration tests."""
    
    # Tecton cluster configuration
    tecton_cluster: str = "community.tecton.ai"
    tecton_workspace_prefix: str = "mcp_server_integration_test_workspace"
    
    # Claude Code configuration
    claude_code_timeout: int = 900  # 15 minutes
    
    # MCP Server configuration
    mcp_configuration: str = '{"mcpServers": {"tecton": {"command": "uv", "args": ["--directory", "' + os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) + '", "run", "mcp", "run", "src/tecton_mcp/mcp_server/server.py"]}}}'
    
    # Allowed tools for Claude Code
    allowed_tools: list[str] = None
    
    def __post_init__(self):
        """Initialize default values that depend on other fields."""
        if self.allowed_tools is None:
            self.allowed_tools = [
                "Bash(find:*)",
                "Bash(tecton plan:*)",
                "mcp__tecton__query_example_code_snippet_index_tool",
                "mcp__tecton__query_documentation_index_tool",
                "mcp__tecton__query_tecton_sdk_reference_tool",
                "mcp__tecton__get_full_tecton_sdk_reference_tool",
                "Bash(tecton test:*)"
            ]
    
    # Evaluation configuration
    evaluation_model: str = "claude-sonnet-4-20250514"
    evaluation_threshold: int = 70  # Minimum score to pass
    
    # Test environment
    test_root: str = os.path.dirname(os.path.abspath(__file__))
    test_cases_dir: str = os.path.join(test_root, "test_cases")
    
    @property
    def claude_code_cmd(self) -> list[str]:
        """Command to run Claude Code in non-interactive mode."""
        allowed_tools_str = ",".join(self.allowed_tools)
        return ["claude", "--verbose", "--model", self.evaluation_model, "--mcp-config", self.mcp_configuration, "--allowedTools", allowed_tools_str]

# Global test configuration
TEST_CONFIG = TestConfig()