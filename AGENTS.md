# Agent Instructions

When modifying files in this repository, run the following command to ensure the Tecton MCP server still initializes correctly without launching a long running process:

```bash
MCP_SMOKE_TEST=1 uv run mcp run src/tecton_mcp/mcp_server/server.py
```

This command should exit immediately after showing the initialization log message.
