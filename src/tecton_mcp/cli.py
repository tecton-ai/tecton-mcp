#!/usr/bin/env python
import argparse
import sys
import os

def run_mcp_server():
    """Run the Tecton MCP server."""
    # Add the src directory to the Python path
    import os
    import sys
    src_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    
    from tecton_mcp.mcp_server.server import mcp
    # Get port from environment variable or use default
    port = int(os.environ.get("MCP_PORT", 8080))
    # Set the port in the environment variable so the FastMCP server can use it
    os.environ["MCP_PORT"] = str(port)
    print(f"Starting Tecton MCP server on port {port}...")
    # The port is already set during FastMCP initialization in server.py
    mcp.run()

def main():
    parser = argparse.ArgumentParser(description="Tecton MCP - Tecton Copilot")
    parser.add_argument("--port", type=int, help="Port to run the MCP server on")
    
    args = parser.parse_args()
    
    # Set port in environment if provided
    if args.port:
        os.environ["MCP_PORT"] = str(args.port)
        
    # Run the MCP server
    run_mcp_server()

if __name__ == "__main__":
    main()
