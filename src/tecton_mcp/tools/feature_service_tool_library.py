"""Utility to register Tecton Feature Services as FastMCP tools."""

import logging
from typing import Dict, Any, List

import tecton
from mcp.server.fastmcp import FastMCP, Context


def _create_feature_service_tool(
    workspace: str, feature_service_name: str
):
    """Return a callable tool for the given Feature Service."""

    def feature_service_tool(
        join_key_map: Dict[str, Any], request_context_map: Dict[str, Any], ctx: Context
    ) -> List[Dict[str, Any]]:
        from tecton_client import TectonClient  # Local import to avoid hard dependency at import time

        client = TectonClient(
            url="https://explore.tecton.ai/",
            api_key="my-api-key",
            default_workspace_name=workspace,
        )
        resp = client.get_features(
            feature_service_name=feature_service_name,
            join_key_map=join_key_map,
            request_context_map=request_context_map,
        )
        return resp.result.features

    return feature_service_tool


def register_tecton_feature_service_as_tools(workspace: str, server: FastMCP) -> None:
    """Register tools for all Feature Services in a workspace.

    This function is resilient to missing Tecton configuration. If any errors
    occur while fetching feature services, they are logged and the registration
    step is skipped.
    """

    logger = logging.getLogger(__name__)

    try:
        ws = tecton.get_workspace(workspace)
        fs_names = ws.list_feature_services()
    except Exception as e:  # noqa: BLE001 - propagate errors gently
        logger.warning("Unable to list feature services: %s", e)
        return

    for fs_name in fs_names:
        fs = ws.get_feature_service(fs_name)
        description = fs.description or ""
        feature_details = []

        try:
            if hasattr(fs, "get_output_schema"):
                schema = fs.get_output_schema()
                for field in schema:
                    feature_details.append(f"{field.name}: {field.dtype}")
            elif hasattr(fs, "features"):
                for f in fs.features:
                    feature_details.append(f"{f.name}: {getattr(f, 'dtype', 'unknown')}")
        except Exception:
            pass

        if feature_details:
            description = f"{description}\nFeatures: {', '.join(feature_details)}"

        tool_func = _create_feature_service_tool(workspace, fs_name)
        tool_name = f"{fs_name}_tool"
        server.add_tool(tool_func, name=tool_name, description=description)
