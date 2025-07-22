"""
Tools for querying Tecton observability metrics.
This module provides functionality to query the Tecton metrics API endpoint.
"""

import logging
import os
import re
from typing import Optional, Dict, List, Any
import requests
from prometheus_client.parser import text_string_to_metric_families

logger = logging.getLogger(__name__)


def get_tecton_cluster_url() -> str:
    """Get the current Tecton cluster URL."""
    import tecton
    from tecton._internals.utils import cluster_url
    return cluster_url()


def parse_openmetrics_to_readable(
    metrics_text: str, 
    metric_filter: Optional[str] = None
) -> str:
    """
    Parse OpenMetrics format text and convert to human-readable format.
    
    Args:
        metrics_text: Raw OpenMetrics response text
        metric_filter: Optional regex pattern to filter metrics
        
    Returns:
        Formatted string with current metrics
    """
    parsed_metrics = []
    
    try:
        # Use the official Prometheus client parser
        for metric_family in text_string_to_metric_families(metrics_text):
            metric_name = metric_family.name
            
            # Apply filter if specified
            if metric_filter:
                try:
                    if not re.search(metric_filter, metric_name):
                        continue
                except re.error as e:
                    logger.warning(f"Invalid regex pattern '{metric_filter}': {e}")
                    # Skip filtering on invalid regex
                    continue
            
            # Process each sample in the metric family
            for sample in metric_family.samples:
                # Convert labels dict to a more readable format
                labels = {}
                if sample.labels:
                    labels = dict(sample.labels)
                
                parsed_metrics.append({
                    'name': sample.name,
                    'value': sample.value,
                    'labels': labels,
                    'timestamp': None  # Prometheus parser doesn't preserve timestamps
                })
    
    except Exception as e:
        logger.error(f"Error parsing OpenMetrics text: {e}")
        return f"Error parsing metrics: {e}"
    
    # Sort by metric name for consistent output
    parsed_metrics.sort(key=lambda x: x['name'])
    
    # Format output
    if not parsed_metrics:
        return "No metrics found matching the filter criteria."
    
    output = ["Current Tecton Metrics:"]
    
    for metric in parsed_metrics:
        metric_line = f"- {metric['name']}: {metric['value']}"
        
        if metric['labels']:
            label_str = ", ".join([f"{k}={v}" for k, v in metric['labels'].items()])
            metric_line += f" ({label_str})"
            
        output.append(metric_line)
    
    return "\n".join(output)


def query_tecton_metrics(
    metric_filter: Optional[str] = None,
    output_format: str = "formatted"
) -> str:
    """
    Query current Tecton metrics from the observability endpoint.
    
    Args:
        metric_filter: Optional regex pattern to filter metrics
        output_format: "raw" for OpenMetrics format, "formatted" for human-readable
        
    Returns:
        String containing the metrics data
    """
    api_key = os.environ.get("TECTON_API_KEY")
    if not api_key:
        return "Error: TECTON_API_KEY environment variable not set"
    
    try:
        # Get cluster URL
        cluster_url = get_tecton_cluster_url()
        metrics_url = f"{cluster_url}/api/v1/observability/metrics"
        
        headers = {
            "Authorization": f"Tecton-key {api_key}"
        }
        
        logger.info(f"Querying metrics from: {metrics_url}")
        response = requests.get(metrics_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Get the raw metrics data
        metrics_data = response.text
        
        if output_format == "raw":
            return metrics_data
        
        # Parse and format for human readability
        return parse_openmetrics_to_readable(metrics_data, metric_filter)
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            return "Error: Authentication failed. Please check your TECTON_API_KEY."
        elif e.response.status_code == 403:
            return "Error: Access denied. Your API key may not have permission to access metrics."
        else:
            return f"Error fetching metrics: HTTP {e.response.status_code} - {e.response.text}"
    except requests.exceptions.Timeout:
        return "Error: Request timed out while fetching metrics."
    except requests.exceptions.ConnectionError:
        return "Error: Could not connect to Tecton cluster. Please check your network connection."
    except Exception as e:
        logger.error(f"Unexpected error querying metrics: {e}")
        return f"Unexpected error: {e}"


def register_metrics_api_tool(mcp_server, cluster_url: str) -> None:
    """
    Register the metrics API tool with the MCP server.
    
    Args:
        mcp_server: The FastMCP server instance
        cluster_url: The Tecton cluster URL
    """
    from mcp.server.fastmcp import Context
    from tecton._internals.sdk_decorators import sdk_public_method
    
    @mcp_server.tool()
    @sdk_public_method
    def query_tecton_metrics_tool(
        metric_filter: Optional[str] = None,
        output_format: str = "formatted",
        ctx: Context = None
    ) -> str:
        """
        Query current Tecton metrics from the observability endpoint.
        
        This tool fetches point-in-time metrics from Tecton's Prometheus-compatible
        scrape endpoint. It returns current system state, not historical data.
        
        Args:
            metric_filter: Optional regex pattern to filter metrics (e.g., "tecton_feature_store.*")
            output_format: "raw" for OpenMetrics format, "formatted" for human-readable
            
        Returns:
            String containing the current metrics data
            
        Examples:
            - Get all current metrics: query_tecton_metrics_tool()
            - Get only feature store metrics: query_tecton_metrics_tool(metric_filter="tecton_feature_store.*")
            - Get materialization metrics: query_tecton_metrics_tool(metric_filter="tecton_materialization.*")
            - Get raw OpenMetrics format: query_tecton_metrics_tool(output_format="raw")
        """
        if ctx:
            ctx.info(f"Querying Tecton metrics with filter: {metric_filter}, output_format: {output_format}")
        
        return query_tecton_metrics(metric_filter, output_format)
    
    logger.info("Metrics API tool registered successfully") 