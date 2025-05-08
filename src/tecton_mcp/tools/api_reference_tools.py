"""
Tools for building and managing the API reference index.
This module provides functionality to build and query the Tecton API reference.
"""

from typing import List, Optional
from tecton_mcp.utils.sdk_introspector import get_sdk_definitions, format_sdk_definitions
from tecton_mcp.tecton_utils import APIGraphBuilder
builder = APIGraphBuilder()  # Instantiate the builder to get the dependency graph

# Define symmetric mappings
SYMMETRIC_MAPPINGS = {
    "BatchFeatureView": "batch_feature_view",
    "batch_feature_view": "BatchFeatureView",
    "StreamFeatureView": "stream_feature_view",
    "stream_feature_view": "StreamFeatureView",
    "RealtimeFeatureView": "realtime_feature_view",
    "realtime_feature_view": "RealtimeFeatureView",
}

def _add_dependencies(item_name: str, graph: dict, filter_set_to_update: set):
    """Recursively adds dependencies of item_name from the graph to the filter set."""
    if item_name not in graph:  # Check if item exists in the graph
        return

    # Get dependencies for the item
    dependencies = graph.get(item_name, {}).get("deps", [])

    for dep in dependencies:
        if dep not in filter_set_to_update:  # Avoid cycles and redundant work
            filter_set_to_update.add(dep)
            _add_dependencies(dep, graph, filter_set_to_update)  # Recurse

def get_full_sdk_reference(filter_list: Optional[List[str]] = None) -> str:
    """Generates a formatted string containing Tecton SDK definitions.

    Expands the filter list based on predefined rules:
    - Includes symmetric counterparts (e.g., BatchFeatureView <-> batch_feature_view).
    - Includes all aggregation functions if 'Aggregate' is requested.
    - Includes all dependencies (based on type hints) for requested items.

    Args:
        filter_list: An optional list of class/function names to filter the results.
                     If None or empty, all public definitions are returned.

    Returns:
        A formatted string containing the requested Tecton SDK definitions.
    """
    details, all_defs = get_sdk_definitions()

    if filter_list:
        # Expand the filter list based on rules
        expanded_filter = set(filter_list)
        needs_aggregations = False

        # Apply symmetric mappings and check for Aggregate request
        for item in filter_list:
            if item in SYMMETRIC_MAPPINGS:
                expanded_filter.add(SYMMETRIC_MAPPINGS[item])
            if item == 'Aggregate':
                needs_aggregations = True

        # Add aggregation functions if Aggregate was requested
        if needs_aggregations:
            for name, detail in details.items():
                if detail.get('module') == 'tecton.aggregation_functions':
                    expanded_filter.add(name)

        # Add dependencies using APIGraphBuilder's graph
        items_to_check_deps = list(expanded_filter) # Check dependencies for the initially requested/expanded items
        for item in items_to_check_deps:
            _add_dependencies(item, builder.graph, expanded_filter) # Pass the set to be modified

        # Filter details and definitions using the final expanded set
        filtered_details = {
            name: detail for name, detail in details.items() if name in expanded_filter
        }
        # Ensure the order respects the original all_defs order where possible
        filtered_all_defs = [name for name in all_defs if name in expanded_filter]
        return format_sdk_definitions(filtered_details, filtered_all_defs)
    else:
        # Return all definitions if no filter list is provided
        return format_sdk_definitions(details, all_defs) 