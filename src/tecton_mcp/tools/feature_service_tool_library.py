"""Utility to register Tecton Feature Services as FastMCP tools."""

import logging
from typing import Dict, Any, List, Annotated
import os

import tecton
from tecton.types import ( # Import Tecton data types
    Int64, Int32, Float64, Float32, String, Bool, Array, Struct, Map, Timestamp, SdkDataType
)
from mcp.server.fastmcp import FastMCP
from pydantic import Field
from tecton_client import TectonClient, MetadataOptions


# Helper function to map Tecton DTypes to Python type hints
def _map_tecton_type_to_python_type(tecton_type: SdkDataType) -> Any:
    # Compare the string representation of the Tecton type object.
    type_str = str(tecton_type)

    if type_str == "Int64" or type_str == "Int32":
        return int
    elif type_str == "Float64" or type_str == "Float32":
        return float
    elif type_str == "String":
        return str
    elif type_str == "Bool":
        return bool
    elif isinstance(tecton_type, Array): # Array, Struct, Map are actual classes
        element_type_hint = _map_tecton_type_to_python_type(tecton_type.element_type)
        return List[element_type_hint]
    elif isinstance(tecton_type, Struct):
        return Dict[str, Any]
    elif isinstance(tecton_type, Map):
        key_type_hint = _map_tecton_type_to_python_type(tecton_type.key_type)
        value_type_hint = _map_tecton_type_to_python_type(tecton_type.value_type)
        return Dict[key_type_hint, value_type_hint]
    elif type_str == "Timestamp":
        return Any 
    
    # Fallback for unhandled SdkDataType instances or if str(tecton_type) doesn't match expected primitives
    # This can also catch cases where tecton_type is an instance of Array, Struct, or Map if the isinstance checks above somehow fail
    # or if new SdkDataType primitives are encountered.
    return Any

def _extract_join_keys_from_feature_view(fv, override_map):
    """Helper function to extract join keys from a single feature view."""
    join_keys_info = {}
    if hasattr(fv, 'entities') and fv.entities:
        for entity_obj in fv.entities:
            if entity_obj._spec and entity_obj._spec.join_keys:
                for column_id_obj in entity_obj._spec.join_keys:
                    key_name = column_id_obj.name
                    key_type_obj = column_id_obj.dtype
                    final_key_name = override_map.get(key_name, key_name)
                    join_keys_info[final_key_name] = key_type_obj
    return join_keys_info


def extract_join_keys_map_from_feature_service(fs):
    join_keys_info = {}
    if fs.features:
        for ref in fs.features:
            fv = ref.feature_definition
            override_map = ref.override_join_keys or {}

            # Extract join keys directly from the feature view (works for materialized feature views)
            direct_join_keys = _extract_join_keys_from_feature_view(fv, override_map)
            join_keys_info.update(direct_join_keys)
            
            # For realtime feature views, also check their source feature view dependencies
            if hasattr(fv, 'sources') and fv.sources:
                for source in fv.sources:
                    # Skip RequestSource objects (they don't have join keys)
                    if hasattr(source, 'feature_definition'):
                        # This is a FeatureReference to another feature view
                        source_fv = source.feature_definition
                        source_override_map = source.override_join_keys or {}
                        
                        # Extract join keys from the source feature view
                        source_join_keys = _extract_join_keys_from_feature_view(source_fv, source_override_map)
                        
                        # Apply the main feature reference override to the source join keys
                        for key_name, key_type in source_join_keys.items():
                            final_key_name = override_map.get(key_name, key_name)
                            join_keys_info[final_key_name] = key_type
    return join_keys_info


def extract_request_context_map_from_feature_service(fs):
    request_context_info = {}
    if fs._request_context and fs._request_context.schema:
        for field_name, tecton_type in fs._request_context.schema.items():
            request_context_info[field_name] = tecton_type
    return request_context_info

def _create_feature_service_tool_function(workspace: str, feature_service_name: str, join_keys_info: Dict, request_context_info: Dict, cluster_url: str):
    """Create a function for the given Feature Service with dynamic parameters."""
    
    # Build parameter list and annotations
    param_list = []
    annotations = {}
    
    # Add join key parameters
    for name, tecton_dtype in join_keys_info.items():
        python_type = _map_tecton_type_to_python_type(tecton_dtype)
        param_list.append(f"{name}: Annotated[{python_type.__name__}, Field(description='Join key: {name}')]")
        annotations[name] = Annotated[python_type, Field(description=f"Join key: {name}")]
    
    # Add request context parameters  
    for name, tecton_dtype in request_context_info.items():
        python_type = _map_tecton_type_to_python_type(tecton_dtype)
        param_list.append(f"{name}: Annotated[{python_type.__name__}, Field(description='Request context: {name}')]")
        annotations[name] = Annotated[python_type, Field(description=f"Request context: {name}")]

    # Create function signature
    params_str = ', '.join(param_list)
    
    # Build the function code
    func_code = f"""
def feature_service_function({params_str}) -> List[Dict[str, Any]]:
    '''Execute the feature service with the provided parameters.'''
    
    join_key_map_dyn = {{}}
    request_context_map_dyn = {{}}

    # Extract join keys from function parameters
"""
    
    # Add code to extract join keys
    for name in join_keys_info.keys():
        func_code += f"    join_key_map_dyn['{name}'] = {name}\n"
    
    # Add code to extract request context
    for name in request_context_info.keys():
        func_code += f"    request_context_map_dyn['{name}'] = {name}\n"
    
    func_code += f"""
    from tecton_client import TectonClient, MetadataOptions
    import logging
    import os

    # Log the feature service execution details
    logger = logging.getLogger(__name__)
    logger.info(f"Executing feature service: {feature_service_name} in workspace: {workspace}")
    logger.info(f"Join key map: {{join_key_map_dyn}}")
    logger.info(f"Request context map: {{request_context_map_dyn}}")

    # Get API key from environment variable
    api_key = os.environ.get('TECTON_API_KEY')
    if not api_key:
        raise ValueError("TECTON_API_KEY environment variable is not set. Please set it to your Tecton API key.")

    # Create TectonClient
    client = TectonClient(
        url="{cluster_url}",
        default_workspace_name="{workspace}",
        api_key=api_key
    )

    logger.info(f"Making request to TectonClient with URL: {cluster_url}")
    logger.info(f"Feature service: {feature_service_name}")
    logger.info(f"Join key map: {{join_key_map_dyn}}")
    logger.info(f"Request context map: {{request_context_map_dyn}}")

    # Get features with all metadata
    resp = client.get_features(
        feature_service_name="{feature_service_name}",
        join_key_map=join_key_map_dyn,
        request_context_map=request_context_map_dyn,
        metadata_options=MetadataOptions.all()
    )

    logger.info(f"TectonClient response: {{resp}}")
    
    # Return the response object (which contains result.features and metadata)
    return resp
"""

    # Create a local namespace with required imports
    local_namespace = {
        'List': List,
        'Dict': Dict, 
        'Any': Any,
        'Annotated': Annotated,
        'Field': Field,
    }
    
    # Execute the function code
    exec(func_code, local_namespace)
    
    # Get the created function
    func = local_namespace['feature_service_function']
    
    # Set annotations on the function
    func.__annotations__ = annotations
    func.__annotations__['return'] = List[Dict[str, Any]]
    
    return func


def register_tecton_feature_service_as_tools(workspace: str, server: FastMCP, cluster_url: str) -> None:
    """Register tools for all Feature Services in a workspace using function-based approach.

    This function is resilient to missing Tecton configuration. If any errors
    occur while fetching feature services, they are logged and the registration
    step is skipped.
    """

    logger = logging.getLogger(__name__)
    
    ws = tecton.get_workspace(workspace)
    fs_names = ws.list_feature_services()

    try:
        for fs_name in fs_names:
            fs = ws.get_feature_service(fs_name)
            description = f"Call FeatureService {fs_name}.\nDescription: {fs.description}"
            feature_details = []

            feature_views = fs.features
            for fv in feature_views:
                for feature in fv.features:
                    feature_details.append(feature)

            if feature_details:
                description = f"{description}\nFeatures: {', '.join(feature_details)}"

            join_keys_info = extract_join_keys_map_from_feature_service(fs)
            request_context_info = extract_request_context_map_from_feature_service(fs)

            # Create function with cluster URL
            tool_func = _create_feature_service_tool_function(workspace, fs_name, join_keys_info, request_context_info, cluster_url)
            tool_name = f"{fs_name}_tool"
            
            # Add the tool to the server using the traditional approach
            server.add_tool(tool_func, name=tool_name, description=description)

            logger.info(f"Registered tool: {tool_name}")

    except Exception as e:
        logger.error(f"Error registering Feature Services: {e}")
        raise
