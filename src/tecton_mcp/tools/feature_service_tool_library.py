"""Utility to register Tecton Feature Services as FastMCP tools."""

import logging
from typing import Dict, Any, List, Callable

import tecton
from tecton.types import ( # Import Tecton data types
    Int64, Int32, Float64, Float32, String, Bool, Array, Struct, Map, Timestamp, SdkDataType
)
from mcp.server.fastmcp import FastMCP, Context


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

def extract_join_keys_map_from_feature_service(fs):
    join_keys_info = {}
    if fs.features:
        for ref in fs.features:
            fv = ref.feature_definition
            override_map = ref.override_join_keys or {}

            if hasattr(fv, 'entities') and fv.entities:
                for entity_obj in fv.entities:
                    if entity_obj._spec and entity_obj._spec.join_keys:
                        for column_id_obj in entity_obj._spec.join_keys:
                            key_name = column_id_obj.name
                            key_type_obj = column_id_obj.dtype
                            final_key_name = override_map.get(key_name, key_name)
                            join_keys_info[final_key_name] = key_type_obj
    return join_keys_info


def extract_request_context_map_from_feature_service(fs):
    request_context_info = {}
    if fs._request_context and fs._request_context.schema:
        for field_name, tecton_type in fs._request_context.schema.items():
            request_context_info[field_name] = tecton_type
    return request_context_info

def _create_feature_service_tool(
    workspace: str, feature_service_name: str
) -> Callable[..., List[Dict[str, Any]]]:
    """Return a callable tool for the given Feature Service, with dynamic parameters declared via annotations."""
    ws = tecton.get_workspace(workspace)
    fs = ws.get_feature_service(feature_service_name)

    join_keys_info = extract_join_keys_map_from_feature_service(fs)
    request_context_info = extract_request_context_map_from_feature_service(fs)

    annotations: Dict[str, Any] = {}
    param_names_ordered = [] # To maintain a somewhat sensible order for generated help, if any

    for name, tecton_dtype in join_keys_info.items():
        annotations[name] = _map_tecton_type_to_python_type(tecton_dtype)
        if name not in param_names_ordered:
            param_names_ordered.append(name)
    
    for name, tecton_dtype in request_context_info.items():
        annotations[name] = _map_tecton_type_to_python_type(tecton_dtype)
        if name not in param_names_ordered:
            param_names_ordered.append(name)

    annotations["ctx"] = Context
    annotations["return"] = List[Dict[str, Any]]

    def feature_service_tool_dynamic(**kwargs_received) -> List[Dict[str, Any]]:
        # Extract ctx; it's explicitly in annotations, so FastMCP should ensure it's passed if required.
        actual_ctx = kwargs_received.pop("ctx", None)
        if actual_ctx is None:
            # This check might be redundant if FastMCP enforces params based on annotations
            raise TypeError(f"Tool for {feature_service_name}: Missing required argument 'ctx'.")

        join_key_map_dyn = {}
        request_context_map_dyn = {}

        # Populate maps from remaining kwargs based on known keys
        for key_name in join_keys_info.keys():
            if key_name in kwargs_received:
                join_key_map_dyn[key_name] = kwargs_received[key_name]
            else:
                # Assuming FastMCP will handle missing required parameters based on annotations.
                # If not, a more robust check/error would be needed here if a join_key is mandatory.
                pass 
        
        for key_name in request_context_info.keys():
            if key_name in kwargs_received:
                request_context_map_dyn[key_name] = kwargs_received[key_name]
            else:
                # Similar assumption for request_context_keys
                pass

        from tecton_client import TectonClient # Local import

        client = TectonClient(
            url="https://explore.tecton.ai/", # Consider making configurable
            api_key="my-api-key",           # Consider making configurable or using Tecton config
            default_workspace_name=workspace, # Captured from outer scope
        )
        resp = client.get_features(
            feature_service_name=feature_service_name, # Captured from outer scope
            join_key_map=join_key_map_dyn,
            request_context_map=request_context_map_dyn,
        )
        return resp.result.features

    feature_service_tool_dynamic.__annotations__ = annotations
    # __name__ and __doc__ will be effectively set by server.add_tool arguments
    # feature_service_tool_dynamic.__name__ = f"tool_for_{feature_service_name.replace('.', '_')}" 

    return feature_service_tool_dynamic


def register_tecton_feature_service_as_tools(workspace: str, server: FastMCP) -> None:
    """Register tools for all Feature Services in a workspace.

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

            tool_func = _create_feature_service_tool(workspace, fs_name)
            tool_name = f"{fs_name}_tool"
            server.add_tool(tool_func, name=tool_name, description=description)

            logger.info(f"Registered tool: {tool_name}")

    except Exception as e:
        logger.error(f"Error registering Feature Services: {e}")
        raise
