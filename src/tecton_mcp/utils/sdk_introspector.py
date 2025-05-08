import inspect
import tecton
import tecton.types
import tecton.aggregation_functions
from tecton_mcp.utils import _FuncOrClass
from functools import lru_cache

# Define names to exclude - can be updated here centrally
EXCLUDE_NAMES = {
    "set_tecton_spark_session", "resource_provider", "login", "logout",
    "list_workspaces", "get_workspace", "get_transformation", "get_feature_view",
    "get_feature_table", "get_entity", "get_data_source", "get_caller_identity",
    "get_feature_service", "get_current_workspace", "complete_login",
    "TectonValidationError", "StrictModel", "StrictFrozenModel",
    "MaterializedFeatureView", "MaterializationJob", "MaterializationContext",
    "MaterializationAttempt", "LoggingConfig", "Inference",
    "FeatureMetadata", "FeatureReference"
}

# Modules to inspect
MODULES_TO_INSPECT = [tecton, tecton.types, tecton.aggregation_functions]


@lru_cache()
def get_sdk_definitions():
    """Introspects Tecton SDK modules and returns definition details."""
    all_defs = set()
    details = {}

    for module in MODULES_TO_INSPECT:
        for name in dir(module):
            if name.startswith('_') or name in EXCLUDE_NAMES:
                continue

            obj = getattr(module, name)
            is_class = inspect.isclass(obj) and getattr(obj, '__module__', '').startswith('tecton')
            is_func = inspect.isfunction(obj) and getattr(obj, '__module__', '').startswith('tecton')

            if is_class or is_func:
                if name not in details:
                    all_defs.add(name)
                    try:
                        foc = _FuncOrClass(obj)
                        details[name] = {
                            "name": name,
                            "type": "Class" if is_class else "Function",
                            "module": obj.__module__,
                            "declaration": foc.callable_declaration
                        }
                    except Exception:
                        # Fallback if _FuncOrClass fails
                        details[name] = {
                            "name": name,
                            "type": "Class" if is_class else "Function",
                            "module": obj.__module__,
                            "declaration": f"# Could not retrieve declaration for {name}"
                        }
    return details, sorted(list(all_defs))

def format_sdk_definitions(details, all_defs):
    """Formats the extracted SDK definitions into a printable string."""
    output_lines = []
    output_lines.append("Found the following public classes/functions in Tecton SDK:")
    for def_name in all_defs:
        output_lines.append(f"- {def_name}")
    output_lines.append("\n" + "=" * 40 + "\n")

    for name, info in sorted(details.items()):
        item_type = info['type']
        module_name = info['module']
        output_lines.append(f"{item_type}: {name}")
        import_from = 'tecton' if hasattr(tecton, name) else module_name
        output_lines.append(f"Import from: {import_from} (defined in: {module_name})")
        output_lines.append("-" * 20)
        output_lines.append(info['declaration'])
        output_lines.append("\n" + "=" * 40 + "\n")

    return "\n".join(output_lines) 