import enum
import inspect
from typing import Any, Union, get_type_hints

from tecton.framework import base_tecton_object

from tecton_mcp.utils import _FuncOrClass

_FCO_NAME_MAP = {
    "BatchFeatureView": "batch_feature_view",
    "StreamFeatureView": "stream_feature_view",
    "RealtimeFeatureView": "realtime_feature_view",
    "PandasBatchConfig": "pandas_batch_config",
    "PyArrowBatchConfig": "pyarrow_batch_config",
    "SparkBatchConfig": "spark_batch_config",
    "SparkStreamConfig": "spark_stream_config",
}


class APIGraphBuilder:
    def __init__(self):
        objects = list(_FuncOrClass.from_expressions(["tecton"]))
        scope = {x.name: x for x in objects}
        deps = self._find_tecton_dependencies(scope)
        res = {}
        for key, value in scope.items():
            res[key] = {"declaration": value.callable_declaration, "deps": deps[key]}
        self.graph = res

    def build_code(self, names: list[str]) -> str:
        code = {}
        for name in names:
            self._build_code(name, code)
            self._build_code(_FCO_NAME_MAP.get(name, name), code)
        return "\n\n".join(code.values())

    def _build_code(self, name: str, code: dict[str, Any]):
        if name in code:
            return
        for dep in self.graph[name]["deps"]:
            self._build_code(dep, code)
        code[name] = self.graph[name]["declaration"]

    def _find_tecton_annotations(self, cls):
        try:
            if inspect.isclass(cls):
                annotations = get_type_hints(cls.__init__)
            elif inspect.isfunction(cls):
                annotations = get_type_hints(cls)
            else:
                raise ValueError("cls must be a class or a function")
            for name, param in annotations.items():
                if is_tecton_type(param):
                    yield param
                # else check if param is union, check if any of the union is a tecton type
                elif hasattr(param, "__origin__") and param.__origin__ == Union:
                    for arg in param.__args__:
                        if is_tecton_type(arg):
                            yield arg
        except Exception:
            return None

    def _find_tecton_mentions(self, obj):
        # check obj.obj is a class type use inspect
        if inspect.isclass(obj):
            if not issubclass(obj, enum.Enum):
                yield from self._find_tecton_annotations(obj)
        elif inspect.isfunction(obj):
            yield from self._find_tecton_annotations(obj)

    def _find_tecton_dependencies(self, scope):
        res = {}
        for key, obj in scope.items():
            res[key] = list(
                x.__name__
                for x in self._find_tecton_mentions(obj.obj)
                if x.__name__ in scope
            )
        return res


def is_tecton_type(tp):
    try:
        tp_str = tp.__module__
        return tp_str.startswith("tecton.") or tp_str == "tecton"
    except Exception:
        return False


def is_tecton_object(x):
    try:
        return isinstance(x, base_tecton_object.BaseTectonObject)
    except Exception:
        return False


def extract_tecton_objects(path: str):
    with open(path, "r") as f:
        code = f.read()
    return dict(_parse(code))


def _parse(code):
    _keys = set(locals().keys())
    exec(code)
    _keys2 = set(locals().keys())
    for x in _keys2 - _keys:
        if x in ["_keys", "code"]:
            continue
        # check x is a type
        v = locals()[x]
        if is_tecton_object(v):
            yield x, v
