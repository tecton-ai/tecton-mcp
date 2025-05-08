"""
Utility for code function and class introspection.
"""

import importlib
import inspect
from functools import lru_cache
from typing import Any, Iterable, List, Tuple

from .code_parser import get_declaration


class _FuncOrClass:
    """
    Represents a function or class object with utilities for introspection.
    This class is used to extract declarations and documentation of functions and classes.
    """
    
    def __init__(self, obj: Any):
        """
        Initialize a _FuncOrClass instance.
        
        Args:
            obj: The function or class object to introspect
        """
        self.obj = obj
        self.declaration = get_declaration(obj, entrypoint_only=False)
        self.callable_declaration = get_declaration(obj, entrypoint_only=True)
        self.name = obj.__name__
        self.doc = (obj.__doc__ or "").split("Args:")[0].strip()

    @property
    def tool_description(self) -> str:
        """Get a description of this object suitable for use as a tool description."""
        return (
            f"Get the declaration of {self.name}. "
            f"The purpose of {self.name}:\n{self.doc}"
        )

    @property
    def tool_name(self) -> str:
        """Get a name for this object suitable for use as a tool name."""
        return f"get_declaration_of_{self.name}"

    @staticmethod
    def from_expressions(expressions: List[str]) -> Iterable["_FuncOrClass"]:
        """
        Create _FuncOrClass instances from module expressions.
        
        Args:
            expressions: List of module paths or module.attribute paths
            
        Returns:
            Iterable of _FuncOrClass instances for functions and classes in the modules
        """
        for expr in expressions:
            try:
                module = importlib.import_module(expr)
                names = [name for name in dir(module) if not name.startswith("_")]
            except Exception:
                parts = expr.rsplit(".", 1)
                module = importlib.import_module(parts[0])
                names = [parts[1]]
            objs = [getattr(module, name) for name in names]
            yield from [
                _FuncOrClass(x)
                for x in objs
                if inspect.isclass(x) or inspect.isfunction(x)
            ] 