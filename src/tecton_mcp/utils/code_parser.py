"""
Utility for parsing code to extract declarations and docstrings.
"""

import ast
import inspect
from typing import Any
from textwrap import dedent


def get_declaration(obj: Any, entrypoint_only: bool) -> str:
    """
    Get the function or class declaration with docstring.

    Args:
        obj: The object
        entrypoint_only: Whether to get only the entrypoint of the object (either the class __init__ or function)

    Returns:
        str: The object declaration with docstring
    """
    if inspect.isclass(obj) or inspect.isfunction(obj):
        src = dedent(inspect.getsource(obj))
        tree = ast.parse(src)
        tree = _traverse(
            tree, init_only=inspect.isclass(obj) if entrypoint_only else False
        )
        return ast.unparse(tree)
    raise ValueError(f"{obj} is not a class or function")


def _traverse(node: Any, init_only: bool) -> Any:
    """
    Traverse an AST node and extract declarations.
    
    Args:
        node: The AST node
        init_only: Whether to extract only __init__ method for classes
        
    Returns:
        The modified AST node
    """
    if isinstance(node, ast.FunctionDef):
        name = node.name
        if name == "__init__" or (not init_only and not name.startswith("_")):
            nb = node.body
            if (
                len(nb) > 0
                and isinstance(nb[0], ast.Expr)
                and isinstance(nb[0].value, ast.Constant)
            ):
                nb = [nb[0], ast.Pass()]
            else:
                nb = [ast.Pass()]
            node.body = nb
            return node
        else:
            return None
    elif hasattr(node, "body"):
        body = []
        for x in node.body:
            xx = _traverse(x, init_only=init_only)
            if xx is not None:
                body.append(xx)
        node.body = body
        return node
    else:
        return node 