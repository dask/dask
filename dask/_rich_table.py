"""Shared visualization utilities for expression trees."""

from __future__ import annotations

import io
import math
from typing import TYPE_CHECKING

from dask.utils import funcname

if TYPE_CHECKING:
    from dask._expr import Expr

# Color coding using Tango palette for readability
# Orange (warm) = sources, new data entering the computation
# Blue (cool) = reducers, data being reduced/consumed
SOURCE_COLOR = "#ce5c00"  # Tango orange dark
REDUCER_COLOR = "#3465a4"  # Tango sky blue


def get_op_style(color: str | None) -> str:
    """Get the rich style for an operation given its color."""
    if color:
        return f"bold {color}"
    return "bold"


def format_bytes(nbytes: float) -> str:
    """Format bytes with 2 significant figures."""
    if math.isnan(nbytes):
        return "?"

    for unit, threshold in [
        ("PiB", 2**50),
        ("TiB", 2**40),
        ("GiB", 2**30),
        ("MiB", 2**20),
        ("kiB", 2**10),
    ]:
        if nbytes >= threshold:
            value = nbytes / threshold
            if value >= 10:
                return f"{value:.0f} {unit}"
            else:
                return f"{value:.1f} {unit}"
    return f"{int(nbytes)} B"


class ExprTable:
    """Wrapper for rich Table with Jupyter and terminal display support."""

    def __init__(self, table):
        self._table = table
        self._html_cache = None
        self._text_cache = None

    def _repr_html_(self):
        """Jupyter notebook display."""
        if self._html_cache is None:
            from rich.console import Console

            # Grab width from real console, use StringIO to suppress stdout
            width = Console().width
            console = Console(file=io.StringIO(), record=True, width=width)
            console.print(self._table)
            self._html_cache = console.export_html(
                inline_styles=True, code_format="<pre>{code}</pre>"
            )
        return self._html_cache

    def __repr__(self):
        """Terminal display."""
        if self._text_cache is None:
            from rich.console import Console

            console = Console(force_terminal=True)
            with console.capture() as capture:
                console.print(self._table)
            self._text_cache = capture.get().rstrip()
        return self._text_cache

    def __str__(self):
        return self.__repr__()

    def _repr_mimebundle_(self, **kwargs):
        """Provide explicit MIME bundle for Jupyter."""
        return {"text/html": self._repr_html_()}

    def print(self):
        """Print to the current console."""
        from rich.console import Console

        Console().print(self._table)


def walk_expr_with_prefix(
    expr: Expr,
    prefix: str = "",
    is_last: bool = True,
    is_expr_child=None,
):
    """
    Walk expression tree depth-first, yielding (expr, display_prefix) pairs.

    Handles proper tree drawing characters for siblings.

    Parameters
    ----------
    expr : Expr
        The expression to walk
    prefix : str
        Current prefix string for tree drawing
    is_last : bool
        Whether this node is the last child of its parent
    is_expr_child : callable, optional
        Function to determine if an operand is an Expr child to recurse into.
        If None, uses hasattr(op, 'dependencies').
    """
    yield expr, prefix

    if is_expr_child is None:
        is_expr_child = lambda op: hasattr(op, "dependencies") and callable(
            op.dependencies
        )

    children = [op for op in expr.operands if is_expr_child(op)]

    for i, child in enumerate(children):
        is_last_child = i == len(children) - 1

        # Build the prefix for this child
        if prefix == "":
            child_prefix = ""
        else:
            # Continue vertical lines for non-last ancestors
            child_prefix = prefix[:-2] + ("  " if is_last else "│ ")

        # Add the branch character
        branch = "└ " if is_last_child else "├ "

        yield from walk_expr_with_prefix(
            child, child_prefix + branch, is_last_child, is_expr_child
        )


def is_expr(op) -> bool:
    """Check if operand is a Dask expression (has operands attribute)."""
    return hasattr(op, "operands")


def compute_row_emphasis(values: list[float], threshold: float = 0.5) -> list[bool]:
    """Compute which rows should be emphasized based on relative values.

    Parameters
    ----------
    values : list of float
        Numeric values for each row (e.g., bytes, partition counts)
    threshold : float
        Ratio threshold (0-1). Rows with value > threshold * max are emphasized.

    Returns
    -------
    list of bool
        True for rows that should be emphasized (normal brightness),
        False for rows that should be de-emphasized (dim).
    """
    # Filter out NaN values for computing max
    valid_values = [v for v in values if not math.isnan(v)]
    if not valid_values:
        return [True] * len(values)

    max_value = max(valid_values)
    if max_value <= 0:
        return [True] * len(values)

    return [not math.isnan(v) and v > threshold * max_value for v in values]


def get_op_display_name(node, use_label_for: frozenset | None = None) -> str:
    """Get the display name for an operation.

    For generic operations like Blockwise, extracts a meaningful name from _name.
    Returns title-cased name (e.g., "Sum", "Tensordot", "Random Sample").

    Parameters
    ----------
    node : Expr
        The expression node
    use_label_for : frozenset, optional
        Set of class names where we extract name from _name instead of class name
    """
    class_name = funcname(type(node))

    if use_label_for and class_name not in use_label_for:
        return class_name

    if use_label_for is None:
        return class_name

    # Extract prefix from _name (everything before the hash)
    expr_name = node._name
    if "-" in expr_name:
        parts = expr_name.rsplit("-", 1)
        if len(parts) == 2 and len(parts[1]) >= 8:
            label = parts[0]
            label = label.replace("_", " ")
            for suffix in ["-aggregate", "-partial"]:
                if suffix in label:
                    label = label.replace(suffix, "")
            label = label.replace("-", " ").strip()
            return label.title()

    return class_name
