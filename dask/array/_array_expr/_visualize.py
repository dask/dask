"""Rich-based visualization for array expressions."""

from __future__ import annotations

import io
import math
from typing import TYPE_CHECKING

from dask.utils import funcname

if TYPE_CHECKING:
    from dask.array._array_expr._expr import ArrayExpr

# Color coding using Tango palette for readability on light backgrounds
# Orange (warm) = sources, new data entering the computation
# Blue (cool) = reducers, data being reduced/consumed
_SOURCE_COLOR = "#ce5c00"  # Tango orange dark
_REDUCER_COLOR = "#3465a4"  # Tango sky blue


def _get_op_color(node) -> str | None:
    """Determine operation color based on class hierarchy and data flow."""
    from dask.array._array_expr._expr import ArrayExpr
    from dask.array._array_expr.reductions._reduction import PartialReduce
    from dask.array._array_expr.slicing._basic import Slice

    # Sources: no ArrayExpr dependencies (data enters here)
    deps = [op for op in node.operands if isinstance(op, ArrayExpr)]
    if not deps:
        return _SOURCE_COLOR

    # Reducers: PartialReduce or Slice subclasses (data shrinks here)
    if isinstance(node, (PartialReduce, Slice)):
        return _REDUCER_COLOR

    return None


def _get_op_style(node) -> str:
    """Get the rich style for an operation."""
    color = _get_op_color(node)
    if color:
        return f"bold {color}"
    return "bold"


# Operations where we prefer showing the _name prefix as the primary name
_USE_LABEL_AS_NAME = frozenset(
    {"Blockwise", "PartialReduce", "Elemwise", "Random", "SliceSlicesIntegers"}
)


def _get_op_display_name(node) -> str:
    """Get the display name for an operation.

    For generic operations like Blockwise, extracts a meaningful name from _name.
    Returns title-cased name (e.g., "Sum", "Tensordot", "Random Sample").
    """
    class_name = funcname(type(node))

    if class_name not in _USE_LABEL_AS_NAME:
        return class_name

    # Extract prefix from _name (everything before the hash)
    # e.g., "sum-aggregate-abc123" -> "Sum"
    #       "random_sample-abc123" -> "Random Sample"
    expr_name = node._name
    if "-" in expr_name:
        # Find where the hash starts (last segment that looks like a hash)
        parts = expr_name.rsplit("-", 1)
        if len(parts) == 2 and len(parts[1]) >= 8:
            # Likely a hash suffix, use the prefix
            label = parts[0]
            # Clean up underscores to spaces
            label = label.replace("_", " ")
            # Remove common suffixes that don't add info
            for suffix in ["-aggregate", "-partial"]:
                if suffix in label:
                    label = label.replace(suffix, "")
            # Clean up any remaining hyphens
            label = label.replace("-", " ").strip()
            # Title case
            return label.title()

    return class_name


def _summarize_chunks(chunks: tuple) -> str:
    """Summarize chunks compactly, e.g. (100,100) -> '100×100'."""
    if not chunks:
        return ""

    # Get representative chunk sizes (first chunk of each dim)
    sizes = [c[0] if c else 0 for c in chunks]
    return "×".join(str(s) for s in sizes)


def _format_shape(shape: tuple) -> str:
    """Format shape tuple compactly."""
    if not shape:
        return "()"
    return f"({', '.join(str(s) for s in shape)})"


def _walk_expr_with_prefix(expr: ArrayExpr, prefix: str = "", is_last: bool = True):
    """
    Walk expression tree depth-first, yielding (expr, display_prefix) pairs.

    Handles proper tree drawing characters for siblings.
    """
    yield expr, prefix

    # Get array expression children
    children = [op for op in expr.operands if hasattr(op, "chunks")]

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

        yield from _walk_expr_with_prefix(child, child_prefix + branch, is_last_child)


def _get_nbytes(node) -> float:
    """Get the number of bytes for an expression, or NaN if unknown."""
    try:
        shape = node.shape
        if any(math.isnan(s) for s in shape):
            return math.nan
        return math.prod(shape) * node.dtype.itemsize
    except Exception:
        return math.nan


def _format_nbytes(nbytes: float) -> str:
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

            console = Console(
                file=io.StringIO(),
                force_terminal=False,
                force_jupyter=False,
                record=True,
            )
            console.print(self._table)
            self._html_cache = console.export_html(
                inline_styles=True, code_format="<pre>{code}</pre>"
            )
        return self._html_cache

    def __repr__(self):
        """Terminal display."""
        if self._text_cache is None:
            from rich.console import Console

            console = Console(
                file=io.StringIO(), force_terminal=True, force_jupyter=False
            )
            console.print(self._table)
            self._text_cache = console.file.getvalue().rstrip()
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


def expr_table(expr: ArrayExpr, color: bool = True) -> ExprTable:
    """
    Display expression tree as a lightweight aligned table.

    Parameters
    ----------
    expr : ArrayExpr
        The expression to visualize
    color : bool
        Whether to color-code operations by type

    Returns
    -------
    ExprTable
        A displayable table object (works in Jupyter and terminal)
    """
    from rich.table import Table
    from rich.text import Text

    table = Table(
        show_header=True,
        header_style="dim",
        box=None,
        padding=(0, 2),
        collapse_padding=True,
    )

    # Add columns
    table.add_column("Operation", no_wrap=True)
    table.add_column("Shape", justify="right", no_wrap=True)
    table.add_column("Bytes", justify="right", style="dim", no_wrap=True)
    table.add_column("Chunks", justify="right", style="dim", no_wrap=True)

    # First pass: collect nodes and compute bytes for highlighting
    nodes_and_prefixes = list(_walk_expr_with_prefix(expr))
    node_bytes = [_get_nbytes(node) for node, _ in nodes_and_prefixes]
    max_bytes = max((b for b in node_bytes if not math.isnan(b)), default=0)

    # Walk tree and add rows
    for (node, prefix), nbytes in zip(nodes_and_prefixes, node_bytes):
        display_name = _get_op_display_name(node)

        # Calculate size ratio for background highlighting
        size_ratio = (
            nbytes / max_bytes if max_bytes > 0 and not math.isnan(nbytes) else 0
        )

        # Apply subtle background highlight for large arrays (gradient based on size)
        # Uses light yellow tint: white (255,255,255) -> light yellow (255,255,220)
        if color and size_ratio > 0.1:
            # Scale: 0.1 -> 250, 1.0 -> 220 (subtle yellow tint)
            yellow_component = int(255 - size_ratio * 35)
            row_style = f"on rgb(255,255,{yellow_component})"
        else:
            row_style = None

        if color:
            op_text = Text()
            op_text.append(prefix, style="dim")
            op_text.append(display_name, style=_get_op_style(node))
        else:
            op_text = f"{prefix}{display_name}"

        row = [
            op_text,
            _format_shape(node.shape),
            _format_nbytes(nbytes),
            _summarize_chunks(node.chunks),
        ]
        table.add_row(*row, style=row_style)

    return ExprTable(table)
