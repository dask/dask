"""Rich-based visualization for array expressions."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

from dask._rich_table import (
    REDUCER_COLOR,
    SOURCE_COLOR,
    ExprTable,
    format_bytes,
    get_op_display_name,
    get_op_style,
    is_expr,
    walk_expr_with_prefix,
)

if TYPE_CHECKING:
    from dask.array._array_expr._expr import ArrayExpr


def _get_op_color(node) -> str | None:
    """Determine operation color based on class hierarchy and data flow."""
    from dask.array._array_expr._expr import ArrayExpr
    from dask.array._array_expr.reductions._reduction import PartialReduce
    from dask.array._array_expr.slicing._basic import Slice

    # Sources: no ArrayExpr dependencies (data enters here)
    deps = [op for op in node.operands if isinstance(op, ArrayExpr)]
    if not deps:
        return SOURCE_COLOR

    # Reducers: PartialReduce or Slice subclasses (data shrinks here)
    if isinstance(node, (PartialReduce, Slice)):
        return REDUCER_COLOR

    return None


# Operations where we prefer showing the _name prefix as the primary name
_USE_LABEL_AS_NAME = frozenset(
    {"Blockwise", "PartialReduce", "Elemwise", "Random", "SliceSlicesIntegers"}
)


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


def _get_nbytes(node) -> float:
    """Get the number of bytes for an expression, or NaN if unknown."""
    try:
        shape = node.shape
        if any(math.isnan(s) for s in shape):
            return math.nan
        return math.prod(shape) * node.dtype.itemsize
    except Exception:
        return math.nan


def _is_array_expr(op):
    """Check if operand is an ArrayExpr for tree walking."""
    return hasattr(op, "chunks") and is_expr(op)


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
    nodes_and_prefixes = list(walk_expr_with_prefix(expr, is_expr_child=_is_array_expr))
    node_bytes = [_get_nbytes(node) for node, _ in nodes_and_prefixes]
    max_bytes = max((b for b in node_bytes if not math.isnan(b)), default=0)

    # Walk tree and add rows
    for (node, prefix), nbytes in zip(nodes_and_prefixes, node_bytes):
        display_name = get_op_display_name(node, _USE_LABEL_AS_NAME)

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
            op_text.append(display_name, style=get_op_style(_get_op_color(node)))
        else:
            op_text = f"{prefix}{display_name}"

        row = [
            op_text,
            _format_shape(node.shape),
            format_bytes(nbytes),
            _summarize_chunks(node.chunks),
        ]
        table.add_row(*row, style=row_style)

    return ExprTable(table)
