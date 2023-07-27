from __future__ import annotations

import functools
import numbers
import operator
import os
from collections import defaultdict
from collections.abc import Generator, Mapping

import dask
import numpy as np
import pandas as pd
import toolz
from dask.base import normalize_token
from dask.core import flatten, ishashable
from dask.dataframe import methods
from dask.dataframe.core import (
    _get_divisions_map_partitions,
    _get_meta_map_partitions,
    apply_and_enforce,
    is_dataframe_like,
    is_index_like,
    is_series_like,
    make_meta,
)
from dask.dataframe.dispatch import meta_nonempty
from dask.dataframe.utils import clear_known_categories, drop_by_shallow_copy
from dask.utils import M, apply, funcname, import_required, is_arraylike
from tlz import merge_sorted, unique

from dask_expr._util import _tokenize_deterministic

replacement_rules = []

no_default = "__no_default__"


class Expr:
    """Primary class for all Expressions

    This mostly includes Dask protocols and various Pandas-like method
    definitions to make us look more like a DataFrame.
    """

    _parameters = []
    _defaults = {}

    def __init__(self, *args, **kwargs):
        operands = list(args)
        for parameter in type(self)._parameters[len(operands) :]:
            try:
                operands.append(kwargs.pop(parameter))
            except KeyError:
                operands.append(type(self)._defaults[parameter])
        assert not kwargs
        self.operands = operands
        if self._required_attribute:
            dep = next(iter(self.dependencies()))._meta
            if not hasattr(dep, self._required_attribute):
                # Raise a ValueError instead of AttributeError to
                # avoid infinite recursion
                raise ValueError(f"{dep} has no attribute {self._required_attribute}")

    @property
    def _required_attribute(self) -> str:
        # Specify if the first `dependency` must support
        # a specific attribute for valid behavior.
        return None

    @functools.cached_property
    def ndim(self):
        meta = self._meta
        try:
            return meta.ndim
        except AttributeError:
            return 0

    def __str__(self):
        s = ", ".join(
            str(param) + "=" + str(operand)
            for param, operand in zip(self._parameters, self.operands)
            if operand != self._defaults.get(param)
        )
        return f"{type(self).__name__}({s})"

    def __repr__(self):
        return str(self)

    def _tree_repr_lines(self, indent=0, recursive=True):
        header = funcname(type(self)) + ":"
        lines = []
        for i, op in enumerate(self.operands):
            if isinstance(op, Expr):
                if recursive:
                    lines.extend(op._tree_repr_lines(2))
            else:
                try:
                    param = self._parameters[i]
                    default = self._defaults[param]
                except (IndexError, KeyError):
                    param = self._parameters[i] if i < len(self._parameters) else ""
                    default = "--no-default--"

                if isinstance(op, pd.core.base.PandasObject):
                    op = "<pandas>"
                elif is_dataframe_like(op):
                    op = "<dataframe>"
                elif is_index_like(op):
                    op = "<index>"
                elif is_series_like(op):
                    op = "<series>"
                elif is_arraylike(op):
                    op = "<array>"

                if repr(op) != repr(default):
                    if param:
                        header += f" {param}={repr(op)}"
                    else:
                        header += repr(op)
        lines = [header] + lines
        lines = [" " * indent + line for line in lines]

        return lines

    def tree_repr(self):
        return os.linesep.join(self._tree_repr_lines())

    def pprint(self):
        for line in self._tree_repr_lines():
            print(line)

    def __hash__(self):
        return hash(self._name)

    def __reduce__(self):
        return type(self), tuple(self.operands)

    def _depth(self):
        """Depth of the expression tree

        Returns
        -------
        depth: int
        """
        if not self.dependencies():
            return 1
        else:
            return max(expr._depth() for expr in self.dependencies()) + 1

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError as err:
            if key == "_meta":
                # Avoid a recursive loop if/when `self._meta`
                # produces an `AttributeError`
                raise RuntimeError(
                    f"Failed to generate metadata for {self}. "
                    "This operation may not be supported by the current backend."
                )

            # Allow operands to be accessed as attributes
            # as long as the keys are not already reserved
            # by existing methods/properties
            _parameters = type(self)._parameters
            if key in _parameters:
                idx = _parameters.index(key)
                return self.operands[idx]
            if is_dataframe_like(self._meta) and key in self._meta.columns:
                return self[key]

            link = "https://github.com/dask-contrib/dask-expr/blob/main/README.md#api-coverage"
            raise AttributeError(
                f"{err}\n\n"
                "This often means that you are attempting to use an unsupported "
                f"API function. Current API coverage is documented here: {link}."
            )

    def operand(self, key):
        # Access an operand unambiguously
        # (e.g. if the key is reserved by a method/property)
        return self.operands[type(self)._parameters.index(key)]

    def dependencies(self):
        # Dependencies are `Expr` operands only
        return [operand for operand in self.operands if isinstance(operand, Expr)]

    def _task(self, index: int):
        """The task for the i'th partition

        Parameters
        ----------
        index:
            The index of the partition of this dataframe

        Examples
        --------
        >>> class Add(Expr):
        ...     def _task(self, i):
        ...         return (operator.add, (self.left._name, i), (self.right._name, i))

        Returns
        -------
        task:
            The Dask task to compute this partition

        See Also
        --------
        Expr._layer
        """
        raise NotImplementedError(
            "Expressions should define either _layer (full dictionary) or _task"
            " (single task).  This expression type defines neither"
        )

    def _layer(self) -> dict:
        """The graph layer added by this expression

        Examples
        --------
        >>> class Add(Expr):
        ...     def _layer(self):
        ...         return {
        ...             (self._name, i): (operator.add, (self.left._name, i), (self.right._name, i))
        ...             for i in range(self.npartitions)
        ...         }

        Returns
        -------
        layer: dict
            The Dask task graph added by this expression

        See Also
        --------
        Expr._task
        Expr.__dask_graph__
        """

        return {(self._name, i): self._task(i) for i in range(self.npartitions)}

    def simplify(self):
        """Simplify expression

        This leverages the ``._simplify_down`` method defined on each class

        Returns
        -------
        expr:
            output expression
        changed:
            whether or not any change occured
        """
        expr = self

        while True:
            _continue = False

            # Simplify this node
            out = expr._simplify_down()
            if out is None:
                out = expr
            if not isinstance(out, Expr):
                return out
            if out._name != expr._name:
                expr = out
                continue

            # Allow children to simplify their parents
            for child in expr.dependencies():
                out = child._simplify_up(expr)
                if out is None:
                    out = expr
                if not isinstance(out, Expr):
                    return out
                if out is not expr and out._name != expr._name:
                    expr = out
                    _continue = True
                    break

            if _continue:
                continue

            # Simplify all of the children
            new_operands = []
            changed = False
            for operand in expr.operands:
                if isinstance(operand, Expr):
                    new = operand.simplify()
                    if new._name != operand._name:
                        changed = True
                else:
                    new = operand
                new_operands.append(new)

            if changed:
                expr = type(expr)(*new_operands)
                continue
            else:
                break

        return expr

    def _simplify_down(self):
        return

    def _simplify_up(self, parent):
        return

    def lower_once(self):
        expr = self

        # Lower this node
        out = expr._lower()
        if out is None:
            out = expr
        if not isinstance(out, Expr):
            return out

        # Lower all children
        new_operands = []
        changed = False
        for operand in out.operands:
            if isinstance(operand, Expr):
                new = operand.lower_once()
                if new._name != operand._name:
                    changed = True
            else:
                new = operand
            new_operands.append(new)

        if changed:
            out = type(out)(*new_operands)

        return out

    def _lower(self):
        return

    def combine_similar(
        self, root: Expr | None = None, _cache: dict | None = None
    ) -> Expr:
        """Combine similar expression nodes using global information

        This leverages the ``._combine_similar`` method defined
        on each class. The global expression-tree traversal will
        change IO leaves first, and finish with the root expression.
        The primary purpose of this method is to allow column
        projections to be "pushed back up" the expression graph
        in the case that simlar IO & Blockwise operations can
        be captured by the same operations.

        Parameters
        ----------
        root:
            The root node of the global expression graph. If not
            specified, the root is assumed to be ``self``.
        _cache:
            Optional dictionary to use for caching.

        Returns
        -------
        expr:
            output expression
        """
        expr = self
        update_root = root is None
        root = root or self

        if _cache is None:
            _cache = {}
        elif (self._name, root._name) in _cache:
            return _cache[(self._name, root._name)]

        while True:
            changed = False

            # Call combine_similar on each dependency
            new_operands = []
            changed_dependency = False
            for operand in expr.operands:
                if isinstance(operand, Expr):
                    new = operand.combine_similar(root=root, _cache=_cache)
                    if new._name != operand._name:
                        changed_dependency = True
                else:
                    new = operand
                new_operands.append(new)

            if changed_dependency:
                expr = type(expr)(*new_operands)
                changed = True
                if update_root:
                    root = expr
                continue

            # Execute "_combine_similar" on expr
            out = expr._combine_similar(root)
            if out is None:
                out = expr
            if not isinstance(out, Expr):
                _cache[(self._name, root._name)] = out
                return out
            if out._name != expr._name:
                changed = True
                expr = out
                if update_root:
                    root = expr

            if not changed:
                break

        _cache[(self._name, root._name)] = expr
        return expr

    def _combine_similar(self, root: Expr):
        return

    def optimize(self, **kwargs):
        return optimize(self, **kwargs)

    @property
    def index(self):
        return Index(self)

    @property
    def size(self):
        return Size(self)

    @property
    def nbytes(self):
        return NBytes(self)

    def __getitem__(self, other):
        if isinstance(other, Expr):
            return Filter(self, other)  # df[df.x > 1]
        else:
            return Projection(self, other)  # df[["a", "b", "c"]]

    def __add__(self, other):
        return Add(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __sub__(self, other):
        return Sub(self, other)

    def __rsub__(self, other):
        return Sub(other, self)

    def __mul__(self, other):
        return Mul(self, other)

    def __rmul__(self, other):
        return Mul(other, self)

    def __truediv__(self, other):
        return Div(self, other)

    def __rtruediv__(self, other):
        return Div(other, self)

    def __lt__(self, other):
        return LT(self, other)

    def __rlt__(self, other):
        return LT(other, self)

    def __gt__(self, other):
        return GT(self, other)

    def __rgt__(self, other):
        return GT(other, self)

    def __le__(self, other):
        return LE(self, other)

    def __rle__(self, other):
        return LE(other, self)

    def __ge__(self, other):
        return GE(self, other)

    def __rge__(self, other):
        return GE(other, self)

    def __eq__(self, other):
        return EQ(self, other)

    def __ne__(self, other):
        return NE(self, other)

    def __and__(self, other):
        return And(self, other)

    def __rand__(self, other):
        return And(other, self)

    def __or__(self, other):
        return Or(self, other)

    def __ror__(self, other):
        return Or(other, self)

    def __xor__(self, other):
        return XOr(self, other)

    def __rxor__(self, other):
        return XOr(other, self)

    def __invert__(self):
        return Invert(self)

    def __neg__(self):
        return Neg(self)

    def __pos__(self):
        return Pos(self)

    def sum(self, skipna=True, numeric_only=False, min_count=0):
        return Sum(self, skipna, numeric_only, min_count)

    def prod(self, skipna=True, numeric_only=False, min_count=0):
        return Prod(self, skipna, numeric_only, min_count)

    def var(self, axis=0, skipna=True, ddof=1, numeric_only=False):
        if axis == 0:
            return Var(self, skipna, ddof, numeric_only)
        elif axis == 1:
            return VarColumns(self, skipna, ddof, numeric_only)
        else:
            raise ValueError(f"axis={axis} not supported. Please specify 0 or 1")

    def std(self, axis=0, skipna=True, ddof=1, numeric_only=False):
        return Sqrt(self.var(axis, skipna, ddof, numeric_only))

    def mean(self, skipna=True, numeric_only=False, min_count=0):
        return Mean(self, skipna=skipna, numeric_only=numeric_only)

    def max(self, skipna=True, numeric_only=False, min_count=0):
        return Max(self, skipna, numeric_only, min_count)

    def any(self, skipna=True):
        return Any(self, skipna=skipna)

    def all(self, skipna=True):
        return All(self, skipna=skipna)

    def idxmin(self, skipna=True, numeric_only=False):
        return IdxMin(self, skipna=skipna, numeric_only=numeric_only)

    def idxmax(self, skipna=True, numeric_only=False):
        return IdxMax(self, skipna=skipna, numeric_only=numeric_only)

    def mode(self, dropna=True):
        return Mode(self, dropna=dropna)

    def min(self, skipna=True, numeric_only=False, min_count=0):
        return Min(self, skipna, numeric_only, min_count)

    def count(self, numeric_only=False):
        return Count(self, numeric_only)

    def abs(self):
        return Abs(self)

    def astype(self, dtypes):
        return AsType(self, dtypes)

    def clip(self, lower=None, upper=None):
        return Clip(self, lower=lower, upper=upper)

    def combine_first(self, other):
        return CombineFirst(self, other=other)

    def to_timestamp(self, freq=None, how="start"):
        return ToTimestamp(self, freq=freq, how=how)

    def isna(self):
        return IsNa(self)

    def round(self, decimals=0):
        return Round(self, decimals=decimals)

    def apply(self, function, *args, **kwargs):
        return Apply(self, function, args, kwargs)

    def replace(self, to_replace=None, value=no_default, regex=False):
        return Replace(self, to_replace=to_replace, value=value, regex=regex)

    def fillna(self, value=None):
        return Fillna(self, value=value)

    def rename_axis(
        self, mapper=no_default, index=no_default, columns=no_default, axis=0
    ):
        return RenameAxis(self, mapper=mapper, index=index, columns=columns, axis=axis)

    def align(self, other, join="outer", fill_value=None):
        from dask_expr._collection import new_collection
        from dask_expr._repartition import Repartition

        if are_co_aligned(self, other):
            left = self

        else:
            dfs = [self, other]
            if not all(df.known_divisions for df in dfs):
                raise ValueError(
                    "Not all divisions are known, can't align "
                    "partitions. Please use `set_index` "
                    "to set the index."
                )

            divisions = list(unique(merge_sorted(*[df.divisions for df in dfs])))
            if len(divisions) == 1:  # single value for index
                divisions = (divisions[0], divisions[0])

            left = Repartition(self, new_divisions=divisions, force=True)
            other = Repartition(other, new_divisions=divisions, force=True)
        aligned = _Align(left, other, join=join, fill_value=fill_value)

        return new_collection(AlignGetitem(aligned, position=0)), new_collection(
            AlignGetitem(aligned, position=1)
        )

    def nunique_approx(self):
        return NuniqueApprox(self, b=16)

    @functools.cached_property
    def divisions(self):
        return tuple(self._divisions())

    def _divisions(self):
        raise NotImplementedError()

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    @property
    def npartitions(self):
        if "npartitions" in self._parameters:
            idx = self._parameters.index("npartitions")
            return self.operands[idx]
        else:
            return len(self.divisions) - 1

    @functools.cached_property
    def _name(self):
        return (
            funcname(type(self)).lower() + "-" + _tokenize_deterministic(*self.operands)
        )

    @property
    def columns(self) -> list:
        try:
            return list(self._meta.columns)
        except AttributeError:
            return []

    @property
    def dtypes(self):
        return self._meta.dtypes

    @property
    def _meta(self):
        raise NotImplementedError()

    def __dask_graph__(self):
        """Traverse expression tree, collect layers"""
        stack = [self]
        seen = set()
        layers = []
        while stack:
            expr = stack.pop()

            if expr._name in seen:
                continue
            seen.add(expr._name)

            layers.append(expr._layer())
            for operand in expr.operands:
                if isinstance(operand, Expr):
                    stack.append(operand)

        return toolz.merge(layers)

    def __dask_keys__(self):
        return [(self._name, i) for i in range(self.npartitions)]

    def substitute(self, substitutions: dict) -> Expr:
        """Substitute specific `Expr` instances within `self`

        Parameters
        ----------
        substitutions:
            mapping old terms to new terms. Note that using
            non-`Expr` keys may produce unexpected results,
            and substituting boolean values is not allowed.

        Examples
        --------
        >>> (df + 10).substitute({10: 20})
        df + 20
        """
        if not substitutions:
            return self

        if self in substitutions:
            return substitutions[self]

        new = []
        update = False
        for operand in self.operands:
            if (
                not isinstance(operand, bool)
                and ishashable(operand)
                and operand in substitutions
            ):
                new.append(substitutions[operand])
                update = True
            elif isinstance(operand, Expr):
                val = operand.substitute(substitutions)
                if operand._name != val._name:
                    update = True
                new.append(val)
            else:
                new.append(operand)

        if update:  # Only recreate if something changed
            return type(self)(*new)
        return self

    def substitute_parameters(self, substitutions: dict) -> Expr:
        """Substitute specific `Expr` parameters

        Parameters
        ----------
        substitutions:
            Mapping of parameter keys to new values. Keys that
            are not found in ``self._parameters`` will be ignored.
        """
        if not substitutions:
            return self

        changed = False
        new_operands = []
        for i, operand in enumerate(self.operands):
            if i < len(self._parameters) and self._parameters[i] in substitutions:
                new_operands.append(substitutions[self._parameters[i]])
                changed = True
            else:
                new_operands.append(operand)
        if changed:
            return type(self)(*new_operands)
        return self

    def _find_similar_operations(self, root: Expr, ignore: list | None = None):
        # Find operations with the same type and operands.
        # Parameter keys specified by `ignore` will not be
        # included in the operand comparison
        alike = [
            op for op in root.find_operations(type(self)) if op._name != self._name
        ]
        if not alike:
            # No other operations of the same type. Early return
            return []

        def _tokenize(rp):
            # Helper function to "tokenize" the operands
            # that are not in the `ignore` list
            return _tokenize_deterministic(
                *[
                    op
                    for i, op in enumerate(rp.operands)
                    if i >= len(rp._parameters) or rp._parameters[i] not in ignore
                ]
            )

        # Return subset of `alike` with the same "token"
        token = _tokenize(self)
        return [item for item in alike if _tokenize(item) == token]

    def _node_label_args(self):
        """Operands to include in the node label by `visualize`"""
        return self.dependencies()

    def _to_graphviz(
        self,
        rankdir="BT",
        graph_attr=None,
        node_attr=None,
        edge_attr=None,
        **kwargs,
    ):
        from dask.dot import label, name

        graphviz = import_required(
            "graphviz",
            "Drawing dask graphs with the graphviz visualization engine requires the `graphviz` "
            "python library and the `graphviz` system library.\n\n"
            "Please either conda or pip install as follows:\n\n"
            "  conda install python-graphviz     # either conda install\n"
            "  python -m pip install graphviz    # or pip install and follow installation instructions",
        )

        graph_attr = graph_attr or {}
        node_attr = node_attr or {}
        edge_attr = edge_attr or {}

        graph_attr["rankdir"] = rankdir
        node_attr["shape"] = "box"
        node_attr["fontname"] = "helvetica"

        graph_attr.update(kwargs)
        g = graphviz.Digraph(
            graph_attr=graph_attr,
            node_attr=node_attr,
            edge_attr=edge_attr,
        )

        stack = [self]
        seen = set()
        dependencies = {}
        while stack:
            expr = stack.pop()

            if expr._name in seen:
                continue
            seen.add(expr._name)

            dependencies[expr] = set(expr.dependencies())
            for dep in expr.dependencies():
                stack.append(dep)

        cache = {}
        for expr in dependencies:
            expr_name = name(expr)
            attrs = {}

            # Make node label
            deps = [
                funcname(type(dep)) if isinstance(dep, Expr) else str(dep)
                for dep in expr._node_label_args()
            ]
            _label = funcname(type(expr))
            if deps:
                _label = f"{_label}({', '.join(deps)})" if deps else _label
            node_label = label(_label, cache=cache)

            attrs.setdefault("label", str(node_label))
            attrs.setdefault("fontsize", "20")
            g.node(expr_name, **attrs)

        for expr, deps in dependencies.items():
            expr_name = name(expr)
            for dep in deps:
                dep_name = name(dep)
                g.edge(dep_name, expr_name)

        return g

    def visualize(self, filename="dask-expr.svg", format=None, **kwargs):
        """
        Visualize the expression graph.
        Requires ``graphviz`` to be installed.

        Parameters
        ----------
        filename : str or None, optional
            The name of the file to write to disk. If the provided `filename`
            doesn't include an extension, '.png' will be used by default.
            If `filename` is None, no file will be written, and the graph is
            rendered in the Jupyter notebook only.
        format : {'png', 'pdf', 'dot', 'svg', 'jpeg', 'jpg'}, optional
            Format in which to write output file. Default is 'svg'.
        **kwargs
           Additional keyword arguments to forward to ``to_graphviz``.
        """
        from dask.dot import graphviz_to_file

        g = self._to_graphviz(**kwargs)
        graphviz_to_file(g, filename, format)
        return g

    def walk(self) -> Generator[Expr]:
        """Iterate through all expressions in the tree

        Returns
        -------
        nodes
            Generator of Expr instances in the graph.
            Ordering is a depth-first search of the expression tree
        """
        stack = [self]
        seen = set()
        while stack:
            node = stack.pop()
            if node._name in seen:
                continue
            seen.add(node._name)

            for dep in node.dependencies():
                stack.append(dep)

            yield node

    def find_operations(self, operation: type | tuple[type]) -> Generator[Expr]:
        """Search the expression graph for a specific operation type

        Parameters
        ----------
        operation
            The operation type to search for.

        Returns
        -------
        nodes
            Generator of `operation` instances. Ordering corresponds
            to a depth-first search of the expression graph.
        """
        assert (
            isinstance(operation, tuple)
            and all(issubclass(e, Expr) for e in operation)
            or issubclass(operation, Expr)
        ), "`operation` must be`Expr` subclass)"
        return (expr for expr in self.walk() if isinstance(expr, operation))


class Literal(Expr):
    """Represent a literal (known) value as an `Expr`"""

    _parameters = ["value"]

    def _divisions(self):
        return (None, None)

    @property
    def _meta(self):
        return make_meta(self.value)

    def _task(self, index: int):
        assert index == 0
        return self.value


class Blockwise(Expr):
    """Super-class for block-wise operations

    This is fairly generic, and includes definitions for `_meta`, `divisions`,
    `_layer` that are often (but not always) correct.  Mostly this helps us
    avoid duplication in the future.

    Note that `Fused` expressions rely on every `Blockwise`
    expression defining a proper `_task` method.
    """

    operation = None
    _keyword_only = []
    _projection_passthrough = False
    _filter_passthrough = False

    @property
    def _required_attribute(self):
        if isinstance(self.operation, type(M.method_caller)):
            return self.operation.method
        return None

    @functools.cached_property
    def _meta(self):
        args = [op._meta if isinstance(op, Expr) else op for op in self._args]
        return self.operation(*args, **self._kwargs)

    @functools.cached_property
    def _kwargs(self) -> dict:
        if self._keyword_only:
            return {
                p: self.operand(p)
                for p in self._parameters
                if p in self._keyword_only and self.operand(p) is not no_default
            }
        return {}

    @functools.cached_property
    def _args(self) -> list:
        if self._keyword_only:
            args = [
                self.operand(p) for p in self._parameters if p not in self._keyword_only
            ] + self.operands[len(self._parameters) :]
            return args
        return self.operands

    def _broadcast_dep(self, dep: Expr):
        # Checks if a dependency should be broadcasted to
        # all partitions of this `Blockwise` operation
        return dep.npartitions == 1 and dep.ndim < self.ndim

    def _divisions(self):
        # This is an issue.  In normal Dask we re-divide everything in a step
        # which combines divisions and graph.
        # We either have to create a new Align layer (ok) or combine divisions
        # and graph into a single operation.
        dependencies = self.dependencies()
        for arg in dependencies:
            if not self._broadcast_dep(arg):
                assert arg.divisions == dependencies[0].divisions
        return dependencies[0].divisions

    @functools.cached_property
    def _name(self):
        if self.operation:
            head = funcname(self.operation)
        else:
            head = funcname(type(self)).lower()
        return head + "-" + _tokenize_deterministic(*self.operands)

    def _blockwise_arg(self, arg, i):
        """Return a Blockwise-task argument"""
        if isinstance(arg, Expr):
            # Make key for Expr-based argument
            if self._broadcast_dep(arg):
                return (arg._name, 0)
            else:
                return (arg._name, i)

        else:
            return arg

    def _task(self, index: int):
        """Produce the task for a specific partition

        Parameters
        ----------
        index:
            Partition index for this task.

        Returns
        -------
        task: tuple
        """
        args = [self._blockwise_arg(op, index) for op in self._args]
        if self._kwargs:
            return apply, self.operation, args, self._kwargs
        else:
            return (self.operation,) + tuple(args)

    def _simplify_up(self, parent):
        if self._projection_passthrough and isinstance(parent, Projection):
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])

    def _combine_similar(self, root: Expr):
        # Push projections back up through `_projection_passthrough`
        # operations if it reduces the number of unique expression nodes.
        if (
            self._projection_passthrough
            and isinstance(self.frame, Projection)
            or self._filter_passthrough
            and isinstance(self.frame, Filter)
        ):
            frame, operations = self.frame, []
            # We have to go back until we reach an operation that was not pushed down
            while isinstance(frame, (Filter, Projection)):
                operations.append(frame.operands[1])
                frame = frame.frame
            else:
                try:
                    common = type(self)(frame, *self.operands[1:])
                except ValueError:
                    # May have encountered a problem with `_required_attribute`.
                    # (There is no guarentee that the same method will exist for
                    # both a Series and DataFrame)
                    return None
            push_up_op = False
            for op in self._find_similar_operations(root, ignore=self._parameters):
                if (
                    isinstance(op.frame, (Projection, Filter))
                    and (
                        common._name == type(op)(op.frame.frame, *op.operands[1:])._name
                    )
                ) or common._name == op._name:
                    push_up_op = True
                    break

            if push_up_op:
                # Add operations back in the same order
                for op in reversed(operations):
                    common = common[op]
                return common
        return None


class MapPartitions(Blockwise):
    _parameters = [
        "frame",
        "func",
        "meta",
        "enforce_metadata",
        "transform_divisions",
        "clear_divisions",
        "kwargs",
    ]
    _defaults = {"kwargs": None}

    def __str__(self):
        return f"MapPartitions({funcname(self.func)})"

    def _broadcast_dep(self, dep: Expr):
        # Always broadcast single-partition dependencies in MapPartitions
        return dep.npartitions == 1

    @property
    def args(self):
        return [self.frame] + self.operands[len(self._parameters) :]

    @functools.cached_property
    def _meta(self):
        meta = self.operand("meta")
        args = [arg._meta if isinstance(arg, Expr) else arg for arg in self.args]
        return _get_meta_map_partitions(args, [], self.func, self.kwargs, meta, None)

    def _divisions(self):
        # Unknown divisions
        if self.clear_divisions:
            return (None,) * (self.frame.npartitions + 1)

        # (Possibly) known divisions
        dfs = [arg for arg in self.args if isinstance(arg, Expr)]
        return _get_divisions_map_partitions(
            True,  # Partitions must already be "aligned"
            self.transform_divisions,
            dfs,
            self.func,
            self.args,
            self.kwargs,
        )

    def _task(self, index: int):
        args = [self._blockwise_arg(op, index) for op in self.args]
        kwargs = self.kwargs if self.kwargs is not None else {}
        if self.enforce_metadata:
            kwargs = kwargs.copy()
            kwargs.update(
                {
                    "_func": self.func,
                    "_meta": self._meta,
                }
            )
            return (apply, apply_and_enforce, args, kwargs)
        else:
            return (
                apply,
                self.func,
                args,
                kwargs,
            )


class _Align(Blockwise):
    _parameters = ["frame", "other", "join", "fill_value"]
    _defaults = {"join": "outer", "fill_value": None}
    _keyword_only = ["join", "fill_value"]
    operation = M.align

    def _divisions(self):
        # Aligning, so take first frames divisions
        return self.frame._divisions()


class AlignGetitem(Blockwise):
    _parameters = ["frame", "position"]
    operation = operator.getitem

    @functools.cached_property
    def _meta(self):
        return self.frame._meta[self.position]

    def _divisions(self):
        return self.frame._divisions()


class DropnaSeries(Blockwise):
    _parameters = ["frame"]
    operation = M.dropna


class DropnaFrame(Blockwise):
    _parameters = ["frame", "how", "subset", "thresh"]
    _defaults = {"how": no_default, "subset": None, "thresh": no_default}
    _keyword_only = ["how", "subset", "thresh"]
    operation = M.dropna

    def _simplify_up(self, parent):
        if self.subset is not None:
            columns = set(parent.columns).union(self.subset)
            if columns == set(self.frame.columns):
                # Don't add unnecessary Projections
                return

            return type(parent)(
                type(self)(self.frame[sorted(columns)], *self.operands[1:]),
                *parent.operands[1:],
            )


class CombineFirst(Blockwise):
    _parameters = ["frame", "other"]
    operation = M.combine_first

    @functools.cached_property
    def _meta(self):
        return make_meta(
            self.operation(
                meta_nonempty(self.frame._meta),
                meta_nonempty(self.other._meta),
            ),
        )

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = parent.columns
            frame_columns = sorted(set(columns).intersection(self.frame.columns))
            other_columns = sorted(set(columns).intersection(self.other.columns))
            if (
                self.frame.columns == frame_columns
                and self.other.columns == other_columns
            ):
                return
            return type(parent)(
                type(self)(self.frame[frame_columns], self.other[other_columns]),
                *parent.operands[1:],
            )


class Sample(Blockwise):
    _parameters = ["frame", "state_data", "frac", "replace"]
    operation = staticmethod(methods.sample)

    @functools.cached_property
    def _meta(self):
        args = [self.operands[0]._meta] + [self.operands[1][0]] + self.operands[2:]
        return self.operation(*args)

    def _task(self, index: int):
        args = [self._blockwise_arg(self.frame, index)] + [
            self.state_data[index],
            self.frac,
            self.replace,
        ]
        return (self.operation,) + tuple(args)


class VarColumns(Blockwise):
    _parameters = ["frame", "skipna", "ddof", "numeric_only"]
    _defaults = {"skipna": True, "ddof": 1, "numeric_only": False}
    _keyword_only = ["skipna", "ddof", "numeric_only"]
    operation = M.var

    @functools.cached_property
    def _kwargs(self) -> dict:
        return {"axis": 1, **super()._kwargs}


class Sqrt(Blockwise):
    _parameters = ["frame"]
    operation = np.sqrt


class Elemwise(Blockwise):
    """
    This doesn't really do anything, but we anticipate that future
    optimizations, like `len` will care about which operations preserve length
    """

    _filter_passthrough = True

    def _simplify_up(self, parent):
        if self._filter_passthrough and isinstance(parent, Filter):
            return type(self)(
                self.frame[parent.operand("predicate")], *self.operands[1:]
            )
        return super()._simplify_up(parent)


class RenameFrame(Elemwise):
    _parameters = ["frame", "columns"]
    _keyword_only = ["columns"]
    operation = M.rename

    def _simplify_up(self, parent):
        if isinstance(parent, Projection) and isinstance(
            self.operand("columns"), Mapping
        ):
            reverse_mapping = {val: key for key, val in self.operand("columns").items()}
            if is_series_like(parent._meta):
                # Fill this out when Series.rename is implemented
                return
            else:
                columns = [
                    reverse_mapping[col] if col in reverse_mapping else col
                    for col in parent.columns
                ]
            return type(self)(self.frame[columns], *self.operands[1:])


class Fillna(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "value"]
    _defaults = {"value": None}
    operation = M.fillna


class Replace(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "to_replace", "value", "regex"]
    _defaults = {"to_replace": None, "value": no_default, "regex": False}
    _keyword_only = ["value", "regex"]
    operation = M.replace


class Isin(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "values"]
    operation = M.isin


class Clip(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "lower", "upper"]
    _defaults = {"lower": None, "upper": None}
    operation = M.clip

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            if self.frame.columns == parent.columns:
                # Don't introduce unnecessary projections
                return
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])


class Between(Elemwise):
    _parameters = ["frame", "left", "right", "inclusive"]
    _defaults = {"inclusive": "both"}
    operation = M.between


class ToTimestamp(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "freq", "how"]
    _defaults = {"freq": None, "how": "start"}
    operation = M.to_timestamp

    def _divisions(self):
        return tuple(
            pd.Index(self.frame.divisions).to_timestamp(freq=self.freq, how=self.how)
        )


class AsType(Elemwise):
    """A good example of writing a trivial blockwise operation"""

    _parameters = ["frame", "dtypes"]
    operation = M.astype

    @functools.cached_property
    def _meta(self):
        def _cat_dtype_without_categories(dtype):
            return (
                isinstance(pd.api.types.pandas_dtype(dtype), pd.CategoricalDtype)
                and getattr(dtype, "categories", None) is None
            )

        meta = super()._meta
        dtypes = self.operand("dtypes")
        if hasattr(dtypes, "items"):
            set_unknown = [
                k for k, v in dtypes.items() if _cat_dtype_without_categories(v)
            ]
            meta = clear_known_categories(meta, cols=set_unknown)

        elif _cat_dtype_without_categories(dtypes):
            meta = clear_known_categories(meta)
        return meta

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            dtypes = self.operand("dtypes")
            if isinstance(dtypes, dict):
                dtypes = {
                    key: val for key, val in dtypes.items() if key in parent.columns
                }
                if not dtypes:
                    return type(parent)(self.frame, *parent.operands[1:])
            return type(self)(self.frame[parent.operand("columns")], dtypes)


class IsNa(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame"]
    operation = M.isna


class Round(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "decimals"]
    operation = M.round


class Abs(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame"]
    operation = M.abs


class RenameAxis(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "mapper", "index", "columns", "axis"]
    _defaults = {
        "mapper": no_default,
        "index": no_default,
        "columns": no_default,
        "axis": 0,
    }
    _keyword_only = ["mapper", "index", "columns", "axis"]
    operation = M.rename_axis


class ToFrame(Elemwise):
    _parameters = ["frame", "name"]
    _defaults = {"name": no_default}
    _keyword_only = ["name"]
    operation = M.to_frame


class ToFrameIndex(Elemwise):
    _parameters = ["frame", "index", "name"]
    _defaults = {"name": no_default, "index": True}
    _keyword_only = ["name", "index"]
    operation = M.to_frame


class Apply(Elemwise):
    """A good example of writing a less-trivial blockwise operation"""

    _parameters = ["frame", "function", "args", "kwargs"]
    _defaults = {"args": (), "kwargs": {}}
    operation = M.apply

    @property
    def _meta(self):
        return self.frame._meta.apply(self.function, *self.args, **self.kwargs)

    def _task(self, index: int):
        return (
            apply,
            M.apply,
            [
                (self.frame._name, index),
                self.function,
            ]
            + list(self.args),
            self.kwargs,
        )


class Map(Elemwise):
    _projection_passthrough = True
    _parameters = ["frame", "arg", "na_action"]
    _defaults = {"na_action": None}
    operation = M.map

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        if is_index_like(self.frame._meta):
            # Implement this consistently with dask.dataframe, e.g. add option to
            # control monotonic map func
            return (None,) * len(self.frame.divisions)
        return super()._divisions()


class ExplodeSeries(Blockwise):
    _parameters = ["frame"]
    operation = M.explode


class ExplodeFrame(ExplodeSeries):
    _parameters = ["frame", "column"]

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = set(parent.columns).union(self.column)
            if columns == set(self.frame.columns):
                # Don't add unnecessary Projections, protects against loops
                return

            return type(parent)(
                type(self)(self.frame[sorted(columns)], *self.operands[1:]),
                *parent.operands[1:],
            )


class Drop(Elemwise):
    _parameters = ["frame", "columns", "errors"]
    _defaults = {"errors": "raise"}
    operation = staticmethod(drop_by_shallow_copy)

    def _simplify_down(self):
        columns = [
            col for col in self.frame.columns if col not in self.operand("columns")
        ]
        return Projection(self.frame, columns)


class Assign(Elemwise):
    """Column Assignment"""

    _parameters = ["frame", "key", "value"]
    operation = staticmethod(methods.assign)

    @functools.cached_property
    def _meta(self):
        args = [
            meta_nonempty(op._meta) if isinstance(op, Expr) else op for op in self._args
        ]
        return make_meta(self.operation(*args, **self._kwargs))

    def _node_label_args(self):
        return [self.frame, self.key, self.value]

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            if self.key not in parent.columns:
                return type(parent)(self.frame, *parent.operands[1:])

            columns = set(parent.columns) - {self.key}
            if columns == set(self.frame.columns):
                # Protect against pushing the same projection twice
                return

            return type(parent)(
                type(self)(self.frame[sorted(columns)], *self.operands[1:]),
                *parent.operands[1:],
            )


class Eval(Elemwise):
    _parameters = ["frame", "_expr", "expr_kwargs"]
    _defaults = {"expr_kwargs": {}}
    _keyword_only = ["expr_kwargs"]
    operation = M.eval

    @functools.cached_property
    def _kwargs(self) -> dict:
        return {**self.expr_kwargs}


class Filter(Blockwise):
    _parameters = ["frame", "predicate"]
    operation = operator.getitem

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            return self.frame[parent.operand("columns")][self.predicate]
        if isinstance(parent, Index):
            return self.frame.index[self.predicate]


class Projection(Elemwise):
    """Column Selection"""

    _parameters = ["frame", "columns"]
    operation = operator.getitem
    _filter_passthrough = False

    @property
    def columns(self):
        if isinstance(self.operand("columns"), list):
            return self.operand("columns")
        elif isinstance(self.operand("columns"), pd.Index):
            return list(self.operand("columns"))
        else:
            return [self.operand("columns")]

    @property
    def _meta(self):
        if is_dataframe_like(self.frame._meta):
            return super()._meta
        # if we are not a DataFrame and have a scalar, we reduce to a scalar
        if not isinstance(self.operand("columns"), list) and not hasattr(
            self.operand("columns"), "dtype"
        ):
            return meta_nonempty(self.frame._meta).iloc[0]
        # Avoid column selection for Series/Index
        return self.frame._meta

    def _node_label_args(self):
        return [self.frame, self.operand("columns")]

    def __str__(self):
        base = str(self.frame)
        if " " in base:
            base = "(" + base + ")"
        return f"{base}[{repr(self.operand('columns'))}]"

    def _divisions(self):
        if self.ndim == 0:
            return (None, None)
        return super()._divisions()

    def _simplify_down(self):
        if (
            str(self.frame.columns) == str(self.columns)
            and self._meta.ndim == self.frame._meta.ndim
        ):
            # TODO: we should get more precise around Expr.columns types
            return self.frame
        if isinstance(self.frame, Projection):
            # df[a][b]
            a = self.frame.operand("columns")
            b = self.operand("columns")

            if not isinstance(a, list):
                # df[scalar][b] -> First selection coerces to Series
                return
            elif isinstance(b, list):
                assert all(bb in a for bb in b)
            else:
                assert b in a

            return self.frame.frame[b]


class Index(Elemwise):
    """Column Selection"""

    _parameters = ["frame"]
    operation = getattr
    _filter_passthrough = False

    @property
    def _meta(self):
        meta = self.frame._meta
        # Handle scalar results
        if is_series_like(meta) or is_dataframe_like(meta):
            return self.frame._meta.index
        return meta

    def _task(self, index: int):
        return (
            getattr,
            (self.frame._name, index),
            "index",
        )


class Lengths(Expr):
    """Returns a tuple of partition lengths"""

    _parameters = ["frame"]

    @property
    def _meta(self):
        return tuple()

    def _divisions(self):
        return (None, None)

    def _simplify_down(self):
        if isinstance(self.frame, Elemwise):
            child = max(self.frame.dependencies(), key=lambda expr: expr.npartitions)
            return Lengths(child)

    def _layer(self):
        name = "part-" + self._name
        dsk = {
            (name, i): (len, (self.frame._name, i))
            for i in range(self.frame.npartitions)
        }
        dsk[(self._name, 0)] = (tuple, list(dsk.keys()))
        return dsk


class ResetIndex(Elemwise):
    """Reset the index of a Series or DataFrame"""

    _parameters = ["frame", "drop"]
    _defaults = {"drop": False}
    _keyword_only = ["drop"]
    operation = M.reset_index

    def _divisions(self):
        return (None,) * (self.frame.npartitions + 1)


class AddPrefix(Elemwise):
    _parameters = ["frame", "prefix"]
    operation = M.add_prefix

    def _convert_columns(self, columns):
        len_prefix = len(self.prefix)
        return [col[len_prefix:] for col in columns]

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            columns = self._convert_columns(parent.columns)
            if columns == self.frame.columns:
                return
            return type(parent)(
                type(self)(self.frame[columns], self.operands[1]),
                parent.operand("columns"),
            )


class AddSuffix(AddPrefix):
    _parameters = ["frame", "suffix"]
    operation = M.add_suffix

    def _convert_columns(self, columns):
        len_suffix = len(self.suffix)
        return [col[:-len_suffix] for col in columns]


class Head(Expr):
    """Take the first `n` rows of the first partition"""

    _parameters = ["frame", "n"]
    _defaults = {"n": 5}

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        return self.frame.divisions[:2]

    def _task(self, index: int):
        raise NotImplementedError()

    def _simplify_down(self):
        if isinstance(self.frame, Elemwise):
            operands = [
                Head(op, self.n) if isinstance(op, Expr) else op
                for op in self.frame.operands
            ]
            return type(self.frame)(*operands)
        if isinstance(self.frame, Head):
            return Head(self.frame.frame, min(self.n, self.frame.n))

    def _lower(self):
        if not isinstance(self, BlockwiseHead):
            # Lower to Blockwise
            return BlockwiseHead(Partitions(self.frame, [0]), self.n)


class BlockwiseHead(Head, Blockwise):
    """Take the first `n` rows of every partition

    Typically used after `Partitions(..., [0])` to take
    the first `n` rows of an entire collection.
    """

    def _divisions(self):
        return self.frame.divisions

    def _task(self, index: int):
        return (M.head, (self.frame._name, index), self.n)


class Tail(Expr):
    """Take the last `n` rows of the last partition"""

    _parameters = ["frame", "n"]
    _defaults = {"n": 5}

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        return self.frame.divisions[-2:]

    def _task(self, index: int):
        raise NotImplementedError()

    def _simplify_down(self):
        if isinstance(self.frame, Elemwise):
            operands = [
                Tail(op, self.n) if isinstance(op, Expr) else op
                for op in self.frame.operands
            ]
            return type(self.frame)(*operands)
        if isinstance(self.frame, Tail):
            return Tail(self.frame.frame, min(self.n, self.frame.n))

    def _lower(self):
        if not isinstance(self, BlockwiseTail):
            # Lower to Blockwise
            return BlockwiseTail(
                Partitions(self.frame, [self.frame.npartitions - 1]), self.n
            )


class BlockwiseTail(Tail, Blockwise):
    """Take the last `n` rows of every partition

    Typically used after `Partitions(..., [-1])` to take
    the last `n` rows of an entire collection.
    """

    def _divisions(self):
        return self.frame.divisions

    def _task(self, index: int):
        return (M.tail, (self.frame._name, index), self.n)


class Binop(Elemwise):
    _parameters = ["left", "right"]
    _filter_passthrough = False

    def __str__(self):
        return f"{self.left} {self._operator_repr} {self.right}"

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            if isinstance(self.left, Expr) and self.left.ndim:
                left = self.left[
                    parent.operand("columns")
                ]  # TODO: filter just the correct columns
            else:
                left = self.left
            if isinstance(self.right, Expr) and self.right.ndim:
                right = self.right[parent.operand("columns")]
            else:
                right = self.right
            return type(self)(left, right)

    def _node_label_args(self):
        return [self.left, self.right]


class Add(Binop):
    operation = operator.add
    _operator_repr = "+"

    def _simplify_down(self):
        if (
            isinstance(self.left, Expr)
            and isinstance(self.right, Expr)
            and self.left._name == self.right._name
        ):
            return 2 * self.left


class Sub(Binop):
    operation = operator.sub
    _operator_repr = "-"


class Mul(Binop):
    operation = operator.mul
    _operator_repr = "*"

    def _simplify_down(self):
        if (
            isinstance(self.right, Mul)
            and isinstance(self.left, numbers.Number)
            and isinstance(self.right.left, numbers.Number)
        ):
            return (self.left * self.right.left) * self.right.right


class Div(Binop):
    operation = operator.truediv
    _operator_repr = "/"


class LT(Binop):
    operation = operator.lt
    _operator_repr = "<"


class LE(Binop):
    operation = operator.le
    _operator_repr = "<="


class GT(Binop):
    operation = operator.gt
    _operator_repr = ">"


class GE(Binop):
    operation = operator.ge
    _operator_repr = ">="


class EQ(Binop):
    operation = operator.eq
    _operator_repr = "=="


class NE(Binop):
    operation = operator.ne
    _operator_repr = "!="


class And(Binop):
    operation = operator.and_
    _operator_repr = "&"


class Or(Binop):
    operation = operator.or_
    _operator_repr = "|"


class XOr(Binop):
    operation = operator.xor
    _operator_repr = "^"


class Unaryop(Elemwise):
    _parameters = ["frame"]

    def __str__(self):
        return f"{self._operator_repr} {self.frame}"

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            if isinstance(self.frame, Expr):
                frame = self.frame[
                    parent.operand("columns")
                ]  # TODO: filter just the correct columns
            else:
                frame = self.frame
            return type(self)(frame)

    def _node_label_args(self):
        return [self.frame]


class Invert(Unaryop):
    operation = operator.inv
    _operator_repr = "~"


class Neg(Unaryop):
    operation = operator.neg
    _operator_repr = "-"


class Pos(Unaryop):
    operation = operator.pos
    _operator_repr = "+"


class Partitions(Expr):
    """Select one or more partitions"""

    _parameters = ["frame", "partitions"]

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        divisions = []
        for part in self.partitions:
            divisions.append(self.frame.divisions[part])
        divisions.append(self.frame.divisions[part + 1])
        return tuple(divisions)

    def _task(self, index: int):
        return (self.frame._name, self.partitions[index])

    def _simplify_down(self):
        if isinstance(self.frame, Blockwise) and not isinstance(
            self.frame, (BlockwiseIO, Fused)
        ):
            operands = [
                Partitions(op, self.partitions)
                if (isinstance(op, Expr) and not self.frame._broadcast_dep(op))
                else op
                for op in self.frame.operands
            ]
            return type(self.frame)(*operands)
        elif isinstance(self.frame, PartitionsFiltered):
            if self.frame._partitions:
                partitions = [self.frame._partitions[p] for p in self.partitions]
            else:
                partitions = self.partitions
            # We assume that expressions defining a special "_partitions"
            # parameter can internally capture the same logic as `Partitions`
            return self.frame.substitute_parameters({"_partitions": partitions})

    def _node_label_args(self):
        return [self.frame, self.partitions]


class PartitionsFiltered(Expr):
    """Mixin class for partition filtering

    A `PartitionsFiltered` subclass must define a
    `_partitions` parameter. When `_partitions` is
    defined, the following expresssions must produce
    the same output for `cls: PartitionsFiltered`:
      - `cls(expr: Expr, ..., _partitions)`
      - `Partitions(cls(expr: Expr, ...), _partitions)`

    In order to leverage the default `Expr._layer`
    method, subclasses should define `_filtered_task`
    instead of `_task`.
    """

    @property
    def _filtered(self) -> bool:
        """Whether or not output partitions have been filtered"""
        return self.operand("_partitions") is not None

    @property
    def _partitions(self) -> list | tuple | range:
        """Selected partition indices"""
        if self._filtered:
            return self.operand("_partitions")
        else:
            return range(self.npartitions)

    @functools.cached_property
    def divisions(self):
        # Common case: Use self._divisions()
        full_divisions = super().divisions
        if not self._filtered:
            return full_divisions

        # Specific case: Specific partitions were selected
        new_divisions = []
        for part in self._partitions:
            new_divisions.append(full_divisions[part])
        new_divisions.append(full_divisions[part + 1])
        return tuple(new_divisions)

    @property
    def npartitions(self):
        if self._filtered:
            return len(self._partitions)
        return super().npartitions

    def _task(self, index: int):
        return self._filtered_task(self._partitions[index])

    def _filtered_task(self, index: int):
        raise NotImplementedError()


@normalize_token.register(Expr)
def normalize_expression(expr):
    return expr._name


def optimize(expr: Expr, combine_similar: bool = True, fuse: bool = True) -> Expr:
    """High level query optimization

    This leverages three optimization passes:

    1.  Class based simplification using the ``_simplify`` function and methods
    2.  Combine similar operations
    3.  Blockwise fusion

    Parameters
    ----------
    expr:
        Input expression to optimize
    combine_similar:
        whether or not to combine similar operations
        (like `ReadParquet`) to aggregate redundant work.
    fuse:
        whether or not to turn on blockwise fusion

    See Also
    --------
    simplify
    combine_similar
    optimize_blockwise_fusion
    """

    result = expr
    while True:
        out = result.simplify().lower_once()
        if out._name == result._name:
            break
        result = out

    if combine_similar:
        result = result.combine_similar()

    if fuse:
        result = optimize_blockwise_fusion(result)

    return result


def is_broadcastable(s):
    """
    This Series is broadcastable against another dataframe in the sequence
    """

    return s.ndim == 1 and s.npartitions == 1 and s.known_divisions or s.ndim == 0


def non_blockwise_ancestors(expr):
    """Traverse through tree to find ancestors that are not blockwise or are IO"""
    stack = [expr]
    while stack:
        e = stack.pop()
        if isinstance(e, IO):
            yield e
        elif isinstance(e, Blockwise):
            dependencies = e.dependencies()
            stack.extend([expr for expr in dependencies if not is_broadcastable(expr)])
        else:
            yield e


def are_co_aligned(*exprs):
    """Do inputs come from different parents, modulo blockwise?"""
    exprs = [expr for expr in exprs if not is_broadcastable(expr)]
    ancestors = [set(non_blockwise_ancestors(e)) for e in exprs]
    return len(set(flatten(ancestors, container=set))) == 1


## Utilites for Expr fusion


def optimize_blockwise_fusion(expr):
    """Traverse the expression graph and apply fusion"""

    def _fusion_pass(expr):
        # Full pass to find global dependencies
        seen = set()
        stack = [expr]
        dependents = defaultdict(set)
        dependencies = {}
        while stack:
            next = stack.pop()

            if next._name in seen:
                continue
            seen.add(next._name)

            if isinstance(next, Blockwise):
                dependencies[next] = set()
                if next not in dependents:
                    dependents[next] = set()

            for operand in next.operands:
                if isinstance(operand, Expr):
                    stack.append(operand)
                    if isinstance(operand, Blockwise):
                        if next in dependencies:
                            dependencies[next].add(operand)
                        dependents[operand].add(next)

        # Traverse each "root" until we find a fusable sub-group.
        # Here we use root to refer to a Blockwise Expr node that
        # has no Blockwise dependents
        roots = [
            k
            for k, v in dependents.items()
            if v == set() or all(not isinstance(_expr, Blockwise) for _expr in v)
        ]
        while roots:
            root = roots.pop()
            seen = set()
            stack = [root]
            group = []
            while stack:
                next = stack.pop()

                if next._name in seen:
                    continue
                seen.add(next._name)

                group.append(next)
                for dep in dependencies[next]:
                    if (dep.npartitions == root.npartitions) and not (
                        dependents[dep] - set(stack) - set(group)
                    ):
                        # All of deps dependents are contained
                        # in the local group (or the local stack
                        # of expr nodes that we know we will be
                        # adding to the local group).
                        # All nodes must also have the same number
                        # of partitions, since broadcasting within
                        # a group is not allowed.
                        stack.append(dep)
                    elif dependencies[dep] and dep._name not in [
                        r._name for r in roots
                    ]:
                        # Couldn't fuse dep, but we may be able to
                        # use it as a new root on the next pass
                        roots.append(dep)

            # Replace fusable sub-group
            if len(group) > 1:
                group_deps = []
                local_names = [_expr._name for _expr in group]
                for _expr in group:
                    group_deps += [
                        operand
                        for operand in _expr.dependencies()
                        if operand._name not in local_names
                    ]
                to_replace = {group[0]: Fused(group, *group_deps)}
                return expr.substitute(to_replace), not roots

        # Return original expr if no fusable sub-groups were found
        return expr, True

    while True:
        original_name = expr._name
        expr, done = _fusion_pass(expr)
        if done or expr._name == original_name:
            break

    return expr


class Fused(Blockwise):
    """Fused ``Blockwise`` expression

    A ``Fused`` corresponds to the fusion of multiple
    ``Blockwise`` expressions into a single ``Expr`` object.
    Before graph-materialization time, the behavior of this
    object should be identical to that of the first element
    of ``Fused.exprs`` (i.e. the top-most expression in
    the fused group).

    Parameters
    ----------
    exprs : List[Expr]
        Group of original ``Expr`` objects being fused together.
    *dependencies:
        List of external ``Expr`` dependencies. External-``Expr``
        dependencies correspond to any ``Expr`` operand that is
        not already included in ``exprs``. Note that these
        dependencies should be defined in the order of the ``Expr``
        objects that require them (in ``exprs``). These
        dependencies do not include literal operands, because those
        arguments should already be captured in the fused subgraphs.
    """

    _parameters = ["exprs"]

    @functools.cached_property
    def _meta(self):
        return self.exprs[0]._meta

    def _tree_repr_lines(self, indent=0, recursive=True):
        header = f"Fused({self._name[-5:]}):"
        if not recursive:
            return [header]

        seen = set()
        lines = []
        stack = [(self.exprs[0], 2)]
        fused_group = [_expr._name for _expr in self.exprs]
        dependencies = {dep._name: dep for dep in self.dependencies()}
        while stack:
            expr, _indent = stack.pop()

            if expr._name in seen:
                continue
            seen.add(expr._name)

            line = expr._tree_repr_lines(_indent, recursive=False)[0]
            lines.append(line.replace(" ", "|", 1))
            for dep in expr.dependencies():
                if dep._name in fused_group:
                    stack.append((dep, _indent + 2))
                elif dep._name in dependencies:
                    dependencies.pop(dep._name)
                    lines.extend(dep._tree_repr_lines(_indent + 2))

        for dep in dependencies.values():
            lines.extend(dep._tree_repr_lines(2))

        lines = [header] + lines
        lines = [" " * indent + line for line in lines]

        return lines

    def __str__(self):
        names = [expr._name.split("-")[0] for expr in self.exprs]
        if len(names) > 3:
            names = [names[0], f"{len(names) - 2}", names[-1]]
        descr = "-".join(names)
        return f"Fused-{descr}"

    @functools.cached_property
    def _name(self):
        return f"{str(self)}-{_tokenize_deterministic(self.exprs)}"

    def _divisions(self):
        return self.exprs[0]._divisions()

    def _broadcast_dep(self, dep: Expr):
        # Always broadcast single-partition dependencies in Fused
        return dep.npartitions == 1

    def _task(self, index):
        graph = {self._name: (self.exprs[0]._name, index)}
        for _expr in self.exprs:
            if isinstance(_expr, Fused):
                (_, subgraph, name) = _expr._task(index)
                graph.update(subgraph)
                graph[(name, index)] = name
            else:
                graph[(_expr._name, index)] = _expr._task(index)

        for i, dep in enumerate(self.dependencies()):
            graph[self._blockwise_arg(dep, index)] = "_" + str(i)

        return (
            Fused._execute_task,
            graph,
            self._name,
        ) + tuple(self._blockwise_arg(dep, index) for dep in self.dependencies())

    @staticmethod
    def _execute_task(graph, name, *deps):
        for i, dep in enumerate(deps):
            graph["_" + str(i)] = dep
        return dask.core.get(graph, name)


from dask_expr._reductions import (
    All,
    Any,
    Count,
    IdxMax,
    IdxMin,
    Max,
    Mean,
    Min,
    Mode,
    NBytes,
    NuniqueApprox,
    Prod,
    Size,
    Sum,
    Var,
)
from dask_expr.io import IO, BlockwiseIO
