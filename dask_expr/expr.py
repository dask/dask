from __future__ import annotations

import functools
import numbers
import operator
import os
from collections import defaultdict

import dask
import pandas as pd
import toolz
from dask.base import normalize_token, tokenize
from dask.core import ishashable
from dask.dataframe import methods
from dask.dataframe.core import (
    _get_divisions_map_partitions,
    _get_meta_map_partitions,
    apply_and_enforce,
    is_dataframe_like,
    is_index_like,
    is_series_like,
)
from dask.utils import M, apply, funcname, import_required, is_arraylike

replacement_rules = []

no_default = "__no_default__"


class Expr:
    """Primary class for all Expressions

    This mostly includes Dask protocols and various Pandas-like method
    definitions to make us look more like a DataFrame.
    """

    commutative = False
    associative = False
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

                elif repr(op) != repr(default):
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
            # Allow operands to be accessed as attributes
            # as long as the keys are not already reserved
            # by existing methods/properties
            _parameters = type(self)._parameters
            if key in _parameters:
                idx = _parameters.index(key)
                return self.operands[idx]
            if is_dataframe_like(self._meta) and key in self._meta.columns:
                return self[key]
            raise err

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

    def optimize(self, **kwargs):
        return optimize(self, **kwargs)

    @property
    def index(self):
        return Index(self)

    @property
    def size(self):
        return Size(self)

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
        return EQ(other, self)

    def __ne__(self, other):
        return NE(other, self)

    def __and__(self, other):
        return And(other, self)

    def __or__(self, other):
        return Or(other, self)

    def sum(self, skipna=True, numeric_only=None, min_count=0):
        return Sum(self, skipna, numeric_only, min_count)

    def prod(self, skipna=True, numeric_only=None, min_count=0):
        return Prod(self, skipna, numeric_only, min_count)

    def mean(self, skipna=True, numeric_only=None, min_count=0):
        return Mean(self, skipna=skipna, numeric_only=numeric_only)

    def max(self, skipna=True, numeric_only=None, min_count=0):
        return Max(self, skipna, numeric_only, min_count)

    def any(self, skipna=True):
        return Any(self, skipna=skipna)

    def all(self, skipna=True):
        return All(self, skipna=skipna)

    def mode(self, dropna=True):
        return Mode(self, dropna=dropna)

    def min(self, skipna=True, numeric_only=None, min_count=0):
        return Min(self, skipna, numeric_only, min_count)

    def count(self, numeric_only=None):
        return Count(self, numeric_only)

    def astype(self, dtypes):
        return AsType(self, dtypes)

    def isna(self):
        return IsNa(self)

    def apply(self, function, *args, **kwargs):
        return Apply(self, function, args, kwargs)

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
        return funcname(type(self)).lower() + "-" + tokenize(*self.operands)

    @property
    def columns(self):
        return self._meta.columns

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


class Blockwise(Expr):
    """Super-class for block-wise operations

    This is fairly generic, and includes definitions for `_meta`, `divisions`,
    `_layer` that are often (but not always) correct.  Mostly this helps us
    avoid duplication in the future.

    Note that `Fused` expressions rely on every `Blockwise`
    expression defining a proper `_task` method.
    """

    operation = None

    @functools.cached_property
    def _meta(self):
        return self.operation(
            *[arg._meta if isinstance(arg, Expr) else arg for arg in self.operands]
        )

    @property
    def _kwargs(self):
        return {}

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
        return head + "-" + tokenize(*self.operands)

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
        args = tuple(self._blockwise_arg(op, index) for op in self.operands)
        if self._kwargs:
            return (apply, self.operation, args, self._kwargs)
        else:
            return (self.operation,) + args


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
        if self.enforce_metadata:
            kwargs = self.kwargs.copy()
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
                self.kwargs,
            )


class Elemwise(Blockwise):
    """
    This doesn't really do anything, but we anticipate that future
    optimizations, like `len` will care about which operations preserve length
    """

    pass


class AsType(Elemwise):
    """A good example of writing a trivial blockwise operation"""

    _parameters = ["frame", "dtypes"]
    operation = M.astype


class IsNa(Elemwise):
    _parameters = ["frame"]
    operation = M.isna


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


class Assign(Elemwise):
    """Column Assignment"""

    _parameters = ["frame", "key", "value"]
    operation = staticmethod(methods.assign)

    def _node_label_args(self):
        return [self.frame, self.key, self.value]


class Filter(Blockwise):
    _parameters = ["frame", "predicate"]
    operation = operator.getitem

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            return self.frame[parent.operand("columns")][self.predicate]


class Projection(Elemwise):
    """Column Selection"""

    _parameters = ["frame", "columns"]
    operation = operator.getitem

    @property
    def columns(self):
        if isinstance(self.operand("columns"), list):
            return pd.Index(self.operand("columns"))
        else:
            return self.operand("columns")

    @property
    def _meta(self):
        if is_dataframe_like(self.frame._meta):
            return super()._meta
        # Avoid column selection for Series/Index
        return self.frame._meta

    def _node_label_args(self):
        return [self.frame, self.operand("columns")]

    def __str__(self):
        base = str(self.frame)
        if " " in base:
            base = "(" + base + ")"
        return f"{base}[{repr(self.columns)}]"

    def _simplify_down(self):
        if isinstance(self.frame, Projection):
            # df[a][b]
            a = self.frame.operand("columns")
            b = self.operand("columns")

            if not isinstance(a, list):
                assert a == b
            elif isinstance(b, list):
                assert all(bb in a for bb in b)
            else:
                assert b in a

            return self.frame.frame[b]


class Index(Elemwise):
    """Column Selection"""

    _parameters = ["frame"]
    operation = getattr

    @property
    def _meta(self):
        return self.frame._meta.index

    def _task(self, index: int):
        return (
            getattr,
            (self.frame._name, index),
            "index",
        )


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
        if not isinstance(self, BlockwiseHead):
            # Lower to Blockwise
            return BlockwiseHead(Partitions(self.frame, [0]), self.n)
        if isinstance(self.frame, Head):
            return Head(self.frame.frame, min(self.n, self.frame.n))


class BlockwiseHead(Head, Blockwise):
    """Take the first `n` rows of every partition

    Typically used after `Partition(..., [0])` to take
    the first `n` rows of an entire collection.
    """

    def _divisions(self):
        return self.frame.divisions

    def _task(self, index: int):
        return (M.head, (self.frame._name, index), self.n)


class Binop(Elemwise):
    _parameters = ["left", "right"]

    def __str__(self):
        return f"{self.left} {self._operator_repr} {self.right}"

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            if isinstance(self.left, Expr):
                left = self.left[
                    parent.operand("columns")
                ]  # TODO: filter just the correct columns
            else:
                left = self.left
            if isinstance(self.right, Expr):
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
            operands = [
                partitions if self.frame._parameters[i] == "_partitions" else op
                for i, op in enumerate(self.frame.operands)
            ]
            return type(self.frame)(*operands)

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


def optimize(expr: Expr, fuse: bool = True) -> Expr:
    """High level query optimization

    This leverages three optimization passes:

    1.  Class based simplification using the ``_simplify`` function and methods
    2.  Blockwise fusion

    Parameters
    ----------
    expr:
        Input expression to optimize
    fuse:
        whether or not to turn on blockwise fusion

    See Also
    --------
    simplify
    optimize_blockwise_fusion
    """
    expr = expr.simplify()

    if fuse:
        expr = optimize_blockwise_fusion(expr)

    return expr


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
                    elif dep not in roots and dependencies[dep]:
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
        return f"{str(self)}-{tokenize(self.exprs)}"

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


from dask_expr.io import BlockwiseIO
from dask_expr.reductions import All, Any, Count, Max, Mean, Min, Mode, Prod, Size, Sum
