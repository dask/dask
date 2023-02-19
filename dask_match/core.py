import math
import operator

import pandas as pd
import toolz
from dask.base import DaskMethodsMixin, named_schedulers, normalize_token, tokenize
from dask.dataframe.core import _concat
from dask.utils import M, apply, funcname
from matchpy import Arity, Operation
from matchpy.expressions.expressions import _OperationMeta


class _APIMeta(_OperationMeta):
    def __call__(cls, *args, variable_name=None, **kwargs):
        args, kwargs = cls.normalize(*args, **kwargs)
        operands = list(args)
        for parameter in cls._parameters[len(operands) :]:
            operands.append(kwargs.pop(parameter, cls._defaults[parameter]))
        assert not kwargs
        return super().__call__(*operands, variable_name=None)


matching = False


class API(Operation, DaskMethodsMixin, metaclass=_APIMeta):
    commutative = False
    associative = False
    _parameters = []
    _defaults = {}

    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)

    @classmethod
    def normalize(cls, *args, **kwargs):
        return args, kwargs

    def __str__(self):
        s = ", ".join(
            str(param) + "=" + str(operand)
            for param, operand in zip(self._parameters, self.operands)
            if operand != self._defaults.get(param)
        )
        return f"{type(self).__name__}({s})"

    def __repr__(self):
        return str(self)

    def __getattr__(self, key):
        if key in type(self)._parameters:
            idx = type(self)._parameters.index(key)
            return self.operands[idx]
        elif key in dir(type(self)):
            return object.__getattribute__(self, key)
        elif key in self.columns:
            return self[key]
        else:
            return object.__getattribute__(self, key)

    def __setattr__(self, key, value):
        if key in type(self)._parameters:
            idx = type(self)._parameters.index(key)
            self.operands[idx] = value
        else:
            object.__setattr__(self, key, value)

    def __getitem__(self, other):
        if isinstance(other, API):
            return Filter(self, other)
        else:
            return Projection(self, other)

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
        if matching:  # Defer to matchpy when optimizing
            return Operation.__eq__(self, other)
        else:
            return EQ(other, self)

    def __ne__(self, other):
        if matching:  # Defer to matchpy when optimizing
            return Operation.__ne__(self, other)
        else:
            return NE(other, self)

    def sum(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Sum(self, skipna, level, numeric_only, min_count)

    def mean(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return self.sum(skipna=skipna) / self.count()

    def max(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Max(self, skipna, level, numeric_only, min_count)

    def mode(self, dropna=True):
        return Mode(self, dropna=dropna)

    def min(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Min(self, skipna, level, numeric_only, min_count)

    def count(self, numeric_only=None):
        return Count(self, numeric_only)

    @property
    def size(self):
        return Size(self)

    def astype(self, dtypes):
        return AsType(self, dtypes)

    def apply(self, function, *args, **kwargs):
        return Apply(self, function, args, kwargs)

    @property
    def divisions(self):
        if "divisions" in self._parameters:
            idx = self._parameters.index("divisions")
            return self.operands[idx]
        return tuple(self._divisions())

    @property
    def dask(self):
        return self.__dask_graph__()

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
            return len(self._divisions()) - 1

    @property
    def _name(self):
        if "_name" in self._parameters:
            idx = self._parameters.index("_name")
            return self.operands[idx]
        return funcname(type(self)).lower() + "-" + tokenize(*self.operands)

    @property
    def columns(self):
        if "columns" in self._parameters:
            idx = self._parameters.index("columns")
            return self.operands[idx]
        else:
            return self._meta.columns

    @property
    def dtypes(self):
        return self._meta.dtypes

    @property
    def _meta(self):
        if "_meta" in self._parameters:
            idx = self._parameters.index("_meta")
            return self.operands[idx]
        raise NotImplementedError()

    def _divisions(self):
        raise NotImplementedError()

    def __dask_graph__(self):
        stack = [self]
        layers = []
        while stack:
            expr = stack.pop()
            layers.append(expr._layer())
            for operand in expr.operands:
                if isinstance(operand, API):
                    stack.append(operand)

        return toolz.merge(layers)

    def __dask_keys__(self):
        return [(self._name, i) for i in range(self.npartitions)]

    def __dask_postcompute__(self):
        return _concat, ()

    def __dask_postpersist__(self):
        raise NotImplementedError()


class Blockwise(API):
    arity = Arity.variadic
    operation = None

    @property
    def _meta(self):
        return self.operation(
            *[arg._meta if isinstance(arg, API) else arg for arg in self.operands]
        )

    @property
    def _kwargs(self):
        return {}

    def _divisions(self):
        # This is an issue.  In normal Dask we re-divide everything in a step
        # which combines divisions and graph.
        # We either have to create a new Align layer (ok) or combine divisions
        # and graph into a single operation.
        first = [o for o in self.operands if isinstance(o, API)][0]
        assert all(
            arg.divisions == first.divisions
            for arg in self.operands
            if isinstance(arg, API)
        )
        return first.divisions

    @property
    def _name(self):
        return funcname(self.operation) + "-" + tokenize(*self.operands)

    def _layer(self):
        return {
            (self._name, i): (
                apply,
                self.operation,
                [
                    (operand._name, i) if isinstance(operand, API) else operand
                    for operand in self.operands
                ],
                self._kwargs,
            )
            for i in range(self.npartitions)
        }


class Elemwise(Blockwise):
    pass


class AsType(Elemwise):
    _parameters = ["frame", "dtypes"]
    operation = M.astype


class Apply(Elemwise):
    _parameters = ["frame", "function", "args", "kwargs"]
    _defaults = {"args": (), "kwargs": {}}
    operation = M.apply

    @property
    def _meta(self):
        return self.frame._meta.apply(self.function, *self.args, **self.kwargs)

    def _layer(self):
        return {
            (self._name, i): (
                apply,
                M.apply,
                [(self.frame._name, i), self.function] + list(self.args),
                self.kwargs,
            )
            for i in range(self.npartitions)
        }


class Filter(Blockwise):
    _parameters = ["frame", "predicate"]

    @property
    def _meta(self):
        return self.frame._meta

    def _divisions(self):
        return self.frame.divisions

    def _layer(self):
        return {
            (self._name, i): (
                operator.getitem,
                (self.frame._name, i),
                (self.predicate._name, i),
            )
            for i in range(self.npartitions)
        }


class Projection(Elemwise):
    _parameters = ["frame", "columns"]
    operation = operator.getitem

    def _divisions(self):
        return self.frame.divisions

    @property
    def _meta(self):
        return self.frame._meta[self.columns]

    def _layer(self):
        return {
            (self._name, i): (operator.getitem, (self.frame._name, i), self.columns)
            for i in range(self.npartitions)
        }

    def __str__(self):
        base = str(self.frame)
        if " " in base:
            base = "(" + base + ")"
        return f"{base}[{repr(self.columns)}]"


class Binop(Elemwise):
    _parameters = ["left", "right"]
    arity = Arity.binary

    def _layer(self):
        return {
            (self._name, i): (
                self.operation,
                (self.left._name, i) if isinstance(self.left, API) else self.left,
                (self.right._name, i) if isinstance(self.right, API) else self.right,
            )
            for i in range(self.npartitions)
        }

    def __str__(self):
        return f"{self.left} {self._operator_repr} {self.right}"


class Add(Binop):
    operation = operator.add
    _operator_repr = "+"


class Sub(Binop):
    operation = operator.sub
    _operator_repr = "-"


class Mul(Binop):
    operation = operator.mul
    _operator_repr = "*"


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


class IO(API):
    pass


class ReadParquet(IO):
    _parameters = ["filename", "columns", "filters"]
    _defaults = {"columns": None, "filters": None}

    @staticmethod
    def normalize(filename=None, columns=None, filters=None):
        if isinstance(columns, tuple):
            columns = list(columns)
        return (
            filename,
            columns,
            filters,
        ), {}

    @property
    def _meta(self):
        df = pd.DataFrame({"a": [1], "b": [2.0], "c": [4], "d": [5.0]})
        if self.columns is not None:
            df = df[self.columns]
        return df.head(0)


class ReadCSV(IO):
    _parameters = ["filename", "usecols", "header"]
    _defaults = {"usecols": None, "header": None}


class from_pandas(IO):
    _parameters = ["frame", "npartitions"]
    _defaults = {"npartitions": 1}

    @property
    def _meta(self):
        return self.frame.head(0)

    def _divisions(self):
        return [None] * (self.npartitions + 1)

    def _layer(self):
        chunksize = int(math.ceil(len(self.frame) / self.npartitions))
        locations = list(range(0, len(self.frame), chunksize)) + [len(self.frame)]
        return {
            (self._name, i): self.frame.iloc[start:stop]
            for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
        }

    def __str__(self):
        return "df"

    __repr__ = __str__


@normalize_token.register(API)
def normalize_expression(expr):
    return expr._name


from dask_match.reductions import Count, Max, Min, Mode, Size, Sum
