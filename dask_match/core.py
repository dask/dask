import math
import numbers
import operator

import pandas as pd
import toolz
from dask.base import DaskMethodsMixin, named_schedulers, normalize_token, tokenize
from dask.dataframe.core import is_series_like
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


class API(Operation, DaskMethodsMixin, metaclass=_APIMeta):
    commutative = False
    associative = False
    _parameters = []
    _defaults = {}

    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )

    @classmethod
    def normalize(cls, *args, **kwargs):
        return args, kwargs

    def __getattr__(self, key):
        if key in type(self)._parameters:
            idx = type(self)._parameters.index(key)
            return self.operands[idx]
        else:
            return object.__getattribute__(self, key)

    def __setattr__(self, key, value):
        if key in type(self)._parameters:
            idx = type(self)._parameters.index(key)
            self.operands[idx] = value
        else:
            object.__setattr__(self, key, value)

    def __getitem__(self, columns):
        return Projection(self, columns)

    def __add__(self, other):
        return Add(self, other)

    def __sub__(self, other):
        return Sub(self, other)

    def __mul__(self, other):
        return Mul(self, other)

    def __radd__(self, other):
        return Add(other, self)

    def __rsub__(self, other):
        return Sub(other, self)

    def __rmul__(self, other):
        return Mul(other, self)

    def sum(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Sum(self, skipna, level, numeric_only, min_count)

    def max(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Max(self, skipna, level, numeric_only, min_count)

    def min(self, skipna=True, level=None, numeric_only=None, min_count=0):
        return Min(self, skipna, level, numeric_only, min_count)

    @property
    def divisions(self):
        if "divisions" in self._parameters:
            idx = self._parameters.index("divisions")
            return self.operands[idx]
        return tuple(self._divisions())

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
        return toolz.first, ()

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
            (self._name, i): (self.operation,)
            + tuple(
                (operand._name, i) if isinstance(operand, API) else operand
                for operand in self.operands[1:]
            )
            for i in range(self.divisions)
        }


class Elemwise(Blockwise):
    pass


class Filter(Blockwise):
    pass


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


class Add(Binop):
    operation = operator.add

    def __str__(self):
        return "{} + {}".format(*self.operands)


class Mul(Binop):
    operation = operator.mul

    def __str__(self):
        return "{} * {}".format(*self.operands)


class Sub(Binop):
    operation = operator.sub

    def __str__(self):
        return "{} - {}".format(*self.operands)


class Reduction(API):
    arity = Arity.variadic
    chunk = None
    aggregate = None
    chunk_kwargs = {}
    aggregate_kwargs = {}

    _defaults = {
        "skipna": True,
        "level": None,
        "numeric_only": None,
        "min_count": 0,
    }

    def _layer(self):
        d = {
            (self._name + "-chunk", i): (
                apply,
                self.chunk,
                [(self.frame._name, i)],
                self.chunk_kwargs,
            )
            for i in range(self.frame.npartitions)
        }
        d[(self._name, 0)] = (
            apply,
            self.aggregate,
            [[(self._name + "-chunk", i) for i in range(self.frame.npartitions)]],
            self.aggregate_kwargs,
        )
        return d

    def _divisions(self):
        return [None, None]


class Sum(Reduction):
    _parameters = ["frame", "skipna", "level", "numeric_only", "min_count"]
    chunk = M.sum

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
            level=self.level,
            numeric_only=self.numeric_only,
            min_count=self.min_count,
        )

    @staticmethod
    def aggregate(results: list, **kwargs):
        if isinstance(results[0], numbers.Number):
            return sum(results)
        else:
            return pd.concat(results, axis=0).sum(**kwargs)

    @property
    def _meta(self):
        return self.frame._meta.sum(**self.chunk_kwargs)


class Max(Reduction):
    _parameters = ["frame", "skipna", "numeric_only"]
    chunk = M.max

    @staticmethod
    def aggregate(results: list):
        if isinstance(results[0], (pd.DataFrame, pd.Series)):
            return pd.concat(
                [r.to_frame().T if is_series_like(r) else r for r in results], axis=0
            ).max()
        else:
            return max(results)

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
            numeric_only=self.numeric_only,
        )

    @property
    def _meta(self):
        return self.frame._meta.max(**self.chunk_kwargs)


class Min(Max):
    chunk = M.min

    @staticmethod
    def aggregate(results: list):
        if isinstance(results[0], (pd.DataFrame, pd.Series)):
            return pd.concat(
                [r.to_frame().T if is_series_like(r) else r for r in results], axis=0
            ).min()
        else:
            return min(results)


class IO(API):
    pass


class ReadParquet(IO):
    _parameters = ["filename", "columns", "filters"]
    _defaults = {"columns": None, "filters": None}


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


@normalize_token.register(API)
def normalize_expression(expr):
    return expr._name
