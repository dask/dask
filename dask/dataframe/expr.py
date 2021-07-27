import operator

from toolz import first

from .. import threaded
from ..base import is_dask_collection, tokenize
from ..blockwise import blockwise
from ..expr import Expr
from ..utils import funcname
from .core import finalize


class ExprFrame(Expr):
    def __dask_keys__(self):
        return [(self._name, i) for i in range(self.npartitions)]

    __dask_scheduler__ = staticmethod(threaded.get)

    def __dask_postcompute__(self):
        return finalize, ()

    @property
    def npartitions(self):
        """Return number of partitions"""
        return len(self.divisions) - 1

    @property
    def columns(self):
        return self._meta.columns

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: UnaryOp(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: BinOp(op, other, self)
        else:
            return lambda self, other: BinOp(op, self, other)

    def __getitem__(self, other):
        return Columns(self, other)


class TimeSeries(ExprFrame):
    _args = ()
    _inputs = ()

    def __init__(self, **kwargs):
        # Cheating here.  Create a normal dataframe, rip out graph
        import dask

        df = dask.datasets.timeseries(**kwargs)
        self._layer = dict(df.__dask_graph__())
        self._meta = df._meta
        self.divisions = df.divisions
        self._name = df.__dask_layers__()[0]

    def _generate_dask_layer(self):
        return self._layer


class BinOp(ExprFrame):
    _args = ("op", "left", "right")

    def __init__(self, op, left, right):
        self.op = op
        self.left = left
        self.right = right
        self._inputs = []
        if is_dask_collection(left):
            self._inputs.append("left")
        if is_dask_collection(right):
            self._inputs.append("right")

        self._name = funcname(op) + "-" + tokenize(left, right)
        self._meta = op(
            left._meta if is_dask_collection(left) else left,
            right._meta if is_dask_collection(right) else right,
        )

    @property
    def divisions(self):
        return first(self._dependencies).divisions

    def _generate_dask_layer(self):
        numblocks = {}
        if is_dask_collection(self.left):
            numblocks[self.left._name] = (self.left.npartitions,)
        if is_dask_collection(self.right):
            numblocks[self.right._name] = (self.right.npartitions,)

        return blockwise(
            self.op,
            self._name,
            "i",
            self.left._name if is_dask_collection(self.left) else self.left,
            "i" if is_dask_collection(self.left) else None,
            self.right._name if is_dask_collection(self.right) else self.right,
            "i" if is_dask_collection(self.right) else None,
            numblocks=numblocks,
        )


class UnaryOp(ExprFrame):
    _args = ("op", "arg")
    _inputs = ("arg",)

    def __init__(self, op, arg):
        self.arg = arg
        self._name = funcname(op) + "-" + tokenize(arg)
        self._meta = op(arg._meta)

    @property
    def divisions(self):
        return first(self._dependencies).divisions

    def _generate_dask_layer(self):
        return blockwise(
            self.op,
            self._name,
            "i",
            self.arg._name,
            "i",
            numblocks={self.arg._name: (self.arg.npartitions,)},
        )


class Columns(BinOp):
    _args = ("arg", "index")
    _inputs = ("arg",)

    def __init__(self, arg, index):
        BinOp.__init__(self, operator.getitem, arg, index)
        self._inputs = ("arg",)
        self.arg = arg
        self.index = index


# bind operators
for op in [
    operator.abs,
    operator.add,
    operator.and_,
    operator.eq,
    operator.gt,
    operator.ge,
    operator.inv,
    operator.lt,
    operator.le,
    operator.mod,
    operator.mul,
    operator.ne,
    operator.neg,
    operator.or_,
    operator.pow,
    operator.sub,
    operator.truediv,
    operator.floordiv,
    operator.xor,
]:
    ExprFrame._bind_operator(op)
