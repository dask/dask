import functools
from collections import namedtuple

import numpy as np
from dask.dataframe.tseries.resample import _resample_bin_and_out_divs, _resample_series

from dask_expr._collection import new_collection
from dask_expr._expr import Blockwise, Expr, Projection
from dask_expr._repartition import Repartition

BlockwiseDep = namedtuple(typename="BlockwiseDep", field_names=["iterable"])


class ResampleReduction(Expr):
    _parameters = [
        "frame",
        "rule",
        "kwargs",
        "fill_value",
        "how_args",
        "how_kwargs",
    ]
    _defaults = {
        "closed": None,
        "label": None,
        "fill_value": np.nan,
        "kwargs": None,
        "how_args": (),
        "how_kwargs": None,
    }
    how = None

    @functools.cached_property
    def npartitions(self):
        return self.frame.npartitions

    def _divisions(self):
        return self._resample_divisions[0]

    @functools.cached_property
    def _meta(self):
        return getattr(self.frame._meta.resample(self.rule, **self.kwargs), self.how)()

    @functools.cached_property
    def kwargs(self):
        return {} if self.operand("kwargs") is None else self.operand("kwargs")

    @functools.cached_property
    def _resample_divisions(self):
        return _resample_bin_and_out_divs(
            self.frame.divisions, self.rule, **self.kwargs
        )

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])

    def _lower(self):
        partitioned = Repartition(
            self.frame, new_divisions=self._resample_divisions[0], force=True
        )
        output_divisions = self._resample_divisions[1]
        return ResampleAggregation(
            partitioned,
            BlockwiseDep(output_divisions[:-1]),
            BlockwiseDep(output_divisions[1:]),
            BlockwiseDep(["left"] * (len(output_divisions[1:]) - 1) + [None]),
            self.rule,
            self.kwargs,
            self.how,
            self.fill_value,
            list(self.how_args),
            self.kwargs,
        )


class ResampleAggregation(Blockwise):
    _parameters = [
        "frame",
        "divisions_left",
        "divisions_right",
        "closed",
        "rule",
        "kwargs",
        "how",
        "fill_value",
        "how_args",
        "how_kwargs",
    ]
    operation = staticmethod(_resample_series)

    @functools.cached_property
    def _meta(self):
        return getattr(self.frame._meta.resample(self.rule, **self.kwargs), self.how)()

    def _blockwise_arg(self, arg, i):
        if isinstance(arg, BlockwiseDep):
            return arg.iterable[i]
        return super()._blockwise_arg(arg, i)


class ResampleCount(ResampleReduction):
    how = "count"


class Resampler:
    """Aggregate using one or more operations

    The purpose of this class is to expose an API similar
    to Pandas' `Resampler` for dask-expr
    """

    def __init__(self, obj, rule, **kwargs):
        if obj.divisions[0] is None:
            msg = (
                "Can only resample dataframes with known divisions\n"
                "See https://docs.dask.org/en/latest/dataframe-design.html#partitions\n"
                "for more information."
            )
            raise ValueError(msg)
        self.obj = obj
        self.rule = rule
        self.kwargs = kwargs

    def _single_agg(
        self,
        expr_cls,
    ):
        return new_collection(
            expr_cls(
                self.obj.expr,
                self.rule,
                self.kwargs,
            )
        )

    def count(self):
        return self._single_agg(ResampleCount)
