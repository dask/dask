import pandas as pd
import toolz
from dask.dataframe import methods
from dask.dataframe.core import (
    _concat,
    idxmaxmin_agg,
    idxmaxmin_chunk,
    idxmaxmin_combine,
    is_dataframe_like,
    is_series_like,
    make_meta,
    meta_nonempty,
)
from dask.utils import M, apply

from dask_expr.expr import Elemwise, Expr, Projection


class ApplyConcatApply(Expr):
    """Perform reduction-like operation on dataframes

    This pattern is commonly used for reductions, groupby-aggregations, and
    more.  It requires three methods to be implemented:

    -   `chunk`: applied to each input partition
    -   `combine`: applied to lists of intermediate partitions as they are
        combined in batches
    -   `aggregate`: applied at the end to finalize the computation

    These methods should be easy to serialize, and can take in keyword
    arguments defined in `chunks/combine/aggregate_kwargs`.

    In many cases people don't define all three functions.  In these cases
    combine takes from aggregate and aggregate takes from chunk.
    """

    _parameters = ["frame"]
    chunk = None
    combine = None
    aggregate = None
    chunk_kwargs = {}
    combine_kwargs = {}
    aggregate_kwargs = {}

    def __dask_postcompute__(self):
        return toolz.first, ()

    def _layer(self):
        # Normalize functions in case not all are defined
        chunk = self.chunk
        chunk_kwargs = self.chunk_kwargs
        split_every = getattr(self, "split_every", 0)

        if self.aggregate:
            aggregate = self.aggregate
            aggregate_kwargs = self.aggregate_kwargs
        else:
            aggregate = chunk
            aggregate_kwargs = chunk_kwargs

        if self.combine:
            combine = self.combine
            combine_kwargs = self.combine_kwargs
        else:
            combine = aggregate
            combine_kwargs = aggregate_kwargs

        d = {}
        keys = self.frame.__dask_keys__()

        # apply chunk to every input partition
        for i, key in enumerate(keys):
            if chunk_kwargs:
                d[self._name, 0, i] = (apply, chunk, [key], chunk_kwargs)
            else:
                d[self._name, 0, i] = (chunk, key)

        keys = list(d)
        j = 1

        # apply combine to batches of intermediate results
        while len(keys) > 1:
            new_keys = []
            for i, batch in enumerate(
                toolz.partition_all(split_every or len(keys), keys)
            ):
                batch = list(batch)
                if combine_kwargs:
                    d[self._name, j, i] = (apply, combine, [batch], combine_kwargs)
                else:
                    d[self._name, j, i] = (combine, batch)
                new_keys.append((self._name, j, i))
            j += 1
            keys = new_keys

        # apply aggregate to the final result
        d[self._name, 0] = (apply, aggregate, [keys], aggregate_kwargs)

        return d

    @property
    def _meta(self):
        meta = meta_nonempty(self.frame._meta)
        meta = self.chunk(meta, **self.chunk_kwargs)
        aggregate = self.aggregate or (lambda x: x)
        if self.combine:
            combine = self.combine
            combine_kwargs = self.combine_kwargs
        else:
            combine = aggregate
            combine_kwargs = self.aggregate_kwargs

        meta = combine([meta], **combine_kwargs)
        meta = aggregate([meta], **self.aggregate_kwargs)
        return make_meta(meta)

    def _divisions(self):
        return (None, None)


class Reduction(ApplyConcatApply):
    """A common pattern of apply concat apply

    Common reductions like sum/min/max/count/... have some shared code around
    `_concat` and so on.  This class inherits from `ApplyConcatApply` in order
    to leverage this shared structure.

    I wouldn't be surprised if there was a way to merge them both into a single
    abstraction in the future.

    This class implements `{chunk,combine,aggregate}` methods of
    `ApplyConcatApply` by depending on `reduction_{chunk,combine,aggregate}`
    methods.
    """

    _defaults = {
        "skipna": True,
        "numeric_only": None,
        "min_count": 0,
        "dropna": True,
    }
    reduction_chunk = None
    reduction_combine = None
    reduction_aggregate = None

    @classmethod
    def chunk(cls, df, **kwargs):
        out = cls.reduction_chunk(df, **kwargs)
        # Return a dataframe so that the concatenated version is also a dataframe
        return out.to_frame().T if is_series_like(out) else out

    @classmethod
    def combine(cls, inputs: list, **kwargs):
        func = cls.reduction_combine or cls.reduction_aggregate or cls.reduction_chunk
        df = _concat(inputs)
        out = func(df, **kwargs)
        # Return a dataframe so that the concatenated version is also a dataframe
        return out.to_frame().T if is_series_like(out) else out

    @classmethod
    def aggregate(cls, inputs, **kwargs):
        func = cls.reduction_aggregate or cls.reduction_chunk
        df = _concat(inputs)
        return func(df, **kwargs)

    def __dask_postcompute__(self):
        return toolz.first, ()

    def _divisions(self):
        return [None, None]

    def __str__(self):
        params = {param: self.operand(param) for param in self._parameters[1:]}
        s = ", ".join(
            k + "=" + repr(v) for k, v in params.items() if v != self._defaults.get(k)
        )
        base = str(self.frame)
        if " " in base:
            base = "(" + base + ")"
        return f"{base}.{self.__class__.__name__.lower()}({s})"

    def _simplify_up(self, parent):
        if isinstance(parent, Projection):
            return type(self)(self.frame[parent.operand("columns")], *self.operands[1:])


class Sum(Reduction):
    _parameters = ["frame", "skipna", "numeric_only", "min_count"]
    reduction_chunk = M.sum

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
            numeric_only=self.numeric_only,
            min_count=self.min_count,
        )


class Prod(Reduction):
    _parameters = ["frame", "skipna", "numeric_only", "min_count"]
    reduction_chunk = M.prod

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
            numeric_only=self.numeric_only,
            min_count=self.min_count,
        )


class Max(Reduction):
    _parameters = ["frame", "skipna"]
    reduction_chunk = M.max

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
        )


class Any(Reduction):
    _parameters = ["frame", "skipna"]
    reduction_chunk = M.any

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
        )


class All(Reduction):
    _parameters = ["frame", "skipna"]
    reduction_chunk = M.all

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
        )


class IdxMin(Reduction):
    _parameters = ["frame", "skipna", "numeric_only"]
    reduction_chunk = idxmaxmin_chunk
    reduction_combine = idxmaxmin_combine
    reduction_aggregate = idxmaxmin_agg
    _fn = "idxmin"

    @property
    def chunk_kwargs(self):
        # TODO: Add numeric_only after Dask release on May 26th
        return dict(skipna=self.skipna, fn=self._fn)

    @property
    def combine_kwargs(self):
        return dict(skipna=self.skipna, fn=self._fn)

    @property
    def aggregate_kwargs(self):
        return {**self.chunk_kwargs, "scalar": is_series_like(self.frame._meta)}


class IdxMax(IdxMin):
    _fn = "idxmax"


class Len(Reduction):
    reduction_chunk = staticmethod(len)
    reduction_aggregate = sum

    def _simplify_down(self):
        if isinstance(self.frame, Elemwise):
            child = max(self.frame.dependencies(), key=lambda expr: expr.npartitions)
            return Len(child)

    def _simplify_up(self, parent):
        return


class Size(Reduction):
    reduction_chunk = staticmethod(lambda df: df.size)
    reduction_aggregate = sum

    def _simplify_down(self):
        if is_dataframe_like(self.frame) and len(self.frame.columns) > 1:
            return len(self.frame.columns) * Len(self.frame)
        else:
            return Len(self.frame)

    def _simplify_up(self, parent):
        return


class NBytes(Reduction):
    # Only supported for Series objects
    reduction_chunk = lambda ser: ser.nbytes
    reduction_aggregate = sum


class Mean(Reduction):
    _parameters = ["frame", "skipna", "numeric_only"]
    _defaults = {"skipna": True, "numeric_only": None}

    @property
    def _meta(self):
        return (
            self.frame._meta.sum(skipna=self.skipna, numeric_only=self.numeric_only) / 2
        )

    def _simplify_down(self):
        return (
            self.frame.sum(skipna=self.skipna, numeric_only=self.numeric_only)
            / self.frame.count()
        )


class Count(Reduction):
    _parameters = ["frame", "numeric_only"]
    split_every = 16
    reduction_chunk = M.count

    @classmethod
    def reduction_aggregate(cls, df):
        return df.sum().astype("int64")


class Min(Max):
    reduction_chunk = M.min


class Mode(ApplyConcatApply):
    """

    Mode was a bit more complicated than class reductions, so we retreat back
    to ApplyConcatApply
    """

    _parameters = ["frame", "dropna"]
    _defaults = {"dropna": True}
    chunk = M.value_counts
    split_every = 16

    @classmethod
    def combine(cls, results: list[pd.Series]):
        df = _concat(results)
        out = df.groupby(df.index).sum()
        out.name = results[0].name
        return out

    @classmethod
    def aggregate(cls, results: list[pd.Series], dropna=None):
        [df] = results
        max = df.max(skipna=dropna)
        out = df[df == max].index.to_series().sort_values().reset_index(drop=True)
        out.name = results[0].name
        return out

    @property
    def chunk_kwargs(self):
        return {"dropna": self.dropna}

    @property
    def aggregate_kwargs(self):
        return {"dropna": self.dropna}


class ValueCounts(Reduction):
    _defaults = {
        "sort": None,
        "ascending": False,
        "dropna": True,
        "normalize": False,
    }

    _parameters = ["frame", "sort", "ascending", "dropna", "normalize"]
    reduction_chunk = M.value_counts
    reduction_aggregate = methods.value_counts_aggregate
    reduction_combine = methods.value_counts_combine

    @classmethod
    def chunk(cls, df, **kwargs):
        return cls.reduction_chunk(df, **kwargs)

    @classmethod
    def combine(cls, inputs: list, **kwargs):
        df = _concat(inputs)
        return cls.reduction_combine(df, **kwargs)

    @property
    def chunk_kwargs(self):
        return {"sort": self.sort, "ascending": self.ascending, "dropna": self.dropna}

    @property
    def aggregate_kwargs(self):
        return {**self.chunk_kwargs, "normalize": self.normalize}

    def _simplify_up(self, parent):
        # We are already a Series
        return
