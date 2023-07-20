import pandas as pd
import toolz
from dask.dataframe import hyperloglog, methods
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

from dask_expr._expr import Elemwise, Expr, Index, Projection


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


class Unique(ApplyConcatApply):
    _parameters = ["frame"]
    chunk = staticmethod(lambda x, **kwargs: methods.unique(x, **kwargs))
    aggregate_func = methods.unique

    @property
    def _meta(self):
        return self.chunk(
            meta_nonempty(self.frame._meta), series_name=self.frame._meta.name
        )

    @property
    def chunk_kwargs(self):
        return {"series_name": self._meta.name}

    @property
    def aggregate_kwargs(self):
        return self.chunk_kwargs

    @classmethod
    def combine(cls, inputs: list, **kwargs):
        return _concat(inputs)

    @classmethod
    def aggregate(cls, inputs: list, **kwargs):
        df = _concat(inputs)
        return cls.aggregate_func(df, **kwargs)

    def _simplify_up(self, parent):
        return

    def __dask_postcompute__(self):
        return _concat, ()

    def _divisions(self):
        return [None, None]


class DropDuplicates(Unique):
    _parameters = ["frame", "subset", "ignore_index"]
    _defaults = {"subset": None, "ignore_index": False}
    chunk = M.drop_duplicates
    aggregate_func = M.drop_duplicates

    @property
    def _meta(self):
        return self.chunk(meta_nonempty(self.frame._meta), **self.chunk_kwargs)

    def _subset_kwargs(self):
        if is_series_like(self.frame._meta):
            return {}
        return {"subset": self.subset}

    @property
    def chunk_kwargs(self):
        return {"ignore_index": self.ignore_index, **self._subset_kwargs()}

    def _simplify_up(self, parent):
        if self.subset is not None:
            columns = set(parent.columns).union(self.subset)
            if columns == set(self.frame.columns):
                # Don't add unnecessary Projections, protects against loops
                return

            return type(parent)(
                type(self)(self.frame[sorted(columns)], *self.operands[1:]),
                *parent.operands[1:],
            )


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
        "numeric_only": False,
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
        if self.ndim == 0:
            return (None, None)
        return (min(self.frame.columns), max(self.frame.columns))

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
        from dask_expr.io.io import IO

        # We introduce Index nodes sometimes.  We special case around them.
        if isinstance(self.frame, Index) and isinstance(self.frame.frame, Elemwise):
            return Len(self.frame.frame)

        # Pass through Elemwises, unless we just introduced an Index
        if isinstance(self.frame, Elemwise) and not isinstance(self.frame, Index):
            child = max(self.frame.dependencies(), key=lambda expr: expr.npartitions)
            return Len(child)

        # Let the child handle it.  They often know best
        if isinstance(self.frame, IO):
            return self

        # Drop all of the columns, just pass through the index
        if len(self.frame.columns):
            return Len(self.frame.index)

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


class Var(Reduction):
    # Uses the parallel version of Welford's online algorithm (Chan 79')
    # (http://i.stanford.edu/pub/cstr/reports/cs/tr/79/773/CS-TR-79-773.pdf)
    _parameters = ["frame", "skipna", "ddof", "numeric_only"]
    _defaults = {"skipna": True, "ddof": 1, "numeric_only": False}

    @property
    def _meta(self):
        return make_meta(
            meta_nonempty(self.frame._meta).var(
                skipna=self.skipna, numeric_only=self.numeric_only
            )
        )

    @property
    def chunk_kwargs(self):
        return dict(skipna=self.skipna, numeric_only=self.numeric_only)

    @property
    def combine_kwargs(self):
        return {}

    @property
    def aggregate_kwargs(self):
        return dict(ddof=self.ddof)

    @classmethod
    def reduction_chunk(cls, x, skipna=True, numeric_only=False):
        kwargs = {"numeric_only": numeric_only} if is_dataframe_like(x) else {}
        if skipna:
            n = x.count(**kwargs)
            kwargs["skipna"] = skipna
            avg = x.mean(**kwargs)
        else:
            # Not skipping nulls, so might as well
            # avoid the full `count` operation
            n = len(x)
            kwargs["skipna"] = skipna
            avg = x.sum(**kwargs) / n
        m2 = ((x - avg) ** 2).sum(**kwargs)
        return n, avg, m2

    @classmethod
    def reduction_combine(cls, parts):
        n, avg, m2 = parts[0]
        for i in range(1, len(parts)):
            n_a, avg_a, m2_a = n, avg, m2
            n_b, avg_b, m2_b = parts[i]
            n = n_a + n_b
            avg = (n_a * avg_a + n_b * avg_b) / n
            delta = avg_b - avg_a
            m2 = m2_a + m2_b + delta**2 * n_a * n_b / n
        return n, avg, m2

    @classmethod
    def reduction_aggregate(cls, vals, ddof=1):
        vals = cls.reduction_combine(vals)
        n, _, m2 = vals
        return m2 / (n - ddof)


class Mean(Reduction):
    _parameters = ["frame", "skipna", "numeric_only"]
    _defaults = {"skipna": True, "numeric_only": False}

    @property
    def _meta(self):
        return (
            self.frame._meta.sum(skipna=self.skipna, numeric_only=self.numeric_only) / 2
        )

    def _lower(self):
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


class NuniqueApprox(Reduction):
    _parameters = ["frame", "b"]
    reduction_chunk = hyperloglog.compute_hll_array
    reduction_combine = hyperloglog.reduce_state
    reduction_aggregate = hyperloglog.estimate_count

    @property
    def _meta(self):
        return 1.0

    @property
    def chunk_kwargs(self):
        return {"b": self.b}

    @property
    def combine_kwargs(self):
        return self.chunk_kwargs

    @property
    def aggregate_kwargs(self):
        return self.chunk_kwargs


class ReductionConstantDim(Reduction):
    """
    Some reductions reduce the number of rows in your object but keep the original
    dimension, e.g. a DataFrame stays a DataFrame instead of getting reduced to
    a Series.
    """

    @classmethod
    def chunk(cls, df, **kwargs):
        return cls.reduction_chunk(df, **kwargs)

    @classmethod
    def combine(cls, inputs: list, **kwargs):
        func = cls.reduction_combine or cls.reduction_aggregate or cls.reduction_chunk
        df = _concat(inputs)
        return func(df, **kwargs)

    def _divisions(self):
        # TODO: We can do better in some cases
        return (None, None)


class NLargest(ReductionConstantDim):
    _defaults = {"n": 5, "_columns": None}
    _parameters = ["frame", "n", "_columns"]
    reduction_chunk = M.nlargest
    reduction_aggregate = M.nlargest

    def _columns_kwarg(self):
        if self._columns is None:
            return {}
        return {"columns": self._columns}

    @property
    def chunk_kwargs(self):
        return {"n": self.n, **self._columns_kwarg()}

    @property
    def combine_kwargs(self):
        return self.chunk_kwargs

    @property
    def aggregate_kwargs(self):
        return self.chunk_kwargs


class NSmallest(NLargest):
    _parameters = ["frame", "n", "_columns"]
    reduction_chunk = M.nsmallest
    reduction_aggregate = M.nsmallest


class ValueCounts(ReductionConstantDim):
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

    @property
    def chunk_kwargs(self):
        return {"sort": self.sort, "ascending": self.ascending, "dropna": self.dropna}

    @property
    def aggregate_kwargs(self):
        return {**self.chunk_kwargs, "normalize": self.normalize}

    def _simplify_up(self, parent):
        # We are already a Series
        return


class MemoryUsage(Reduction):
    reduction_chunk = M.memory_usage
    reduction_aggregate = M.sum

    def _divisions(self):
        # TODO: We can do better, but not high priority
        return (None, None)


class MemoryUsageIndex(MemoryUsage):
    _parameters = ["frame", "deep"]
    _defaults = {"deep": False}

    @property
    def chunk_kwargs(self):
        return {"deep": self.deep}


class MemoryUsageFrame(MemoryUsage):
    _parameters = ["frame", "deep", "_index"]
    _defaults = {"deep": False, "_index": True}

    @property
    def chunk_kwargs(self):
        return {"deep": self.deep, "index": self._index}

    @property
    def combine_kwargs(self):
        return {"is_dataframe": is_dataframe_like(self.frame._meta)}

    @staticmethod
    def reduction_combine(x, is_dataframe):
        if is_dataframe:
            return x.groupby(x.index).sum()
        return x.sum()
