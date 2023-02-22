import pandas as pd
import toolz
from dask.dataframe.core import _concat, is_series_like
from dask.utils import M, apply
from matchpy import Pattern, ReplacementRule, Wildcard

from dask_match.core import Expr


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
    split_every = 0
    chunk_kwargs = {}
    combine_kwargs = {}
    aggregate_kwargs = {}

    def __dask_postcompute__(self):
        return toolz.first, ()

    def _layer(self):
        # Normalize functions in case not all are defined
        chunk = self.chunk
        chunk_kwargs = self.chunk_kwargs

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
                toolz.partition_all(self.split_every or len(keys), keys)
            ):
                batch = list(batch)
                if combine_kwargs:
                    d[self._name, j, i] = (apply, combine, [batch], self.combine_kwargs)
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
        meta = self.frame._meta
        meta = self.chunk(meta, **self.chunk_kwargs)
        meta = self.combine([meta], **self.combine_kwargs)
        meta = self.aggregate([meta], **self.aggregate_kwargs)
        return meta

    def _divisions(self):
        return [None, None]


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
        "level": None,
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
        params = {param: getattr(self, param) for param in self._parameters[1:]}
        s = ", ".join(
            k + "=" + repr(v) for k, v in params.items() if v != self._defaults.get(k)
        )
        base = str(self.frame)
        if " " in base:
            base = "(" + base + ")"
        return f"{base}.{self.__class__.__name__.lower()}({s})"


class Sum(Reduction):
    _parameters = ["frame", "skipna", "level", "numeric_only", "min_count"]
    reduction_chunk = M.sum

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
            level=self.level,
            numeric_only=self.numeric_only,
            min_count=self.min_count,
        )

    @property
    def _meta(self):
        return self.frame._meta.sum(**self.chunk_kwargs)

    @classmethod
    def _replacement_rules(cls):
        a, b, c, d, e, f = map(Wildcard.dot, "abcdef")
        yield ReplacementRule(
            Pattern(Sum(a, b, c, d, e)[f]),
            lambda a, b, c, d, e, f: Sum(a[f], b, c, d, e),
        )


class Max(Reduction):
    _parameters = ["frame", "skipna"]
    reduction_chunk = M.max

    @property
    def chunk_kwargs(self):
        return dict(
            skipna=self.skipna,
        )

    @property
    def _meta(self):
        return self.frame._meta.max(**self.chunk_kwargs)

    @classmethod
    def _replacement_rules(cls):
        df = Wildcard.dot("df")
        skipna = Wildcard.dot("skipna")
        columns = Wildcard.dot("columns")

        yield ReplacementRule(
            Pattern(Max(df, skipna=skipna)[columns]),
            lambda df, skipna, columns: df[columns].max(skipna=skipna),
        )


class Size(Reduction):
    reduction_chunk = staticmethod(lambda df: df.size)
    reduction_aggregate = sum


class Count(Reduction):
    _parameters = ["frame"]
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
