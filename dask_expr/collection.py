import functools

import numpy as np
from dask.base import DaskMethodsMixin, named_schedulers
from dask.dataframe.core import (
    _concat,
    is_dataframe_like,
    is_index_like,
    is_series_like,
)
from dask.utils import IndexCallable
from fsspec.utils import stringify_path
from tlz import first

from dask_expr import expr
from dask_expr.expr import no_default
from dask_expr.repartition import Repartition

#
# Utilities to wrap Expr API
# (Helps limit boiler-plate code in collection APIs)
#


def _wrap_expr_api(*args, wrap_api=None, **kwargs):
    # Use Expr API, but convert to/from Expr objects
    assert wrap_api is not None
    result = wrap_api(
        *[arg.expr if isinstance(arg, FrameBase) else arg for arg in args],
        **kwargs,
    )
    if isinstance(result, expr.Expr):
        return new_collection(result)
    return result


def _wrap_expr_op(self, other, op=None):
    # Wrap expr operator
    assert op is not None
    if isinstance(other, FrameBase):
        other = other.expr
    return new_collection(getattr(self.expr, op)(other))


#
# Collection classes
#


class FrameBase(DaskMethodsMixin):
    """Base class for Expr-backed Collections"""

    __dask_scheduler__ = staticmethod(
        named_schedulers.get("threads", named_schedulers["sync"])
    )
    __dask_optimize__ = staticmethod(lambda dsk, keys, **kwargs: dsk)

    def __init__(self, expr):
        self._expr = expr

    @property
    def expr(self):
        return self._expr

    @property
    def _meta(self):
        return self.expr._meta

    @property
    def size(self):
        return new_collection(self.expr.size)

    def __reduce__(self):
        return new_collection, (self._expr,)

    def __getitem__(self, other):
        if isinstance(other, FrameBase):
            return new_collection(self.expr.__getitem__(other.expr))
        return new_collection(self.expr.__getitem__(other))

    def __dask_graph__(self):
        out = self.expr
        out = out.simplify()
        return out.__dask_graph__()

    def __dask_keys__(self):
        out = self.expr
        out = out.simplify()
        return out.__dask_keys__()

    def simplify(self):
        return new_collection(self.expr.simplify())

    @property
    def dask(self):
        return self.__dask_graph__()

    def __dask_postcompute__(self):
        return _concat, ()

    def __dask_postpersist__(self):
        return from_graph, (self._meta, self.divisions, self._name)

    def __getattr__(self, key):
        try:
            # Prioritize `FrameBase` attributes
            return object.__getattribute__(self, key)
        except AttributeError as err:
            try:
                # Fall back to `expr` API
                # (Making sure to convert to/from Expr)
                val = getattr(self.expr, key)
                if callable(val):
                    return functools.partial(_wrap_expr_api, wrap_api=val)
                return val
            except AttributeError:
                # Raise original error
                raise err

    @property
    def index(self):
        return new_collection(self.expr.index)

    def head(self, n=5, compute=True):
        out = new_collection(expr.Head(self.expr, n=n))
        if compute:
            out = out.compute()
        return out

    def copy(self):
        """Return a copy of this object"""
        return new_collection(self.expr)

    def _partitions(self, index):
        # Used by `partitions` for partition-wise slicing

        # Convert index to list
        if isinstance(index, int):
            index = [index]
        index = np.arange(self.npartitions, dtype=object)[index].tolist()

        # Check that selection makes sense
        assert set(index).issubset(range(self.npartitions))

        # Return selected partitions
        return new_collection(expr.Partitions(self.expr, index))

    @property
    def partitions(self):
        """Partition-wise slicing of a collection

        Examples
        --------
        >>> df.partitions[0]
        >>> df.partitions[:3]
        >>> df.partitions[::10]
        """
        return IndexCallable(self._partitions)

    def shuffle(
        self,
        index: str | list,
        ignore_index: bool = False,
        npartitions: int | None = None,
        backend: str | None = None,
        **options,
    ):
        """Shuffle a collection by column names

        Parameters
        ----------
        index:
            Column names to shuffle by.
        ignore_index: optional
            Whether to ignore the index. Default is ``False``.
        npartitions: optional
            Number of output partitions. The partition count will
            be preserved by default.
        backend: optional
            Desired shuffle backend. Default chosen at optimization time.
        **options: optional
            Algorithm-specific options.
        """
        from dask_expr.shuffle import Shuffle

        # Preserve partition count by default
        npartitions = npartitions or self.npartitions

        # Returned shuffled result
        return new_collection(
            Shuffle(
                self.expr,
                index,
                npartitions,
                ignore_index,
                backend,
                options,
            )
        )

    def map_partitions(
        self,
        func,
        *args,
        meta=no_default,
        enforce_metadata=True,
        transform_divisions=True,
        align_dataframes=False,
        **kwargs,
    ):
        """Apply a Python function to each partition

        Parameters
        ----------
        func : function
            Function applied to each partition.
        args, kwargs :
            Arguments and keywords to pass to the function. Arguments and
            keywords may contain ``FrameBase`` or regular python objects.
            DataFrame-like args (both dask and pandas) must have the same
            number of partitions as ``self` or comprise a single partition.
            Key-word arguments, Single-partition arguments, and general
            python-object argments will be broadcasted to all partitions.
        enforce_metadata : bool, default True
            Whether to enforce at runtime that the structure of the DataFrame
            produced by ``func`` actually matches the structure of ``meta``.
            This will rename and reorder columns for each partition, and will
            raise an error if this doesn't work, but it won't raise if dtypes
            don't match.
        transform_divisions : bool, default True
            Whether to apply the function onto the divisions and apply those
            transformed divisions to the output.
        meta : Any, optional
            DataFrame object representing the schema of the expected result.
        """

        if align_dataframes:
            # TODO: Handle alignment?
            # Perhaps we only handle the case that all `Expr` operands
            # have the same number of partitions or can be broadcasted
            # within `MapPartitions`. If so, the `map_partitions` API
            # will need to call `Repartition` on operands that are not
            # aligned with `self.expr`.
            raise NotImplementedError()
        new_expr = expr.MapPartitions(
            self.expr,
            func,
            meta,
            enforce_metadata,
            transform_divisions,
            kwargs,
            *[arg.expr if isinstance(arg, FrameBase) else arg for arg in args],
        )
        return new_collection(new_expr)

    def repartition(self, npartitions=None, divisions=None, force=False):
        """Repartition a collection

        Exactly one of `divisions` or `npartitions` should be specified.
        A ``ValueError`` will be raised when that is not the case.

        Parameters
        ----------
        npartitions : int, optional
            Approximate number of partitions of output. The number of
            partitions used may be slightly lower than npartitions depending
            on data distribution, but will never be higher.
        divisions : list, optional
            The "dividing lines" used to split the dataframe into partitions.
            For ``divisions=[0, 10, 50, 100]``, there would be three output partitions,
            where the new index contained [0, 10), [10, 50), and [50, 100), respectively.
            See https://docs.dask.org/en/latest/dataframe-design.html#partitions.
        force : bool, default False
            Allows the expansion of the existing divisions.
            If False then the new divisions' lower and upper bounds must be
            the same as the old divisions'.
        """

        if sum([divisions is not None, npartitions is not None]) != 1:
            raise ValueError(
                "Please provide exactly one of the ``npartitions=`` or "
                "``divisions=`` keyword arguments."
            )

        return new_collection(Repartition(self.expr, npartitions, divisions, force))


# Add operator attributes
for op in [
    "__add__",
    "__radd__",
    "__sub__",
    "__rsub__",
    "__mul__",
    "__rmul__",
    "__truediv__",
    "__rtruediv__",
    "__lt__",
    "__rlt__",
    "__gt__",
    "__rgt__",
    "__le__",
    "__rle__",
    "__ge__",
    "__rge__",
    "__eq__",
    "__ne__",
]:
    setattr(FrameBase, op, functools.partialmethod(_wrap_expr_op, op=op))


class DataFrame(FrameBase):
    """DataFrame-like Expr Collection"""

    def assign(self, **pairs):
        result = self
        for k, v in pairs.items():
            if not isinstance(v, Series):
                raise TypeError(f"Column assignment doesn't support type {type(v)}")
            if not isinstance(k, str):
                raise TypeError(f"Column name cannot be type {type(k)}")
            result = new_collection(expr.Assign(result.expr, k, v.expr))
        return result

    def __setitem__(self, key, value):
        out = self.assign(**{key: value})
        self._expr = out._expr

    def __delitem__(self, key):
        columns = [c for c in self.columns if c != key]
        out = self[columns]
        self._expr = out._expr

    def __getattr__(self, key):
        try:
            # Prioritize `DataFrame` attributes
            return object.__getattribute__(self, key)
        except AttributeError as err:
            try:
                # Check if key is in columns if key
                # is not a normal attribute
                if key in self.expr._meta.columns:
                    return Series(self.expr[key])
                raise err
            except AttributeError:
                # Fall back to `BaseFrame.__getattr__`
                return super().__getattr__(key)

    def __repr__(self):
        return f"<dask_expr.expr.DataFrame: expr={self.expr}>"


class Series(FrameBase):
    """Series-like Expr Collection"""

    @property
    def name(self):
        return self.expr._meta.name

    def __repr__(self):
        return f"<dask_expr.expr.Series: expr={self.expr}>"


class Index(Series):
    """Index-like Expr Collection"""

    def __repr__(self):
        return f"<dask_expr.expr.Index: expr={self.expr}>"


class Scalar(FrameBase):
    """Scalar Expr Collection"""

    def __repr__(self):
        return f"<dask_expr.expr.Scalar: expr={self.expr}>"

    def __dask_postcompute__(self):
        return first, ()


def new_collection(expr):
    """Create new collection from an expr"""

    meta = expr._meta
    if is_dataframe_like(meta):
        return DataFrame(expr)
    elif is_series_like(meta):
        return Series(expr)
    elif is_index_like(meta):
        return Index(expr)
    else:
        return Scalar(expr)


def optimize(collection, fuse=True):
    return new_collection(expr.optimize(collection.expr, fuse=fuse))


def from_pandas(*args, **kwargs):
    from dask_expr.io.io import FromPandas

    return new_collection(FromPandas(*args, **kwargs))


def from_graph(*args, **kwargs):
    from dask_expr.io.io import FromGraph

    return new_collection(FromGraph(*args, **kwargs))


def read_csv(*args, **kwargs):
    from dask_expr.io.csv import ReadCSV

    return new_collection(ReadCSV(*args, **kwargs))


def read_parquet(
    path=None,
    columns=None,
    filters=(),
    categories=None,
    index=None,
    storage_options=None,
    dtype_backend=None,
    calculate_divisions=False,
    ignore_metadata_file=False,
    metadata_task_size=None,
    split_row_groups="infer",
    blocksize="default",
    aggregate_files=None,
    parquet_file_extension=(".parq", ".parquet", ".pq"),
    filesystem="fsspec",
    **kwargs,
):
    from dask_expr.io.parquet import ReadParquet, _list_columns

    if hasattr(path, "name"):
        path = stringify_path(path)

    kwargs["dtype_backend"] = dtype_backend

    return new_collection(
        ReadParquet(
            path,
            columns=_list_columns(columns),
            filters=filters,
            categories=categories,
            index=index,
            storage_options=storage_options,
            calculate_divisions=calculate_divisions,
            ignore_metadata_file=ignore_metadata_file,
            metadata_task_size=metadata_task_size,
            split_row_groups=split_row_groups,
            blocksize=blocksize,
            aggregate_files=aggregate_files,
            parquet_file_extension=parquet_file_extension,
            filesystem=filesystem,
            kwargs=kwargs,
        )
    )
