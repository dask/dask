import os
from collections.abc import Iterable
from functools import partial
from math import ceil
from operator import getitem
from threading import Lock
from typing import Optional, Union

import numpy as np
import pandas as pd

import dask.array as da
from dask.base import tokenize
from dask.blockwise import BlockwiseDepDict, blockwise
from dask.dataframe.core import (
    DataFrame,
    Index,
    Series,
    _concat,
    _emulate,
    apply_and_enforce,
    has_parallel_type,
    new_dd_object,
)
from dask.dataframe.io.utils import DataFrameIOFunction
from dask.dataframe.shuffle import set_partition
from dask.dataframe.utils import (
    check_meta,
    insert_meta_param_description,
    is_series_like,
    make_meta,
)
from dask.delayed import delayed
from dask.highlevelgraph import HighLevelGraph
from dask.layers import DataFrameIOLayer
from dask.utils import M, _deprecated, funcname, is_arraylike

lock = Lock()


def _meta_from_array(x, columns=None, index=None, meta=None):
    """Create empty DataFrame or Series which has correct dtype"""

    if x.ndim > 2:
        raise ValueError(
            "from_array does not input more than 2D array, got"
            " array with shape %r" % (x.shape,)
        )

    if index is not None:
        if not isinstance(index, Index):
            raise ValueError("'index' must be an instance of dask.dataframe.Index")
        index = index._meta

    if meta is None:
        meta = pd.DataFrame()

    if getattr(x.dtype, "names", None) is not None:
        # record array has named columns
        if columns is None:
            columns = list(x.dtype.names)
        elif np.isscalar(columns):
            raise ValueError("For a struct dtype, columns must be a list.")
        elif not all(i in x.dtype.names for i in columns):
            extra = sorted(set(columns).difference(x.dtype.names))
            raise ValueError(f"dtype {x.dtype} doesn't have fields {extra}")
        fields = x.dtype.fields
        dtypes = [fields[n][0] if n in fields else "f8" for n in columns]
    elif x.ndim == 1:
        if np.isscalar(columns) or columns is None:
            return meta._constructor_sliced(
                [], name=columns, dtype=x.dtype, index=index
            )
        elif len(columns) == 1:
            return meta._constructor(
                np.array([], dtype=x.dtype), columns=columns, index=index
            )
        raise ValueError(
            "For a 1d array, columns must be a scalar or single element list"
        )
    else:
        if np.isnan(x.shape[1]):
            raise ValueError("Shape along axis 1 must be known")
        if columns is None:
            columns = list(range(x.shape[1])) if x.ndim == 2 else [0]
        elif len(columns) != x.shape[1]:
            raise ValueError(
                "Number of column names must match width of the array. "
                f"Got {len(columns)} names for {x.shape[1]} columns"
            )
        dtypes = [x.dtype] * len(columns)

    data = {c: np.array([], dtype=dt) for (c, dt) in zip(columns, dtypes)}
    return meta._constructor(data, columns=columns, index=index)


def from_array(x, chunksize=50000, columns=None, meta=None):
    """Read any sliceable array into a Dask Dataframe

    Uses getitem syntax to pull slices out of the array.  The array need not be
    a NumPy array but must support slicing syntax

        x[50000:100000]

    and have 2 dimensions:

        x.ndim == 2

    or have a record dtype:

        x.dtype == [('name', 'O'), ('balance', 'i8')]

    Parameters
    ----------
    x : array_like
    chunksize : int, optional
        The number of rows per partition to use.
    columns : list or string, optional
        list of column names if DataFrame, single string if Series
    meta : object, optional
        An optional `meta` parameter can be passed for dask
        to specify the concrete dataframe type to use for partitions of
        the Dask dataframe. By default, pandas DataFrame is used.

    Returns
    -------
    dask.DataFrame or dask.Series
        A dask DataFrame/Series
    """
    if isinstance(x, da.Array):
        return from_dask_array(x, columns=columns, meta=meta)

    meta = _meta_from_array(x, columns, meta=meta)

    divisions = tuple(range(0, len(x), chunksize))
    divisions = divisions + (len(x) - 1,)
    token = tokenize(x, chunksize, columns)
    name = "from_array-" + token

    dsk = {}
    for i in range(0, int(ceil(len(x) / chunksize))):
        data = (getitem, x, slice(i * chunksize, (i + 1) * chunksize))
        if is_series_like(meta):
            dsk[name, i] = (type(meta), data, None, meta.dtype, meta.name)
        else:
            dsk[name, i] = (type(meta), data, None, meta.columns)
    return new_dd_object(dsk, name, meta, divisions)


def from_pandas(
    data: Union[pd.DataFrame, pd.Series],
    npartitions: Optional[int] = None,
    chunksize: Optional[int] = None,
    sort: bool = True,
    name: Optional[str] = None,
) -> DataFrame:
    """
    Construct a Dask DataFrame from a Pandas DataFrame

    This splits an in-memory Pandas dataframe into several parts and constructs
    a dask.dataframe from those parts on which Dask.dataframe can operate in
    parallel.  By default, the input dataframe will be sorted by the index to
    produce cleanly-divided partitions (with known divisions).  To preserve the
    input ordering, make sure the input index is monotonically-increasing. The
    ``sort=False`` option will also avoid reordering, but will not result in
    known divisions.

    Note that, despite parallelism, Dask.dataframe may not always be faster
    than Pandas.  We recommend that you stay with Pandas for as long as
    possible before switching to Dask.dataframe.

    Parameters
    ----------
    data : pandas.DataFrame or pandas.Series
        The DataFrame/Series with which to construct a Dask DataFrame/Series
    npartitions : int, optional
        The number of partitions of the index to create. Note that depending on
        the size and index of the dataframe, the output may have fewer
        partitions than requested.
    chunksize : int, optional
        The number of rows per index partition to use.
    sort: bool
        Sort the input by index first to obtain cleanly divided partitions
        (with known divisions).  If False, the input will not be sorted, and
        all divisions will be set to None. Default is True.
    name: string, optional
        An optional keyname for the dataframe.  Defaults to hashing the input

    Returns
    -------
    dask.DataFrame or dask.Series
        A dask DataFrame/Series partitioned along the index

    Examples
    --------
    >>> from dask.dataframe import from_pandas
    >>> df = pd.DataFrame(dict(a=list('aabbcc'), b=list(range(6))),
    ...                   index=pd.date_range(start='20100101', periods=6))
    >>> ddf = from_pandas(df, npartitions=3)
    >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
    (Timestamp('2010-01-01 00:00:00', freq='D'),
     Timestamp('2010-01-03 00:00:00', freq='D'),
     Timestamp('2010-01-05 00:00:00', freq='D'),
     Timestamp('2010-01-06 00:00:00', freq='D'))
    >>> ddf = from_pandas(df.a, npartitions=3)  # Works with Series too!
    >>> ddf.divisions  # doctest: +NORMALIZE_WHITESPACE
    (Timestamp('2010-01-01 00:00:00', freq='D'),
     Timestamp('2010-01-03 00:00:00', freq='D'),
     Timestamp('2010-01-05 00:00:00', freq='D'),
     Timestamp('2010-01-06 00:00:00', freq='D'))

    Raises
    ------
    TypeError
        If something other than a ``pandas.DataFrame`` or ``pandas.Series`` is
        passed in.

    See Also
    --------
    from_array : Construct a dask.DataFrame from an array that has record dtype
    read_csv : Construct a dask.DataFrame from a CSV file
    """
    if isinstance(getattr(data, "index", None), pd.MultiIndex):
        raise NotImplementedError("Dask does not support MultiIndex Dataframes.")

    if not has_parallel_type(data):
        raise TypeError("Input must be a pandas DataFrame or Series.")

    if (npartitions is None) == (none_chunksize := (chunksize is None)):
        raise ValueError("Exactly one of npartitions and chunksize must be specified.")

    nrows = len(data)

    if none_chunksize:
        if not isinstance(npartitions, int):
            raise TypeError(
                "Please provide npartitions as an int, or possibly as None if you specify chunksize."
            )
        chunksize = int(ceil(nrows / npartitions))
    elif not isinstance(chunksize, int):
        raise TypeError(
            "Please provide chunksize as an int, or possibly as None if you specify npartitions."
        )

    name = name or ("from_pandas-" + tokenize(data, chunksize))

    if not nrows:
        return new_dd_object({(name, 0): data}, name, data, [None, None])

    if data.index.isna().any() and not data.index.is_numeric():
        raise NotImplementedError(
            "Index in passed data is non-numeric and contains nulls, which Dask does not entirely support.\n"
            "Consider passing `data.loc[~data.isna()]` instead."
        )

    if sort:
        if not data.index.is_monotonic_increasing:
            data = data.sort_index(ascending=True)
        divisions, locations = sorted_division_locations(
            data.index, chunksize=chunksize
        )
    else:
        locations = list(range(0, nrows, chunksize)) + [len(data)]
        divisions = [None] * len(locations)

    dsk = {
        (name, i): data.iloc[start:stop]
        for i, (start, stop) in enumerate(zip(locations[:-1], locations[1:]))
    }
    return new_dd_object(dsk, name, data, divisions)


@_deprecated(after_version="2022.02.1")
def from_bcolz(x, chunksize=None, categorize=True, index=None, lock=lock, **kwargs):
    """Read BColz CTable into a Dask Dataframe

    BColz is a fast on-disk compressed column store with careful attention
    given to compression.  https://bcolz.readthedocs.io/en/latest/

    Parameters
    ----------
    x : bcolz.ctable
    chunksize : int, optional
        The size(rows) of blocks to pull out from ctable.
    categorize : bool, defaults to True
        Automatically categorize all string dtypes
    index : string, optional
        Column to make the index
    lock: bool or Lock
        Lock to use when reading or False for no lock (not-thread-safe)

    See Also
    --------
    from_array: more generic function not optimized for bcolz
    """
    if lock is True:
        lock = Lock()

    import bcolz

    import dask.array as da

    if isinstance(x, str):
        x = bcolz.ctable(rootdir=x)
    bc_chunklen = max(x[name].chunklen for name in x.names)
    if chunksize is None and bc_chunklen > 10000:
        chunksize = bc_chunklen

    categories = dict()
    if categorize:
        for name in x.names:
            if (
                np.issubdtype(x.dtype[name], np.string_)
                or np.issubdtype(x.dtype[name], np.unicode_)
                or np.issubdtype(x.dtype[name], np.object_)
            ):
                a = da.from_array(x[name], chunks=(chunksize * len(x.names),))
                categories[name] = da.unique(a).compute()

    columns = tuple(x.dtype.names)
    divisions = tuple(range(0, len(x), chunksize))
    divisions = divisions + (len(x) - 1,)
    if x.rootdir:
        token = tokenize(
            (x.rootdir, os.path.getmtime(x.rootdir)),
            chunksize,
            categorize,
            index,
            kwargs,
        )
    else:
        token = tokenize(
            (id(x), x.shape, x.dtype), chunksize, categorize, index, kwargs
        )
    new_name = "from_bcolz-" + token

    dsk = {
        (new_name, i): (
            dataframe_from_ctable,
            x,
            (slice(i * chunksize, (i + 1) * chunksize),),
            columns,
            categories,
            lock,
        )
        for i in range(0, int(ceil(len(x) / chunksize)))
    }

    meta = dataframe_from_ctable(x, slice(0, 0), columns, categories, lock)
    result = DataFrame(dsk, new_name, meta, divisions)

    if index:
        assert index in x.names
        a = da.from_array(x[index], chunks=(chunksize * len(x.names),))
        q = np.linspace(0, 100, len(x) // chunksize + 2)
        divisions = tuple(da.percentile(a, q).compute())
        return set_partition(result, index, divisions, **kwargs)
    else:
        return result


def dataframe_from_ctable(x, slc, columns=None, categories=None, lock=lock):
    """Get DataFrame from bcolz.ctable

    Parameters
    ----------
    x: bcolz.ctable
    slc: slice
    columns: list of column names or None

    >>> import bcolz
    >>> x = bcolz.ctable([[1, 2, 3, 4], [10, 20, 30, 40]], names=['a', 'b'])
    >>> dataframe_from_ctable(x, slice(1, 3))
       a   b
    1  2  20
    2  3  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns=['b'])
        b
    1  20
    2  30

    >>> dataframe_from_ctable(x, slice(1, 3), columns='b')
    1    20
    2    30
    Name: b, dtype: int...

    """
    import bcolz

    if columns is None:
        columns = x.dtype.names
    if isinstance(columns, tuple):
        columns = list(columns)

    x = x[columns]
    if type(slc) is slice:
        start = slc.start
        stop = slc.stop if slc.stop < len(x) else len(x)
    else:
        start = slc[0].start
        stop = slc[0].stop if slc[0].stop < len(x) else len(x)
    idx = pd.Index(range(start, stop))

    if lock:
        lock.acquire()
    try:
        if isinstance(x, bcolz.ctable):
            chunks = [x[name][slc] for name in columns]
            if categories is not None:
                chunks = [
                    pd.Categorical.from_codes(
                        np.searchsorted(categories[name], chunk), categories[name], True
                    )
                    if name in categories
                    else chunk
                    for name, chunk in zip(columns, chunks)
                ]
            result = pd.DataFrame(
                dict(zip(columns, chunks)), columns=columns, index=idx
            )

        elif isinstance(x, bcolz.carray):
            chunk = x[slc]
            if categories is not None and columns and columns in categories:
                chunk = pd.Categorical.from_codes(
                    np.searchsorted(categories[columns], chunk),
                    categories[columns],
                    True,
                )
            result = pd.Series(chunk, name=columns, index=idx)
    finally:
        if lock:
            lock.release()
    return result


def _partition_from_array(data, index=None, initializer=None, **kwargs):
    """Create a Dask partition for either a DataFrame or Series.

    Designed to be used with :func:`dask.blockwise.blockwise`. ``data`` is the array
    from which the partition will be created. ``index`` can be:

    1. ``None``, in which case each partition has an independent RangeIndex
    2. a `tuple` with two elements, the start and stop values for a RangeIndex for
       this partition, which gives a continuously varying RangeIndex over the
       whole Dask DataFrame
    3. an instance of a ``pandas.Index`` or a subclass thereof

    The ``kwargs`` _must_ contain an ``initializer`` key which is set by calling
    ``type(meta)``.
    """
    if isinstance(index, tuple):
        index = pd.RangeIndex(*index)
    return initializer(data, index=index, **kwargs)


def from_dask_array(x, columns=None, index=None, meta=None):
    """Create a Dask DataFrame from a Dask Array.

    Converts a 2d array into a DataFrame and a 1d array into a Series.

    Parameters
    ----------
    x : da.Array
    columns : list or string
        list of column names if DataFrame, single string if Series
    index : dask.dataframe.Index, optional
        An optional *dask* Index to use for the output Series or DataFrame.

        The default output index depends on whether `x` has any unknown
        chunks. If there are any unknown chunks, the output has ``None``
        for all the divisions (one per chunk). If all the chunks are known,
        a default index with known divisions is created.

        Specifying `index` can be useful if you're conforming a Dask Array
        to an existing dask Series or DataFrame, and you would like the
        indices to match.
    meta : object, optional
        An optional `meta` parameter can be passed for dask
        to specify the concrete dataframe type to be returned.
        By default, pandas DataFrame is used.

    Examples
    --------
    >>> import dask.array as da
    >>> import dask.dataframe as dd
    >>> x = da.ones((4, 2), chunks=(2, 2))
    >>> df = dd.io.from_dask_array(x, columns=['a', 'b'])
    >>> df.compute()
         a    b
    0  1.0  1.0
    1  1.0  1.0
    2  1.0  1.0
    3  1.0  1.0

    See Also
    --------
    dask.bag.to_dataframe: from dask.bag
    dask.dataframe._Frame.values: Reverse conversion
    dask.dataframe._Frame.to_records: Reverse conversion
    """
    meta = _meta_from_array(x, columns, index, meta=meta)

    name = "from-dask-array-" + tokenize(x, columns)
    graph_dependencies = [x]
    arrays_and_indices = [x.name, "ij" if x.ndim == 2 else "i"]
    numblocks = {x.name: x.numblocks}

    if index is not None:
        # An index is explicitly given by the caller, so we can pass it through to the
        # initializer after a few checks.
        if index.npartitions != x.numblocks[0]:
            msg = (
                "The index and array have different numbers of blocks. "
                "({} != {})".format(index.npartitions, x.numblocks[0])
            )
            raise ValueError(msg)
        divisions = index.divisions
        graph_dependencies.append(index)
        arrays_and_indices.extend([index._name, "i"])
        numblocks[index._name] = (index.npartitions,)

    elif np.isnan(sum(x.shape)):
        # The shape of the incoming array is not known in at least one dimension. As
        # such, we can't create an index for the entire output DataFrame and we set
        # the divisions to None to represent that.
        divisions = [None] * (len(x.chunks[0]) + 1)
    else:
        # The shape of the incoming array is known and we don't have an explicit index.
        # Create a mapping of chunk number in the incoming array to
        # (start row, stop row) tuples. These tuples will be used to create a sequential
        # RangeIndex later on that is continuous over the whole DataFrame.
        divisions = [0]
        stop = 0
        index_mapping = {}
        for i, increment in enumerate(x.chunks[0]):
            stop += increment
            index_mapping[(i,)] = (divisions[i], stop)
            divisions.append(stop)
        divisions[-1] -= 1
        arrays_and_indices.extend([BlockwiseDepDict(mapping=index_mapping), "i"])

    if is_series_like(meta):
        kwargs = {"dtype": x.dtype, "name": meta.name, "initializer": type(meta)}
    else:
        kwargs = {"columns": meta.columns, "initializer": type(meta)}

    blk = blockwise(
        _partition_from_array,
        name,
        "i",
        *arrays_and_indices,
        numblocks=numblocks,
        concatenate=True,
        # kwargs passed through to the DataFrame/Series initializer
        **kwargs,
    )

    graph = HighLevelGraph.from_collections(name, blk, dependencies=graph_dependencies)
    return new_dd_object(graph, name, meta, divisions)


def _link(token, result):
    """A dummy function to link results together in a graph

    We use this to enforce an artificial sequential ordering on tasks that
    don't explicitly pass around a shared resource
    """
    return None


def _df_to_bag(df, index=False, format="tuple"):
    if isinstance(df, pd.DataFrame):
        if format == "tuple":
            return list(map(tuple, df.itertuples(index)))
        elif format == "dict":
            if index:
                return [
                    {**{"index": idx}, **values}
                    for values, idx in zip(df.to_dict("records"), df.index)
                ]
            else:
                return df.to_dict(orient="records")
    elif isinstance(df, pd.Series):
        if format == "tuple":
            return list(df.items()) if index else list(df)
        elif format == "dict":
            return df.to_frame().to_dict(orient="records")


def to_bag(df, index=False, format="tuple"):
    """Create Dask Bag from a Dask DataFrame

    Parameters
    ----------
    index : bool, optional
        If True, the elements are tuples of ``(index, value)``, otherwise
        they're just the ``value``.  Default is False.
    format : {"tuple", "dict", "frame"}, optional
        Whether to return a bag of tuples, dictionaries, or
        dataframe-like objects. Default is "tuple". If "frame",
        the original partitions of ``df`` will not be transformed
        in any way.


    Examples
    --------
    >>> bag = df.to_bag()  # doctest: +SKIP
    """
    from dask.bag.core import Bag

    if not isinstance(df, (DataFrame, Series)):
        raise TypeError("df must be either DataFrame or Series")
    name = "to_bag-" + tokenize(df, index, format)
    if format == "frame":
        # Use existing graph and name of df, but
        # drop meta to produce a Bag collection
        dsk = df.dask
        name = df._name
    else:
        dsk = {
            (name, i): (_df_to_bag, block, index, format)
            for (i, block) in enumerate(df.__dask_keys__())
        }
        dsk.update(df.__dask_optimize__(df.__dask_graph__(), df.__dask_keys__()))
    return Bag(dsk, name, df.npartitions)


def to_records(df):
    """Create Dask Array from a Dask Dataframe

    Warning: This creates a dask.array without precise shape information.
    Operations that depend on shape information, like slicing or reshaping,
    will not work.

    Examples
    --------
    >>> df.to_records()  # doctest: +SKIP

    See Also
    --------
    dask.dataframe._Frame.values
    dask.dataframe.from_dask_array
    """
    return df.map_partitions(M.to_records)


# TODO: type this -- causes lots of papercuts
@insert_meta_param_description
def from_delayed(
    dfs,
    meta=None,
    divisions=None,
    prefix="from-delayed",
    verify_meta=True,
):
    """Create Dask DataFrame from many Dask Delayed objects

    Parameters
    ----------
    dfs : list of Delayed or Future
        An iterable of ``dask.delayed.Delayed`` objects, such as come from
        ``dask.delayed`` or an iterable of ``distributed.Future`` objects,
        such as come from ``client.submit`` interface. These comprise the individual
        partitions of the resulting dataframe.
    $META
    divisions : tuple, str, optional
        Partition boundaries along the index.
        For tuple, see https://docs.dask.org/en/latest/dataframe-design.html#partitions
        For string 'sorted' will compute the delayed values to find index
        values.  Assumes that the indexes are mutually sorted.
        If None, then won't use index information
    prefix : str, optional
        Prefix to prepend to the keys.
    verify_meta : bool, optional
        If True check that the partitions have consistent metadata, defaults to True.
    """
    from dask.delayed import Delayed

    if isinstance(dfs, Delayed):
        dfs = [dfs]
    dfs = [
        delayed(df) if not isinstance(df, Delayed) and hasattr(df, "key") else df
        for df in dfs
    ]

    for df in dfs:
        if not isinstance(df, Delayed):
            raise TypeError("Expected Delayed object, got %s" % type(df).__name__)

    if meta is None:
        meta = delayed(make_meta)(dfs[0]).compute()
    else:
        meta = make_meta(meta)

    if not dfs:
        dfs = [delayed(make_meta)(meta)]

    if divisions is None or divisions == "sorted":
        divs = [None] * (len(dfs) + 1)
    else:
        divs = tuple(divisions)
        if len(divs) != len(dfs) + 1:
            raise ValueError("divisions should be a tuple of len(dfs) + 1")

    name = prefix + "-" + tokenize(*dfs)
    layer = DataFrameIOLayer(
        name=name,
        columns=None,
        inputs=BlockwiseDepDict(
            {(i,): inp.key for i, inp in enumerate(dfs)},
            produces_keys=True,
        ),
        io_func=partial(check_meta, meta=meta, funcname="from_delayed")
        if verify_meta
        else lambda x: x,
    )
    df = new_dd_object(
        HighLevelGraph.from_collections(name, layer, dfs), name, meta, divs
    )

    if divisions == "sorted":
        from dask.dataframe.shuffle import compute_and_set_divisions

        df = compute_and_set_divisions(df)

    return df


def sorted_division_locations(seq, npartitions=None, chunksize=None):
    """Find division locations and values in sorted list

    Examples
    --------

    >>> L = ['A', 'B', 'C', 'D', 'E', 'F']
    >>> sorted_division_locations(L, chunksize=2)
    (['A', 'C', 'E', 'F'], [0, 2, 4, 6])

    >>> sorted_division_locations(L, chunksize=3)
    (['A', 'D', 'F'], [0, 3, 6])

    >>> L = ['A', 'A', 'A', 'A', 'B', 'B', 'B', 'C']
    >>> sorted_division_locations(L, chunksize=3)
    (['A', 'B', 'C'], [0, 4, 8])

    >>> sorted_division_locations(L, chunksize=2)
    (['A', 'B', 'C'], [0, 4, 8])

    >>> sorted_division_locations(['A'], chunksize=2)
    (['A', 'A'], [0, 1])
    """
    if (npartitions is None) == (chunksize is None):
        raise ValueError("Exactly one of npartitions and chunksize must be specified.")

    if npartitions:
        chunksize = ceil(len(seq) / npartitions)

    positions = [0]
    values = [seq[0]]
    for pos in range(0, len(seq), chunksize):
        if pos <= positions[-1]:
            continue
        while pos + 1 < len(seq) and seq[pos - 1] == seq[pos]:
            pos += 1
        values.append(seq[pos])
        if pos == len(seq) - 1:
            pos += 1
        positions.append(pos)

    if positions[-1] != len(seq):
        positions.append(len(seq))
        values.append(seq[-1])

    return values, positions


class _PackedArgCallable(DataFrameIOFunction):
    """Packed-argument wrapper for DataFrameIOFunction

    This is a private helper class for ``from_map``. This class
    ensures that packed positional arguments will be expanded
    before the underlying function (``func``) is called. This class
    also handles optional metadata enforcement and column projection
    (when ``func`` satisfies the ``DataFrameIOFunction`` protocol).
    """

    def __init__(
        self,
        func,
        args=None,
        kwargs=None,
        meta=None,
        packed=False,
        enforce_metadata=False,
    ):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.meta = meta
        self.enforce_metadata = enforce_metadata
        self.packed = packed
        self.is_dataframe_io_func = isinstance(self.func, DataFrameIOFunction)

    @property
    def columns(self):
        if self.is_dataframe_io_func:
            return self.func.columns
        return None

    def project_columns(self, columns):
        if self.is_dataframe_io_func:
            return _PackedArgCallable(
                self.func.project_columns(columns),
                args=self.args,
                kwargs=self.kwargs,
                meta=self.meta[columns],
                packed=self.packed,
                enforce_metadata=self.enforce_metadata,
            )
        return self

    def __call__(self, packed_arg):
        if not self.packed:
            packed_arg = [packed_arg]
        if self.enforce_metadata:
            return apply_and_enforce(
                *packed_arg,
                *(self.args or []),
                _func=self.func,
                _meta=self.meta,
                **(self.kwargs or {}),
            )
        return self.func(
            *packed_arg,
            *(self.args or []),
            **(self.kwargs or {}),
        )


@insert_meta_param_description
def from_map(
    func,
    *iterables,
    args=None,
    meta=None,
    divisions=None,
    label=None,
    token=None,
    enforce_metadata=True,
    **kwargs,
):
    """Create a DataFrame collection from a custom function map

    WARNING: The ``from_map`` API is experimental, and stability is not
    yet guaranteed. Use at your own risk!

    Parameters
    ----------
    func : callable
        Function used to create each partition. If ``func`` satisfies the
        ``DataFrameIOFunction`` protocol, column projection will be enabled.
    *iterables : Iterable objects
        Iterable objects to map to each output partition. All iterables must
        be the same length. This length determines the number of partitions
        in the output collection (only one element of each iterable will
        be passed to ``func`` for each partition).
    args : list or tuple, optional
        Positional arguments to broadcast to each output partition. Note
        that these arguments will always be passed to ``func`` after the
        ``iterables`` positional arguments.
    $META
    divisions : tuple, str, optional
        Partition boundaries along the index.
        For tuple, see https://docs.dask.org/en/latest/dataframe-design.html#partitions
        For string 'sorted' will compute the delayed values to find index
        values.  Assumes that the indexes are mutually sorted.
        If None, then won't use index information
    label : str, optional
        String to use as the function-name label in the output
        collection-key names.
    token : str, optional
        String to use as the "token" in the output collection-key names.
    enforce_metadata : bool, default True
        Whether to enforce at runtime that the structure of the DataFrame
        produced by ``func`` actually matches the structure of ``meta``.
        This will rename and reorder columns for each partition,
        and will raise an error if this doesn't work or types don't match.
    **kwargs:
        Key-word arguments to broadcast to each output partition. These
        same arguments will be passed to ``func`` for every output partition.

    Examples
    --------
    >>> import pandas as pd
    >>> import dask.dataframe as dd
    >>> func = lambda x, size=0: pd.Series([x] * size)
    >>> inputs = ["A", "B"]
    >>> dd.from_map(func, inputs, size=2).compute()
    0    A
    1    A
    0    B
    1    B
    dtype: object

    This API can also be used as an alternative to other file-based
    IO functions, like ``read_parquet`` (which are already just
    ``from_map`` wrapper functions):

    >>> import pandas as pd
    >>> import dask.dataframe as dd
    >>> paths = ["0.parquet", "1.parquet", "2.parquet"]
    >>> dd.from_map(pd.read_parquet, paths).head()  # doctest: +SKIP
                        name
    timestamp
    2000-01-01 00:00:00   Laura
    2000-01-01 00:00:01  Oliver
    2000-01-01 00:00:02   Alice
    2000-01-01 00:00:03  Victor
    2000-01-01 00:00:04     Bob

    Since ``from_map`` allows you to map an arbitrary function
    to any number of iterable objects, it can be a very convenient
    means of implementing functionality that may be missing from
    from other DataFrame-creation methods. For example, if you
    happen to have apriori knowledge about the number of rows
    in each of the files in a dataset, you can generate a
    DataFrame collection with a global RangeIndex:

    >>> import pandas as pd
    >>> import numpy as np
    >>> import dask.dataframe as dd
    >>> paths = ["0.parquet", "1.parquet", "2.parquet"]
    >>> file_sizes = [86400, 86400, 86400]
    >>> def func(path, row_offset):
    ...     # Read parquet file and set RangeIndex offset
    ...     df = pd.read_parquet(path)
    ...     return df.set_index(
    ...         pd.RangeIndex(row_offset, row_offset+len(df))
    ...     )
    >>> def get_ddf(paths, file_sizes):
    ...     offsets = [0] + list(np.cumsum(file_sizes))
    ...     return dd.from_map(
    ...         func, paths, offsets[:-1], divisions=offsets
    ...     )
    >>> ddf = get_ddf(paths, file_sizes)  # doctest: +SKIP
    >>> ddf.index  # doctest: +SKIP
    Dask Index Structure:
    npartitions=3
    0         int64
    86400       ...
    172800      ...
    259200      ...
    dtype: int64
    Dask Name: myfunc, 6 tasks

    See Also
    --------
    dask.dataframe.from_delayed
    dask.layers.DataFrameIOLayer
    """

    # Input validation
    if not callable(func):
        raise ValueError("`func` argument must be `callable`")
    lengths = set()
    iterables = list(iterables)
    for i, iterable in enumerate(iterables):
        if not isinstance(iterable, Iterable):
            raise ValueError(
                f"All elements of `iterables` must be Iterable, got {type(iterable)}"
            )
        try:
            lengths.add(len(iterable))
        except (AttributeError, TypeError):
            iterables[i] = list(iterable)
            lengths.add(len(iterables[i]))
    if len(lengths) == 0:
        raise ValueError("`from_map` requires at least one Iterable input")
    elif len(lengths) > 1:
        raise ValueError("All `iterables` must have the same length")
    if lengths == {0}:
        raise ValueError("All `iterables` must have a non-zero length")

    # Check for `produces_tasks` and `creation_info`.
    # These options are included in the function signature,
    # because they are not intended for "public" use.
    produces_tasks = kwargs.pop("produces_tasks", False)
    creation_info = kwargs.pop("creation_info", None)

    if produces_tasks or len(iterables) == 1:
        if len(iterables) > 1:
            # Tasks are not detected correctly when they are "packed"
            # within an outer list/tuple
            raise ValueError(
                "Multiple iterables not supported when produces_tasks=True"
            )
        inputs = iterables[0]
        packed = False
    else:
        inputs = list(zip(*iterables))
        packed = True

    # Define collection name
    label = label or funcname(func)
    token = token or tokenize(
        func, meta, inputs, args, divisions, enforce_metadata, **kwargs
    )
    name = f"{label}-{token}"

    # Get "projectable" column selection.
    # Note that this relies on the IO function
    # ducktyping with DataFrameIOFunction
    column_projection = func.columns if isinstance(func, DataFrameIOFunction) else None

    # NOTE: Most of the metadata-handling logic used here
    # is copied directly from `map_partitions`
    if meta is None:
        meta = _emulate(
            func,
            *(inputs[0] if packed else inputs[:1]),
            *(args or []),
            udf=True,
            **kwargs,
        )
        meta_is_emulated = True
    else:
        meta = make_meta(meta)
        meta_is_emulated = False

    if not (has_parallel_type(meta) or is_arraylike(meta) and meta.shape):
        if not meta_is_emulated:
            raise TypeError(
                "Meta is not valid, `from_map` expects output to be a pandas object. "
                "Try passing a pandas object as meta or a dict or tuple representing the "
                "(name, dtype) of the columns."
            )
        # If `meta` is not a pandas object, the concatenated results will be a
        # different type
        meta = make_meta(_concat([meta]))

    # Ensure meta is empty DataFrame
    meta = make_meta(meta)

    # Define io_func
    if packed or args or kwargs or enforce_metadata:
        io_func = _PackedArgCallable(
            func,
            args=args,
            kwargs=kwargs,
            meta=meta if enforce_metadata else None,
            enforce_metadata=enforce_metadata,
            packed=packed,
        )
    else:
        io_func = func

    # Construct DataFrameIOLayer
    layer = DataFrameIOLayer(
        name,
        column_projection,
        inputs,
        io_func,
        label=label,
        produces_tasks=produces_tasks,
        creation_info=creation_info,
    )

    # Return new DataFrame-collection object
    divisions = divisions or [None] * (len(inputs) + 1)
    graph = HighLevelGraph.from_collections(name, layer, dependencies=[])
    return new_dd_object(graph, name, meta, divisions)


DataFrame.to_records.__doc__ = to_records.__doc__
DataFrame.to_bag.__doc__ = to_bag.__doc__
