from collections.abc import Iterable
from math import ceil
from threading import Lock
from typing import Optional, Union

import pandas as pd

from dask.dataframe.core import (
    DataFrame,
    _concat,
    _emulate,
    apply_and_enforce,
    has_parallel_type,
)
from dask.dataframe.io.utils import DataFrameIOFunction
from dask.dataframe.utils import insert_meta_param_description, make_meta
from dask.operation.dataframe.collection import new_dd_collection
from dask.operation.dataframe.core import FrameCreation
from dask.utils import funcname, is_arraylike

lock = Lock()


def from_pandas(
    data: Union[pd.DataFrame, pd.Series],
    npartitions: Optional[int] = None,
    chunksize: Optional[int] = None,
    sort: bool = True,
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

    if not nrows:
        return new_dd_collection(
            FrameCreation(
                lambda x: x,
                [data],
                "from_pandas",
                data,
                (None, None),
            )
        )

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

    return new_dd_collection(
        FrameCreation(
            lambda x: x,
            [
                data.iloc[start:stop]
                for (start, stop) in zip(locations[:-1], locations[1:])
            ],
            "from_pandas",
            data,
            divisions,
        )
    )


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
    assert not creation_info  # For now

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

    # # Get "projectable" column selection.
    # # Note that this relies on the IO function
    # # ducktyping with DataFrameIOFunction
    # column_projection = func.columns if isinstance(func, DataFrameIOFunction) else None

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

    divisions = divisions or [None] * (len(inputs) + 1)
    operation = FrameCreation(
        io_func,
        inputs,
        label,
        meta,
        tuple(divisions),
        # columns=column_projection,
    )
    # Return new DataFrame-collection object
    return new_dd_collection(operation)
