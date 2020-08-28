import contextlib
import logging
import math
import shutil
from operator import getitem
import uuid
import tempfile

import tlz as toolz
import numpy as np
import pandas as pd

from .core import DataFrame, Series, _Frame, _concat, map_partitions, new_dd_object

from .. import base, config
from ..base import tokenize, compute, compute_as_if_collection, is_dask_collection
from ..delayed import delayed
from ..highlevelgraph import HighLevelGraph
from ..sizeof import sizeof
from ..utils import digit, insert, M
from .utils import hash_object_dispatch, group_split_dispatch
from . import methods

logger = logging.getLogger(__name__)


def set_index(
    df,
    index,
    npartitions=None,
    shuffle=None,
    compute=False,
    drop=True,
    upsample=1.0,
    divisions=None,
    partition_size=128e6,
    **kwargs
):
    """ See _Frame.set_index for docstring """
    if isinstance(index, Series) and index._name == df.index._name:
        return df
    if isinstance(index, (DataFrame, tuple, list)):
        # Accept ["a"], but not [["a"]]
        if (
            isinstance(index, list)
            and len(index) == 1
            and not isinstance(index[0], list)  # if index = [["a"]], leave it that way
        ):
            index = index[0]
        else:
            raise NotImplementedError(
                "Dask dataframe does not yet support multi-indexes.\n"
                "You tried to index with this index: %s\n"
                "Indexes must be single columns only." % str(index)
            )

    if npartitions == "auto":
        repartition = True
        npartitions = max(100, df.npartitions)
    else:
        if npartitions is None:
            npartitions = df.npartitions
        repartition = False

    if not isinstance(index, Series):
        index2 = df[index]
    else:
        index2 = index

    if divisions is None:
        if repartition:
            index2, df = base.optimize(index2, df)
            parts = df.to_delayed(optimize_graph=False)
            sizes = [delayed(sizeof)(part) for part in parts]
        else:
            (index2,) = base.optimize(index2)
            sizes = []

        divisions = index2._repartition_quantiles(npartitions, upsample=upsample)
        iparts = index2.to_delayed(optimize_graph=False)
        mins = [ipart.min() for ipart in iparts]
        maxes = [ipart.max() for ipart in iparts]
        sizes, mins, maxes = base.optimize(sizes, mins, maxes)
        divisions, sizes, mins, maxes = base.compute(
            divisions, sizes, mins, maxes, optimize_graph=False
        )
        divisions = methods.tolist(divisions)

        empty_dataframe_detected = pd.isnull(divisions).all()
        if repartition or empty_dataframe_detected:
            total = sum(sizes)
            npartitions = max(math.ceil(total / partition_size), 1)
            npartitions = min(npartitions, df.npartitions)
            n = len(divisions)
            try:
                divisions = np.interp(
                    x=np.linspace(0, n - 1, npartitions + 1),
                    xp=np.linspace(0, n - 1, n),
                    fp=divisions,
                ).tolist()
            except (TypeError, ValueError):  # str type
                indexes = np.linspace(0, n - 1, npartitions + 1).astype(int)
                divisions = [divisions[i] for i in indexes]

        mins = remove_nans(mins)
        maxes = remove_nans(maxes)
        if pd.api.types.is_categorical_dtype(index2.dtype):
            dtype = index2.dtype
            mins = pd.Categorical(mins, dtype=dtype).codes.tolist()
            maxes = pd.Categorical(maxes, dtype=dtype).codes.tolist()

        if (
            mins == sorted(mins)
            and maxes == sorted(maxes)
            and all(mx < mn for mx, mn in zip(maxes[:-1], mins[1:]))
        ):
            divisions = mins + [maxes[-1]]
            result = set_sorted_index(df, index, drop=drop, divisions=divisions)
            return result.map_partitions(M.sort_index)

    return set_partition(
        df, index, divisions, shuffle=shuffle, drop=drop, compute=compute, **kwargs
    )


def remove_nans(divisions):
    """Remove nans from divisions

    These sometime pop up when we call min/max on an empty partition

    Examples
    --------
    >>> remove_nans((np.nan, 1, 2))
    [1, 1, 2]
    >>> remove_nans((1, np.nan, 2))
    [1, 2, 2]
    >>> remove_nans((1, 2, np.nan))
    [1, 2, 2]
    """
    divisions = list(divisions)

    for i in range(len(divisions) - 2, -1, -1):
        if pd.isnull(divisions[i]):
            divisions[i] = divisions[i + 1]

    for i in range(len(divisions) - 1, -1, -1):
        if not pd.isnull(divisions[i]):
            for j in range(i + 1, len(divisions)):
                divisions[j] = divisions[i]
            break

    return divisions


def set_partition(
    df, index, divisions, max_branch=32, drop=True, shuffle=None, compute=None
):
    """Group DataFrame by index

    Sets a new index and partitions data along that index according to
    divisions.  Divisions are often found by computing approximate quantiles.
    The function ``set_index`` will do both of these steps.

    Parameters
    ----------
    df: DataFrame/Series
        Data that we want to re-partition
    index: string or Series
        Column to become the new index
    divisions: list
        Values to form new divisions between partitions
    drop: bool, default True
        Whether to delete columns to be used as the new index
    shuffle: str (optional)
        Either 'disk' for an on-disk shuffle or 'tasks' to use the task
        scheduling framework.  Use 'disk' if you are on a single machine
        and 'tasks' if you are on a distributed cluster.
    max_branch: int (optional)
        If using the task-based shuffle, the amount of splitting each
        partition undergoes.  Increase this for fewer copies but more
        scheduler overhead.

    See Also
    --------
    set_index
    shuffle
    partd
    """
    meta = df._meta._constructor_sliced([0])
    if isinstance(divisions, tuple):
        # pd.isna considers tuples to be scalars. Convert to a list.
        divisions = list(divisions)

    if np.isscalar(index):
        dtype = df[index].dtype
    else:
        dtype = index.dtype

    if pd.isna(divisions).any() and pd.api.types.is_integer_dtype(dtype):
        # Can't construct a Series[int64] when any / all of the divisions are NaN.
        divisions = df._meta._constructor_sliced(divisions)
    else:
        divisions = df._meta._constructor_sliced(divisions, dtype=dtype)

    if np.isscalar(index):
        partitions = df[index].map_partitions(
            set_partitions_pre, divisions=divisions, meta=meta
        )
        df2 = df.assign(_partitions=partitions)
    else:
        partitions = index.map_partitions(
            set_partitions_pre, divisions=divisions, meta=meta
        )
        df2 = df.assign(_partitions=partitions, _index=index)

    df3 = rearrange_by_column(
        df2,
        "_partitions",
        max_branch=max_branch,
        npartitions=len(divisions) - 1,
        shuffle=shuffle,
        compute=compute,
        ignore_index=True,
    )

    if np.isscalar(index):
        df4 = df3.map_partitions(
            set_index_post_scalar,
            index_name=index,
            drop=drop,
            column_dtype=df.columns.dtype,
        )
    else:
        df4 = df3.map_partitions(
            set_index_post_series,
            index_name=index.name,
            drop=drop,
            column_dtype=df.columns.dtype,
        )

    df4.divisions = methods.tolist(divisions)

    return df4.map_partitions(M.sort_index)


def shuffle(
    df,
    index,
    shuffle=None,
    npartitions=None,
    max_branch=32,
    ignore_index=False,
    compute=None,
):
    """Group DataFrame by index

    Hash grouping of elements. After this operation all elements that have
    the same index will be in the same partition. Note that this requires
    full dataset read, serialization and shuffle. This is expensive. If
    possible you should avoid shuffles.

    This does not preserve a meaningful index/partitioning scheme. This is not
    deterministic if done in parallel.

    See Also
    --------
    set_index
    set_partition
    shuffle_disk
    """
    list_like = pd.api.types.is_list_like(index) and not is_dask_collection(index)
    if shuffle == "tasks" and (isinstance(index, str) or list_like):
        # Avoid creating the "_partitions" column if possible.
        # We currently do this if the user is passing in
        # specific column names (and shuffle == "tasks").
        if isinstance(index, str):
            index = [index]
        else:
            index = list(index)
        nset = set(index)
        if nset.intersection(set(df.columns)) == nset:
            return rearrange_by_column(
                df,
                index,
                npartitions=npartitions,
                max_branch=max_branch,
                shuffle=shuffle,
                ignore_index=ignore_index,
                compute=compute,
            )

    if not isinstance(index, _Frame):
        index = df._select_columns_or_index(index)

    partitions = index.map_partitions(
        partitioning_index,
        npartitions=npartitions or df.npartitions,
        meta=df._meta._constructor_sliced([0]),
        transform_divisions=False,
    )
    df2 = df.assign(_partitions=partitions)
    df2._meta.index.name = df._meta.index.name
    df3 = rearrange_by_column(
        df2,
        "_partitions",
        npartitions=npartitions,
        max_branch=max_branch,
        shuffle=shuffle,
        compute=compute,
        ignore_index=ignore_index,
    )
    del df3["_partitions"]
    return df3


def rearrange_by_divisions(df, column, divisions, max_branch=None, shuffle=None):
    """ Shuffle dataframe so that column separates along divisions """
    divisions = df._meta._constructor_sliced(divisions)
    meta = df._meta._constructor_sliced([0])
    # Assign target output partitions to every row
    partitions = df[column].map_partitions(
        set_partitions_pre, divisions=divisions, meta=meta
    )
    df2 = df.assign(_partitions=partitions)

    # Perform shuffle
    df3 = rearrange_by_column(
        df2,
        "_partitions",
        max_branch=max_branch,
        npartitions=len(divisions) - 1,
        shuffle=shuffle,
    )
    del df3["_partitions"]
    return df3


def rearrange_by_column(
    df,
    col,
    npartitions=None,
    max_branch=None,
    shuffle=None,
    compute=None,
    ignore_index=False,
):
    shuffle = shuffle or config.get("shuffle", None) or "disk"
    if shuffle == "disk":
        return rearrange_by_column_disk(df, col, npartitions, compute=compute)
    elif shuffle == "tasks":
        df2 = rearrange_by_column_tasks(
            df, col, max_branch, npartitions, ignore_index=ignore_index
        )
        if ignore_index:
            df2._meta = df2._meta.reset_index(drop=True)
        return df2
    else:
        raise NotImplementedError("Unknown shuffle method %s" % shuffle)


class maybe_buffered_partd(object):
    """
    If serialized, will return non-buffered partd. Otherwise returns a buffered partd
    """

    def __init__(self, buffer=True, tempdir=None):
        self.tempdir = tempdir or config.get("temporary_directory", None)
        self.buffer = buffer
        self.compression = config.get("dataframe.shuffle-compression", None)

    def __reduce__(self):
        if self.tempdir:
            return (maybe_buffered_partd, (False, self.tempdir))
        else:
            return (maybe_buffered_partd, (False,))

    def __call__(self, *args, **kwargs):
        import partd

        path = tempfile.mkdtemp(suffix=".partd", dir=self.tempdir)

        try:
            partd_compression = (
                getattr(partd.compressed, self.compression)
                if self.compression
                else None
            )
        except AttributeError as e:
            raise ImportError(
                "Not able to import and load {0} as compression algorithm."
                "Please check if the library is installed and supported by Partd.".format(
                    self.compression
                )
            ) from e
        file = partd.File(path)
        partd.file.cleanup_files.append(path)
        # Envelope partd file with compression, if set and available
        if partd_compression:
            file = partd_compression(file)
        if self.buffer:
            return partd.PandasBlocks(partd.Buffer(partd.Dict(), file))
        else:
            return partd.PandasBlocks(file)


def rearrange_by_column_disk(df, column, npartitions=None, compute=False):
    """Shuffle using local disk

    See Also
    --------
    rearrange_by_column_tasks:
        Same function, but using tasks rather than partd
        Has a more informative docstring
    """
    if npartitions is None:
        npartitions = df.npartitions

    token = tokenize(df, column, npartitions)
    always_new_token = uuid.uuid1().hex

    p = ("zpartd-" + always_new_token,)
    dsk1 = {p: (maybe_buffered_partd(),)}

    # Partition data on disk
    name = "shuffle-partition-" + always_new_token
    dsk2 = {
        (name, i): (shuffle_group_3, key, column, npartitions, p)
        for i, key in enumerate(df.__dask_keys__())
    }

    dependencies = []
    layer = {}
    if compute:
        graph = HighLevelGraph.merge(df.dask, dsk1, dsk2)
        keys = [p, sorted(dsk2)]
        pp, values = compute_as_if_collection(DataFrame, graph, keys)
        dsk1 = {p: pp}
        dsk2 = dict(zip(sorted(dsk2), values))
    else:
        dependencies.append(df)

    # Barrier
    barrier_token = "barrier-" + always_new_token
    dsk3 = {barrier_token: (barrier, list(dsk2))}

    # Collect groups
    name1 = "shuffle-collect-1" + token
    dsk4 = {
        (name1, i): (collect, p, i, df._meta, barrier_token) for i in range(npartitions)
    }
    cleanup_token = "cleanup-" + always_new_token
    barrier_token2 = "barrier2-" + always_new_token
    # A task that depends on `cleanup-`, but has a small output
    dsk5 = {(barrier_token2, i): (barrier, part) for i, part in enumerate(dsk4)}
    # This indirectly depends on `cleanup-` and so runs after we're done using the disk
    dsk6 = {cleanup_token: (cleanup_partd_files, p, list(dsk5))}

    name = "shuffle-collect-2" + token
    dsk7 = {(name, i): (_noop, (name1, i), cleanup_token) for i in range(npartitions)}
    divisions = (None,) * (npartitions + 1)

    layer = toolz.merge(dsk1, dsk2, dsk3, dsk4, dsk5, dsk6, dsk7)
    graph = HighLevelGraph.from_collections(name, layer, dependencies=dependencies)
    return DataFrame(graph, name, df._meta, divisions)


def _noop(x, cleanup_token):
    """
    A task that does nothing.
    """
    return x


def _simple_rearrange_by_column_tasks(df, column, npartitions, ignore_index=False):
    """A simplified (single-stage) version of ``rearrange_by_column_tasks``."""

    token = tokenize(df, column)
    simple_shuffle_group_token = "simple-shuffle-group-" + token
    simple_shuffle_split_token = "simple-shuffle-split-" + token
    simple_shuffle_combine_token = "simple-shuffle-combine-" + token

    # Pre-Materialize tuples with max number of values
    # to be iterated upon in this function and
    # loop using slicing later.
    iter_tuples = tuple(range(max(df.npartitions, npartitions)))

    group = {}
    split = {}
    combine = {}

    for i in iter_tuples[: df.npartitions]:
        # Convert partition into dict of dataframe pieces
        group[(simple_shuffle_group_token, i)] = (
            shuffle_group,
            (df._name, i),
            column,
            0,
            npartitions,
            npartitions,
            ignore_index,
            npartitions,
        )

    for j in iter_tuples[:npartitions]:
        _concat_list = []
        for i in iter_tuples[: df.npartitions]:
            # Get out each individual dataframe piece from the dicts
            split[(simple_shuffle_split_token, i, j)] = (
                getitem,
                (simple_shuffle_group_token, i),
                j,
            )

            _concat_list.append((simple_shuffle_split_token, i, j))

        # concatenate those pieces together, with their friends
        combine[(simple_shuffle_combine_token, j)] = (
            _concat,
            _concat_list,
            ignore_index,
        )

    dsk = toolz.merge(group, split, combine)
    graph = HighLevelGraph.from_collections(
        simple_shuffle_combine_token, dsk, dependencies=[df]
    )

    return new_dd_object(
        graph, simple_shuffle_combine_token, df, (None,) * (npartitions + 1)
    )


def rearrange_by_column_tasks(
    df, column, max_branch=32, npartitions=None, ignore_index=False
):
    """Order divisions of DataFrame so that all values within column(s) align

    This enacts a task-based shuffle.  It contains most of the tricky logic
    around the complex network of tasks.  Typically before this function is
    called a new column, ``"_partitions"`` has been added to the dataframe,
    containing the output partition number of every row.  This function
    produces a new dataframe where every row is in the proper partition.  It
    accomplishes this by splitting each input partition into several pieces,
    and then concatenating pieces from different input partitions into output
    partitions.  If there are enough partitions then it does this work in
    stages to avoid scheduling overhead.

    Lets explain the motivation for this further.  Imagine that we have 1000
    input partitions and 1000 output partitions. In theory we could split each
    input into 1000 pieces, and then move the 1 000 000 resulting pieces
    around, and then concatenate them all into 1000 output groups.  This would
    be fine, but the central scheduling overhead of 1 000 000 tasks would
    become a bottleneck.  Instead we do this in stages so that we split each of
    the 1000 inputs into 30 pieces (we now have 30 000 pieces) move those
    around, concatenate back down to 1000, and then do the same process again.
    This has the same result as the full transfer, but now we've moved data
    twice (expensive) but done so with only 60 000 tasks (cheap).

    Note that the `column` input may correspond to a list of columns (rather
    than just a single column name).  In this case, the `shuffle_group` and
    `shuffle_group_2` functions will use hashing to map each row to an output
    partition. This approach may require the same rows to be hased multiple
    times, but avoids the need to assign a new "_partitions" column.

    Parameters
    ----------
    df: dask.dataframe.DataFrame
    column: str or list
        A column name on which we want to split, commonly ``"_partitions"``
        which is assigned by functions upstream.  This could also be a list of
        columns (in which case shuffle_group will create a hash array/column).
    max_branch: int
        The maximum number of splits per input partition.  Defaults to 32.
        If there are more partitions than this then the shuffling will occur in
        stages in order to avoid creating npartitions**2 tasks
        Increasing this number increases scheduling overhead but decreases the
        number of full-dataset transfers that we have to make.
    npartitions: Optional[int]
        The desired number of output partitions

    Returns
    -------
    df3: dask.dataframe.DataFrame

    See also
    --------
    rearrange_by_column_disk: same operation, but uses partd
    rearrange_by_column: parent function that calls this or rearrange_by_column_disk
    shuffle_group: does the actual splitting per-partition
    """
    max_branch = max_branch or 32
    n = df.npartitions

    stages = int(math.ceil(math.log(n) / math.log(max_branch)))
    if stages > 1:
        k = int(math.ceil(n ** (1 / stages)))
    else:
        k = n

    if (npartitions or n) <= max_branch:
        # We are creating a small number of output partitions.
        # No need for staged shuffling. Staged shuffling will
        # sometimes require extra work/communication in this case.
        return _simple_rearrange_by_column_tasks(
            df, column, (npartitions or n), ignore_index=ignore_index
        )

    groups = []
    splits = []
    joins = []

    inputs = [tuple(digit(i, j, k) for j in range(stages)) for i in range(k ** stages)]

    token = tokenize(df, column, max_branch)
    shuffle_join_name = "shuffle-join-" + token
    shuffle_group_name = "shuffle-group-" + token
    shuffle_split_name = "shuffle-split-" + token
    shuffle_token = "shuffle-" + token

    start = {}
    end = {}

    for idx, inp in enumerate(inputs):
        group = {}
        split = {}
        join = {}
        start[(shuffle_join_name, 0, inp)] = (
            (df._name, idx) if idx < df.npartitions else df._meta
        )

        for stage in range(1, stages + 1):
            # Convert partition into dict of dataframe pieces
            group[(shuffle_group_name, stage, inp)] = (
                shuffle_group,
                (shuffle_join_name, stage - 1, inp),
                column,
                stage - 1,
                k,
                n,
                ignore_index,
                npartitions,
            )

            _concat_list = []
            for i in range(k):
                # Get out each individual dataframe piece from the dicts
                split[(shuffle_split_name, stage, i, inp)] = (
                    getitem,
                    (shuffle_group_name, stage, inp),
                    i,
                )

                _concat_list.append(
                    (
                        shuffle_split_name,
                        stage,
                        inp[stage - 1],
                        insert(inp, stage - 1, i),
                    )
                )

            # concatenate those pieces together, with their friends
            join[(shuffle_join_name, stage, inp)] = (
                _concat,
                _concat_list,
                ignore_index,
            )

        groups.append(group)
        splits.append(split)
        joins.append(join)

        end[(shuffle_token, idx)] = (shuffle_join_name, stages, inp)

    groups.extend(splits)
    groups.extend(joins)

    dsk = toolz.merge(start, end, *(groups))
    graph = HighLevelGraph.from_collections(shuffle_token, dsk, dependencies=[df])

    df2 = new_dd_object(graph, shuffle_token, df._meta, df.divisions)

    if npartitions is not None and npartitions != df.npartitions:
        token = tokenize(df2, npartitions)
        repartition_group_token = "repartition-group-" + token

        dsk = {
            (repartition_group_token, i): (
                shuffle_group_2,
                k,
                column,
                ignore_index,
                npartitions,
            )
            for i, k in enumerate(df2.__dask_keys__())
        }

        repartition_get_name = "repartition-get-" + token

        for p in range(npartitions):
            dsk[(repartition_get_name, p)] = (
                shuffle_group_get,
                (repartition_group_token, p % df.npartitions),
                p,
            )

        graph2 = HighLevelGraph.from_collections(
            repartition_get_name, dsk, dependencies=[df2]
        )
        df3 = new_dd_object(
            graph2, repartition_get_name, df2._meta, [None] * (npartitions + 1)
        )
    else:
        df3 = df2
        df3.divisions = (None,) * (df.npartitions + 1)

    return df3


########################################################
# Various convenience functions to be run by the above #
########################################################


def partitioning_index(df, npartitions):
    """
    Computes a deterministic index mapping each record to a partition.

    Identical rows are mapped to the same partition.

    Parameters
    ----------
    df : DataFrame/Series/Index
    npartitions : int
        The number of partitions to group into.

    Returns
    -------
    partitions : ndarray
        An array of int64 values mapping each record to a partition.
    """
    return hash_object_dispatch(df, index=False) % int(npartitions)


def barrier(args):
    list(args)
    return 0


def cleanup_partd_files(p, keys):
    """
    Cleanup the files in a partd.File dataset.

    Parameters
    ----------
    p : partd.Interface
        File or Encode wrapping a file should be OK.
    keys: List
        Just for scheduling purposes, not actually used.
    """
    import partd

    if isinstance(p, partd.Encode):
        maybe_file = p.partd
    else:
        maybe_file

    if isinstance(maybe_file, partd.File):
        path = maybe_file.path
    else:
        path = None

    if path:
        shutil.rmtree(path, ignore_errors=True)


def collect(p, part, meta, barrier_token):
    """ Collect partitions from partd, yield dataframes """
    with ensure_cleanup_on_exception(p):
        res = p.get(part)
        return res if len(res) > 0 else meta


def set_partitions_pre(s, divisions):
    partitions = divisions.searchsorted(s, side="right") - 1
    partitions[(s >= divisions.iloc[-1]).values] = len(divisions) - 2
    return partitions


def shuffle_group_2(df, cols, ignore_index, nparts):
    if not len(df):
        return {}, df

    if isinstance(cols, str):
        cols = [cols]

    if cols and cols[0] == "_partitions":
        ind = df[cols[0]].astype(np.int32)
    else:
        ind = (
            hash_object_dispatch(df[cols] if cols else df, index=False) % int(nparts)
        ).astype(np.int32)

    n = ind.max() + 1
    result2 = group_split_dispatch(df, ind.values.view(), n, ignore_index=ignore_index)
    return result2, df.iloc[:0]


def shuffle_group_get(g_head, i):
    g, head = g_head
    if i in g:
        return g[i]
    else:
        return head


def shuffle_group(df, cols, stage, k, npartitions, ignore_index, nfinal):
    """Splits dataframe into groups

    The group is determined by their final partition, and which stage we are in
    in the shuffle

    Parameters
    ----------
    df: DataFrame
    cols: str or list
        Column name(s) on which to split the dataframe. If ``cols`` is not
        "_partitions", hashing will be used to determine target partition
    stage: int
        We shuffle dataframes with many partitions we in a few stages to avoid
        a quadratic number of tasks.  This number corresponds to which stage
        we're in, starting from zero up to some small integer
    k: int
        Desired number of splits from this dataframe
    npartition: int
        Total number of output partitions for the full dataframe
    nfinal: int
        Total number of output partitions after repartitioning

    Returns
    -------
    out: Dict[int, DataFrame]
        A dictionary mapping integers in {0..k} to dataframes such that the
        hash values of ``df[col]`` are well partitioned.
    """
    if isinstance(cols, str):
        cols = [cols]

    if cols and cols[0] == "_partitions":
        ind = df[cols[0]]
    else:
        ind = hash_object_dispatch(df[cols] if cols else df, index=False)
        if nfinal and nfinal != npartitions:
            ind = ind % int(nfinal)

    c = ind.values
    typ = np.min_scalar_type(npartitions * 2)

    c = np.mod(c, npartitions).astype(typ, copy=False)
    np.floor_divide(c, k ** stage, out=c)
    np.mod(c, k, out=c)

    return group_split_dispatch(df, c, k, ignore_index=ignore_index)


@contextlib.contextmanager
def ensure_cleanup_on_exception(p):
    """Ensure a partd.File is cleaned up.

    We have several tasks referring to a `partd.File` instance. We want to
    ensure that the file is cleaned up if and only if there's an exception
    in the tasks using the `partd.File`.
    """
    try:
        yield
    except Exception:
        # the function (e.g. shuffle_group_3) had an internal exception.
        # We'll cleanup our temporary files and re-raise.
        try:
            p.drop()
        except Exception:
            logger.exception("ignoring exception in ensure_cleanup_on_exception")
        raise


def shuffle_group_3(df, col, npartitions, p):
    with ensure_cleanup_on_exception(p):
        g = df.groupby(col)
        d = {i: g.get_group(i) for i in g.groups}
        p.append(d, fsync=True)


def set_index_post_scalar(df, index_name, drop, column_dtype):
    df2 = df.drop("_partitions", axis=1).set_index(index_name, drop=drop)
    df2.columns = df2.columns.astype(column_dtype)
    return df2


def set_index_post_series(df, index_name, drop, column_dtype):
    df2 = df.drop("_partitions", axis=1).set_index("_index", drop=True)
    df2.index.name = index_name
    df2.columns = df2.columns.astype(column_dtype)
    return df2


def drop_overlap(df, index):
    return df.drop(index) if index in df.index else df


def get_overlap(df, index):
    return df.loc[[index]] if index in df.index else df._constructor()


def fix_overlap(ddf, overlap):
    """ Ensures that the upper bound on each partition of ddf is exclusive """
    name = "fix-overlap-" + tokenize(ddf, overlap)
    n = len(ddf.divisions) - 1
    dsk = {(name, i): (ddf._name, i) for i in range(n)}

    for i in overlap:
        frames = [(get_overlap, (ddf._name, i - 1), ddf.divisions[i]), (ddf._name, i)]
        dsk[(name, i)] = (methods.concat, frames)
        dsk[(name, i - 1)] = (drop_overlap, dsk[(name, i - 1)], ddf.divisions[i])

    graph = HighLevelGraph.from_collections(name, dsk, dependencies=[ddf])
    return new_dd_object(graph, name, ddf._meta, ddf.divisions)


def compute_and_set_divisions(df, **kwargs):
    mins = df.index.map_partitions(M.min, meta=df.index)
    maxes = df.index.map_partitions(M.max, meta=df.index)
    mins, maxes = compute(mins, maxes, **kwargs)

    if (
        sorted(mins) != list(mins)
        or sorted(maxes) != list(maxes)
        or any(a > b for a, b in zip(mins, maxes))
    ):
        raise ValueError(
            "Partitions must be sorted ascending with the index", mins, maxes
        )

    df.divisions = tuple(mins) + (list(maxes)[-1],)

    overlap = [i for i in range(1, len(mins)) if mins[i] >= maxes[i - 1]]
    return fix_overlap(df, overlap) if overlap else df


def set_sorted_index(df, index, drop=True, divisions=None, **kwargs):
    if not isinstance(index, Series):
        meta = df._meta.set_index(index, drop=drop)
    else:
        meta = df._meta.set_index(index._meta, drop=drop)

    result = map_partitions(M.set_index, df, index, drop=drop, meta=meta)

    if not divisions:
        return compute_and_set_divisions(result, **kwargs)
    elif len(divisions) != len(df.divisions):
        msg = (
            "When doing `df.set_index(col, sorted=True, divisions=...)`, "
            "divisions indicates known splits in the index column. In this "
            "case divisions must be the same length as the existing "
            "divisions in `df`\n\n"
            "If the intent is to repartition into new divisions after "
            "setting the index, you probably want:\n\n"
            "`df.set_index(col, sorted=True).repartition(divisions=divisions)`"
        )
        raise ValueError(msg)

    result.divisions = tuple(divisions)
    return result
