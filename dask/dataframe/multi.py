"""
Algorithms that Involve Multiple DataFrames
===========================================

The pandas operations ``concat``, ``join``, and ``merge`` combine multiple
DataFrames.  This module contains analogous algorithms in the parallel case.

There are two important cases:

1.  We combine along a partitioned index
2.  We combine along an unpartitioned index or other column

In the first case we know which partitions of each dataframe interact with
which others.  This lets uss be significantly more clever and efficient.

In the second case each partition from one dataset interacts with all
partitions from the other.  We handle this through a shuffle operation.

Partitioned Joins
-----------------

In the first case where we join along a partitioned index we proceed in the
following stages.

1.  Align the partitions of all inputs to be the same.  This involves a call
    to ``dd.repartition`` which will split up and concat existing partitions as
    necessary.  After this step all inputs have partitions that align with
    each other.  This step is relatively cheap.
    See the function ``align_partitions``.
2.  Remove unnecessary partitions based on the type of join we perform (left,
    right, inner, outer).  We can do this at the partition level before any
    computation happens.  We'll do it again on each partition when we call the
    in-memory function.  See the function ``require``.
3.  Embarrassingly parallel calls to ``pd.concat``, ``pd.join``, or
    ``pd.merge``.  Now that the data is aligned and unnecessary blocks have
    been removed we can rely on the fast in-memory Pandas join machinery to
    execute joins per-partition.  We know that all intersecting records exist
    within the same partition


Hash Joins via Shuffle
----------------------

When we join along an unpartitioned index or along an arbitrary column any
partition from one input might interact with any partition in another.  In
this case we perform a hash-join by shuffling data in each input by that
column.  This results in new inputs with the same partition structure cleanly
separated along that column.

We proceed with hash joins in the following stages:

1.  Shuffle each input on the specified column.  See the function
    ``dask.dataframe.shuffle.shuffle``.
2.  Perform embarrassingly parallel join across shuffled inputs.
"""
from __future__ import absolute_import, division, print_function

from bisect import bisect_left, bisect_right

from toolz import merge_sorted, unique, partial
import toolz
import numpy as np
import pandas as pd

from ..base import tokenize
from .core import (_Frame, Scalar, DataFrame,
                   Index, _maybe_from_pandas)
from .io import from_pandas
from .shuffle import shuffle


def bound(seq, left, right):
    """ Bound sorted list by left and right values

    >>> bound([1, 3, 4, 5, 8, 10, 12], 4, 10)
    [4, 5, 8, 10]
    """
    return seq[bisect_left(seq, left): bisect_right(seq, right)]


def align_partitions(*dfs):
    """ Mutually partition and align DataFrame blocks

    This serves as precursor to multi-dataframe operations like join, concat,
    or merge.

    Parameters
    ----------
    dfs: sequence of dd.DataFrame, dd.Series and dd.base.Scalar
        Sequence of dataframes to be aligned on their index


    Returns
    -------
    dfs: sequence of dd.DataFrame, dd.Series and dd.base.Scalar
        These must have consistent divisions with each other
    divisions: tuple
        Full divisions sequence of the entire result
    result: list
        A list of lists of keys that show which data exist on which
        divisions
    """
    dfs1 = [df for df in dfs if isinstance(df, _Frame)]
    if len(dfs) == 0:
        raise ValueError("dfs contains no DataFrame and Series")
    divisions = list(unique(merge_sorted(*[df.divisions for df in dfs1])))
    dfs2 = [df.repartition(divisions, force=True)
            if isinstance(df, _Frame) else df for df in dfs]

    result = list()
    inds = [0 for df in dfs]
    for d in divisions[:-1]:
        L = list()
        for i, df in enumerate(dfs2):
            if isinstance(df, _Frame):
                j = inds[i]
                divs = df.divisions
                if j < len(divs) - 1 and divs[j] == d:
                    L.append((df._name, inds[i]))
                    inds[i] += 1
                else:
                    L.append(None)
            else: # Scalar has no divisions
                L.append(None)
        result.append(L)
    return dfs2, tuple(divisions), result


def _maybe_align_partitions(args):
    """ Align DataFrame blocks if divisions are different """
    # passed to align_partitions
    indexer, dasks = zip(*[x for x in enumerate(args)
                           if isinstance(x[1], (_Frame, Scalar))])

    # to get current divisions
    dfs = [df for df in dasks if isinstance(df, _Frame)]
    if len(dfs) == 0:
        # no need to align
        return args

    divisions = dfs[0].divisions
    if not all(df.divisions == divisions for df in dfs):
        dasks, _, _ = align_partitions(*dasks)
        for i, d in zip(indexer, dasks):
            args[i] = d
    return args


def require(divisions, parts, required=None):
    """ Clear out divisions where required components are not present

    In left, right, or inner joins we exclude portions of the dataset if one
    side or the other is not present.  We can achieve this at the partition
    level as well

    >>> divisions = [1, 3, 5, 7, 9]
    >>> parts = [(('a', 0), None),
    ...          (('a', 1), ('b', 0)),
    ...          (('a', 2), ('b', 1)),
    ...          (None, ('b', 2))]

    >>> divisions2, parts2 = require(divisions, parts, required=[0])
    >>> divisions2
    (1, 3, 5, 7)
    >>> parts2  # doctest: +NORMALIZE_WHITESPACE
    ((('a', 0), None),
     (('a', 1), ('b', 0)),
     (('a', 2), ('b', 1)))

    >>> divisions2, parts2 = require(divisions, parts, required=[1])
    >>> divisions2
    (3, 5, 7, 9)
    >>> parts2  # doctest: +NORMALIZE_WHITESPACE
    ((('a', 1), ('b', 0)),
     (('a', 2), ('b', 1)),
     (None, ('b', 2)))

    >>> divisions2, parts2 = require(divisions, parts, required=[0, 1])
    >>> divisions2
    (3, 5, 7)
    >>> parts2  # doctest: +NORMALIZE_WHITESPACE
    ((('a', 1), ('b', 0)),
     (('a', 2), ('b', 1)))
    """
    if not required:
        return divisions, parts
    for i in required:
        present = [j for j, p in enumerate(parts) if p[i] is not None]
        divisions = tuple(divisions[min(present): max(present) + 2])
        parts = tuple(parts[min(present): max(present) + 1])
    return divisions, parts


###############################################################
# Join / Merge
###############################################################

required = {'left': [0], 'right': [1], 'inner': [0, 1], 'outer': []}

def join_indexed_dataframes(lhs, rhs, how='left', lsuffix='', rsuffix=''):
    """ Join two partitiond dataframes along their index """
    (lhs, rhs), divisions, parts = align_partitions(lhs, rhs)
    divisions, parts = require(divisions, parts, required[how])

    left_empty = lhs._pd
    right_empty = rhs._pd

    name = 'join-indexed-' + tokenize(lhs, rhs, how, lsuffix, rsuffix)

    dsk = dict()
    for i, (a, b) in enumerate(parts):
        if a is None and how in ('right', 'outer'):
            a = left_empty
        if b is None and how in ('left', 'outer'):
            b = right_empty

        dsk[(name, i)] = (pd.DataFrame.join, a, b, None, how,
                          lsuffix, rsuffix)

    # dummy result
    dummy = left_empty.join(right_empty, on=None, how=how,
                            lsuffix=lsuffix, rsuffix=rsuffix)
    return DataFrame(toolz.merge(lhs.dask, rhs.dask, dsk),
                     name, dummy, divisions)


def _pdmerge(left, right, how, left_on, right_on,
             left_index, right_index, suffixes,
             default_left_columns, default_right_columns):

    if not len(left):
        left = pd.DataFrame(columns=default_left_columns)

    if not len(right):
        right = pd.DataFrame(columns=default_right_columns)

    result = pd.merge(left, right, how=how,
                      left_on=left_on, right_on=right_on,
                      left_index=left_index, right_index=right_index,
                      suffixes=suffixes)
    return result


def hash_join(lhs, left_on, rhs, right_on, how='inner',
              npartitions=None, suffixes=('_x', '_y')):
    """ Join two DataFrames on particular columns with hash join

    This shuffles both datasets on the joined column and then performs an
    embarassingly parallel join partition-by-partition

    >>> hash_join(a, 'id', rhs, 'id', how='left', npartitions=10)  # doctest: +SKIP
    """
    if npartitions is None:
        npartitions = max(lhs.npartitions, rhs.npartitions)

    lhs2 = shuffle(lhs, left_on, npartitions)
    rhs2 = shuffle(rhs, right_on, npartitions)

    if isinstance(left_on, Index):
        left_on = None
        left_index = True
    else:
        left_index = False

    if isinstance(right_on, Index):
        right_on = None
        right_index = True
    else:
        right_index = False

    # dummy result
    dummy = pd.merge(lhs._pd, rhs._pd, how, None,
                     left_on=left_on, right_on=right_on,
                     left_index=left_index, right_index=right_index,
                     suffixes=suffixes)

    merger = partial(_pdmerge, suffixes=suffixes,
                     default_left_columns=list(lhs.columns),
                     default_right_columns=list(rhs.columns))

    if isinstance(left_on, list):
        left_on = (list, tuple(left_on))
    if isinstance(right_on, list):
        right_on = (list, tuple(right_on))

    token = tokenize(lhs, left_on, rhs, right_on, left_index, right_index,
                     how, npartitions, suffixes)
    name = 'hash-join-' + token

    dsk = dict(((name, i), (merger, (lhs2._name, i), (rhs2._name, i),
                            how, left_on, right_on,
                            left_index, right_index))
                for i in range(npartitions))

    divisions = [None] * (npartitions + 1)
    return DataFrame(toolz.merge(lhs2.dask, rhs2.dask, dsk),
                     name, dummy, divisions)


def _pdconcat(dfs, axis=0, join='outer'):
    """ Concatenate caring empty Series """

    # Concat with empty Series with axis=1 will not affect to the
    # result. Special handling is needed in each partition
    if axis == 1:
        # becahse dfs is a generator, once convert to list
        dfs = list(dfs)

        if join == 'outer':
            # outer concat should keep all empty Series

            # input must include one non-empty data at least
            # because of the alignment
            first = [df for df in dfs if len(df) > 0][0]

            def _pad(base, fillby):
                if isinstance(base, pd.Series) and len(base) == 0:
                    # use aligned index to keep index for outer concat
                    return pd.Series([np.nan] * len(fillby),
                                      index=fillby.index, name=base.name)
                else:
                    return base

            dfs = [_pad(df, first) for df in dfs]
        else:
            # inner concat should result in empty if any input is empty
            if any(len(df) == 0 for df in dfs):
                dfs = [pd.DataFrame(columns=df.columns)
                       if isinstance(df, pd.DataFrame) else
                       pd.Series(name=df.name) for df in dfs]

    return pd.concat(dfs, axis=axis, join=join)


def concat_indexed_dataframes(dfs, axis=0, join='outer'):
    """ Concatenate indexed dataframes together along the index """

    if join not in ('inner', 'outer'):
        raise ValueError("'join' must be 'inner' or 'outer'")

    from dask.dataframe.core import _emulate
    dummy = _emulate(pd.concat, dfs, axis=axis, join=join)

    dfs = _maybe_from_pandas(dfs)
    dfs2, divisions, parts = align_partitions(*dfs)
    empties = [df._pd for df in dfs]

    parts2 = [[df if df is not None else empty
               for df, empty in zip(part, empties)]
              for part in parts]

    name = 'concat-indexed-' + tokenize(join, *dfs)
    dsk = dict(((name, i), (_pdconcat, part, axis, join))
                for i, part in enumerate(parts2))

    return _Frame(toolz.merge(dsk, *[df.dask for df in dfs2]),
                  name, dummy, divisions)


def merge(left, right, how='inner', on=None, left_on=None, right_on=None,
          left_index=False, right_index=False, suffixes=('_x', '_y'),
          npartitions=None):

    if not on and not left_on and not right_on and not left_index and not right_index:
        on = [c for c in left.columns if c in right.columns]
        if not on:
            left_index = right_index = True

    if on and not left_on and not right_on:
        left_on = right_on = on
        on = None

    if (isinstance(left, (pd.Series, pd.DataFrame)) and
        isinstance(right, (pd.Series, pd.DataFrame))):
        return pd.merge(left, right, how=how, on=on, left_on=left_on,
                        right_on=right_on, left_index=left_index,
                        right_index=right_index, suffixes=suffixes)

    # Transform pandas objects into dask.dataframe objects
    if isinstance(left, (pd.Series, pd.DataFrame)):
        if right_index and left_on:  # change to join on index
            left = left.set_index(left[left_on])
            left_on = False
            left_index = True
        left = from_pandas(left, npartitions=1)  # turn into DataFrame

    if isinstance(right, (pd.Series, pd.DataFrame)):
        if left_index and right_on:  # change to join on index
            right = right.set_index(right[right_on])
            right_on = False
            right_index = True
        right = from_pandas(right, npartitions=1)  # turn into DataFrame

    # Both sides are now dd.DataFrame or dd.Series objects

    if left_index and right_index:  # Do indexed join
        return join_indexed_dataframes(left, right, how=how,
                                       lsuffix=suffixes[0], rsuffix=suffixes[1])

    else:                           # Do hash join
        return hash_join(left, left.index if left_index else left_on,
                         right, right.index if right_index else right_on,
                         how, npartitions, suffixes)


###############################################################
# Concat
###############################################################

def _concat_dfs(dfs, name, join='outer'):
    """ Internal function to concat dask dict and DataFrame.columns """
    dsk = dict()
    i = 0

    empties = [df._pd for df in dfs]
    dummy = pd.concat(empties, axis=0, join=join)

    if isinstance(dummy, pd.Series):
        # in this case, input must be all Series. No need to care DataFrame.
        columns = pd.Index([])
    else:
        columns = dummy.columns
        if len(columns) == 0:
            raise ValueError('Failed to concat, no columns remain')

    for df in dfs:
        if isinstance(df, DataFrame):
            # filter DataFrame columns
            if not columns.equals(df.columns):
                df = df[[c for c in columns if c in df.columns]]
        # Series must remain if output columns exist

        dsk = toolz.merge(dsk, df.dask)
        for key in df._keys():
            dsk[(name, i)] = key
            i += 1

    return dsk, dummy


def concat(dfs, axis=0, join='outer', interleave_partitions=False):
    """ Concatenate DataFrames along rows.

    - When axis=0 (default), concatenate DataFrames row-wise:

      - If all divisions are known and ordered, concatenate DataFrames keeping
        divisions. When divisions are not ordered, specifying
        interleave_partition=True allows concatenate divisions each by each.

      - If any of division is unknown, concatenate DataFrames resetting its
        division to unknown (None)

    - When axis=1, concatenate DataFrames column-wise:

      - Allowed if all divisions are known.

      - If any of division is unknown, it raises ValueError.

    Parameters
    ----------

    dfs : list
        List of dask.DataFrames to be concatenated
    axis : {0, 1, 'index', 'columns'}, default 0
        The axis to concatenate along
    join : {'inner', 'outer'}, default 'outer'
        How to handle indexes on other axis
    interleave_partitions : bool, default False
        Whether to concatenate DataFrames ignoring its order. If True, every
        divisions are concatenated each by each.

    Examples
    --------

    If all divisions are known and ordered, divisions are kept.

    >>> a                                               # doctest: +SKIP
    dd.DataFrame<x, divisions=(1, 3, 5)>
    >>> b                                               # doctest: +SKIP
    dd.DataFrame<y, divisions=(6, 8, 10)>
    >>> dd.concat([a, b])                               # doctest: +SKIP
    dd.DataFrame<concat-..., divisions=(1, 3, 6, 8, 10)>

    Unable to concatenate if divisions are not ordered.

    >>> a                                               # doctest: +SKIP
    dd.DataFrame<x, divisions=(1, 3, 5)>
    >>> b                                               # doctest: +SKIP
    dd.DataFrame<y, divisions=(2, 3, 6)>
    >>> dd.concat([a, b])                               # doctest: +SKIP
    ValueError: All inputs have known divisions which cannnot be concatenated
    in order. Specify interleave_partitions=True to ignore order

    Specify interleave_partitions=True to ignore the division order.

    >>> dd.concat([a, b], interleave_partitions=True)   # doctest: +SKIP
    dd.DataFrame<concat-..., divisions=(1, 2, 3, 5, 6)>

    If any of division is unknown, the result division will be unknown

    >>> a                                               # doctest: +SKIP
    dd.DataFrame<x, divisions=(None, None)>
    >>> b                                               # doctest: +SKIP
    dd.DataFrame<y, divisions=(1, 4, 10)>
    >>> dd.concat([a, b])                               # doctest: +SKIP
    dd.DataFrame<concat-..., divisions=(None, None, None, None)>
    """
    if not isinstance(dfs, list):
        dfs = [dfs]
    if len(dfs) == 0:
        raise ValueError('Input must be a list longer than 0')
    if len(dfs) == 1:
        return dfs[0]

    if join not in ('inner', 'outer'):
        raise ValueError("'join' must be 'inner' or 'outer'")

    axis = DataFrame._validate_axis(axis)
    dasks = [df for df in dfs if isinstance(df, _Frame)]

    if all(df.known_divisions for df in dasks):

        dfs = _maybe_from_pandas(dfs)
        if axis == 1:
            return concat_indexed_dataframes(dfs, axis=axis, join=join)
        else:
            # must be converted here to check whether divisions can be
            # concatenated
            dfs = _maybe_from_pandas(dfs)
            # each DataFrame's division must be greater than previous one
            if all(dfs[i].divisions[-1] < dfs[i + 1].divisions[0]
                   for i in range(len(dfs) - 1)):
                name = 'concat-{0}'.format(tokenize(*dfs))
                dsk, dummy = _concat_dfs(dfs, name, join=join)

                divisions = []
                for df in dfs[:-1]:
                    # remove last to concatenate with next
                    divisions += df.divisions[:-1]
                divisions += dfs[-1].divisions
                return _Frame(toolz.merge(dsk, *[df.dask for df in dfs]),
                              name, dummy, divisions)
            else:
                if interleave_partitions:
                    return concat_indexed_dataframes(dfs, join=join)

                raise ValueError('All inputs have known divisions which cannnot '
                                 'be concatenated in order. Specify '
                                 'interleave_partitions=True to ignore order')

    else:
        if axis == 1:
             raise ValueError('Unable to concatenate DataFrame with unknown '
                              'division specifying axis=1')
        else:
            # concat will not regard Series as row
            dfs = _maybe_from_pandas(dfs)
            name = 'concat-{0}'.format(tokenize(*dfs))
            dsk, dummy = _concat_dfs(dfs, name, join=join)

            divisions = [None] * (sum([df.npartitions for df in dfs]) + 1)
            return _Frame(toolz.merge(dsk, *[df.dask for df in dfs]),
                          name, dummy, divisions)



###############################################################
# Append
###############################################################

def _append(df, other, divisions):
    """ Internal function to append 2 dd.DataFrame/Series instances """
    # ToDo: might be possible to merge the logic to concat,
    token = tokenize(df, other)
    name = '{0}-append--{1}'.format(df._token_prefix, token)
    dsk = {}

    npart = df.npartitions
    for i in range(npart):
        dsk[(name, i)] = (df._name, i)
    for j in range(other.npartitions):
        dsk[(name, npart + j)] = (other._name, j)
    dsk = toolz.merge(dsk, df.dask, other.dask)
    dummy = df._pd.append(other._pd)
    return _Frame(dsk, name, dummy, divisions)


###############################################################
# Melt
###############################################################

def melt(frame, id_vars=None, value_vars=None, var_name=None,
         value_name='value', col_level=None):

    from dask.dataframe.core import no_default

    return frame.map_partitions(pd.melt, no_default, id_vars=id_vars,
                                value_vars=value_vars,
                                var_name=var_name, value_name=value_name,
                                col_level=col_level, token='melt')

