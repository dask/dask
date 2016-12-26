from __future__ import absolute_import, division, print_function

from collections import Iterator
from distutils.version import LooseVersion
import operator
from operator import getitem, setitem
from pprint import pformat
import uuid
import warnings

from toolz import merge, partial, first, unique, partition_all
import pandas as pd
from pandas.util.decorators import cache_readonly
import numpy as np

try:
    from chest import Chest as Cache
except ImportError:
    Cache = dict

from .. import array as da
from .. import core
from ..array.core import partial_by_order
from .. import threaded
from ..compatibility import apply, operator_div, bind_method, PY3
from ..utils import (repr_long_list, random_state_data,
                     pseudorandom, derived_from, funcname, memory_repr,
                     put_lines, M)
from ..base import Base, compute, tokenize, normalize_token
from ..async import get_sync
from . import methods
from .utils import (meta_nonempty, make_meta, insert_meta_param_description,
                    raise_on_meta_error)
from .hashing import hash_pandas_object

no_default = '__no_default__'

pd.computation.expressions.set_use_numexpr(False)


def _concat(args, **kwargs):
    """ Generic concat operation """
    if not args:
        return args
    if isinstance(first(core.flatten(args)), np.ndarray):
        return da.core.concatenate3(args)
    if isinstance(args[0], (pd.DataFrame, pd.Series)):
        args2 = [arg for arg in args if len(arg)]
        if not args2:
            return args[0]
        return pd.concat(args2)
    if isinstance(args[0], (pd.Index)):
        args = [arg for arg in args if len(arg)]
        return args[0].append(args[1:])
    try:
        return pd.Series(args)
    except:
        return args


def _get_return_type(meta):
    if isinstance(meta, _Frame):
        meta = meta._meta

    if isinstance(meta, pd.Series):
        return Series
    elif isinstance(meta, pd.DataFrame):
        return DataFrame
    elif isinstance(meta, pd.Index):
        return Index
    return Scalar


def new_dd_object(dsk, _name, meta, divisions):
    """Generic constructor for dask.dataframe objects.

    Decides the appropriate output class based on the type of `meta` provided.
    """
    return _get_return_type(meta)(dsk, _name, meta, divisions)


def optimize(dsk, keys, **kwargs):
    from .optimize import optimize
    return optimize(dsk, keys, **kwargs)


def finalize(results):
    return _concat(results)


class Scalar(Base):
    """ A Dask object to represent a pandas scalar"""

    _optimize = staticmethod(optimize)
    _default_get = staticmethod(threaded.get)
    _finalize = staticmethod(first)

    def __init__(self, dsk, name, meta, divisions=None):
        # divisions is ignored, only present to be compatible with other
        # objects.
        self.dask = dsk
        self._name = name
        meta = make_meta(meta)
        if isinstance(meta, (pd.DataFrame, pd.Series, pd.Index)):
            raise ValueError("Expected meta to specify scalar, got "
                             "{0}".format(type(meta).__name__))
        self._meta = meta

    @property
    def _meta_nonempty(self):
        return self._meta

    @property
    def dtype(self):
        return self._meta.dtype

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        if not hasattr(self._meta, 'dtype'):
            o.remove('dtype')  # dtype only in `dir` if available
        return list(o)

    @property
    def divisions(self):
        """Dummy divisions to be compat with Series and DataFrame"""
        return [None, None]

    def __repr__(self):
        name = self._name if len(self._name) < 10 else self._name[:7] + '...'
        if hasattr(self._meta, 'dtype'):
            extra = ', dtype=%s' % self._meta.dtype
        else:
            extra = ', type=%s' % type(self._meta).__name__
        return "dd.Scalar<%s%s>" % (name, extra)

    def __array__(self):
        # array interface is required to support pandas instance + Scalar
        # Otherwise, above op results in pd.Series of Scalar (object dtype)
        return np.asarray(self.compute())

    @property
    def _args(self):
        return (self.dask, self._name, self._meta)

    def __getstate__(self):
        return self._args

    def __setstate__(self, state):
        self.dask, self._name, self._meta = state

    @property
    def key(self):
        return (self._name, 0)

    def _keys(self):
        return [self.key]

    @classmethod
    def _get_unary_operator(cls, op):
        def f(self):
            name = funcname(op) + '-' + tokenize(self)
            dsk = {(name, 0): (op, (self._name, 0))}
            meta = op(self._meta_nonempty)
            return Scalar(merge(dsk, self.dask), name, meta)
        return f

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        return lambda self, other: _scalar_binary(op, self, other, inv=inv)


def _scalar_binary(op, self, other, inv=False):
    name = '{0}-{1}'.format(funcname(op), tokenize(self, other))

    dsk = self.dask
    return_type = _get_return_type(other)

    if isinstance(other, Scalar):
        dsk = merge(dsk, other.dask)
        other_key = (other._name, 0)
    elif isinstance(other, Base):
        return NotImplemented
    else:
        other_key = other

    if inv:
        dsk.update({(name, 0): (op, other_key, (self._name, 0))})
    else:
        dsk.update({(name, 0): (op, (self._name, 0), other_key)})

    other_meta = make_meta(other)
    other_meta_nonempty = meta_nonempty(other_meta)
    if inv:
        meta = op(other_meta_nonempty, self._meta_nonempty)
    else:
        meta = op(self._meta_nonempty, other_meta_nonempty)

    if return_type is not Scalar:
        return return_type(dsk, name, meta,
                           [other.index.min(), other.index.max()])
    else:
        return Scalar(dsk, name, meta)


class _Frame(Base):
    """ Superclass for DataFrame and Series

    Parameters
    ----------

    dsk: dict
        The dask graph to compute this DataFrame
    name: str
        The key prefix that specifies which keys in the dask comprise this
        particular DataFrame / Series
    meta: pandas.DataFrame, pandas.Series, or pandas.Index
        An empty pandas object with names, dtypes, and indices matching the
        expected output.
    divisions: tuple of index values
        Values along which we partition our blocks on the index
    """

    _optimize = staticmethod(optimize)
    _default_get = staticmethod(threaded.get)
    _finalize = staticmethod(finalize)

    def __init__(self, dsk, name, meta, divisions):
        self.dask = dsk
        self._name = name
        meta = make_meta(meta)
        if not isinstance(meta, self._partition_type):
            raise ValueError("Expected meta to specify type {0}, got type "
                             "{1}".format(self._partition_type.__name__,
                                          type(meta).__name__))
        self._meta = meta
        self.divisions = tuple(divisions)

    @property
    def _constructor(self):
        return new_dd_object

    @property
    def npartitions(self):
        """Return number of partitions"""
        return len(self.divisions) - 1

    @property
    def size(self):
        return self.reduction(methods.size, np.sum, token='size', meta=int,
                              split_every=False)

    @property
    def _meta_nonempty(self):
        """ A non-empty version of `_meta` with fake data."""
        return meta_nonempty(self._meta)

    @property
    def _args(self):
        return (self.dask, self._name, self._meta, self.divisions)

    def __getstate__(self):
        return self._args

    def __setstate__(self, state):
        self.dask, self._name, self._meta, self.divisions = state

    def copy(self):
        """ Make a copy of the dataframe

        This is strictly a shallow copy of the underlying computational graph.
        It does not affect the underlying data
        """
        return new_dd_object(self.dask, self._name,
                             self._meta, self.divisions)

    def _keys(self):
        return [(self._name, i) for i in range(self.npartitions)]

    def __repr__(self):
        name = self._name if len(self._name) < 10 else self._name[:7] + '...'
        if self.known_divisions:
            div_text = ', divisions=%s' % repr_long_list(self.divisions)
        else:
            div_text = ''

        return ("dd.%s<%s, npartitions=%s%s>" %
                (self.__class__.__name__, name, self.npartitions, div_text))

    def __array__(self, dtype=None, **kwargs):
        self._computed = self.compute()
        x = np.array(self._computed)
        return x

    def __array_wrap__(self, array, context=None):
        raise NotImplementedError

    @property
    def _elemwise(self):
        return elemwise

    @property
    def index(self):
        """Return dask Index instance"""
        name = self._name + '-index'
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))

        return Index(merge(dsk, self.dask), name,
                     self._meta.index, self.divisions)

    def reset_index(self, drop=False):
        """Reset the index to the default index.

        Note that unlike in ``pandas``, the reset ``dask.dataframe`` index will
        not be monotonically increasing from 0. Instead, it will restart at 0
        for each partition (e.g. ``index1 = [0, ..., 10], index2 = [0, ...]``).
        This is due to the inability to statically know the full length of the
        index.

        For DataFrame with multi-level index, returns a new DataFrame with
        labeling information in the columns under the index names, defaulting
        to 'level_0', 'level_1', etc. if any are None. For a standard index,
        the index name will be used (if set), otherwise a default 'index' or
        'level_0' (if 'index' is already taken) will be used.

        Parameters
        ----------
        drop : boolean, default False
            Do not try to insert index into dataframe columns.
        """
        return self.map_partitions(M.reset_index, drop=drop).clear_divisions()

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    def clear_divisions(self):
        divisions = (None,) * (self.npartitions + 1)
        return type(self)(self.dask, self._name, self._meta, divisions)

    def get_partition(self, n):
        """Get a dask DataFrame/Series representing the `nth` partition."""
        if 0 <= n < self.npartitions:
            name = 'get-partition-%s-%s' % (str(n), self._name)
            dsk = {(name, 0): (self._name, n)}
            divisions = self.divisions[n:n + 2]
            return new_dd_object(merge(self.dask, dsk), name,
                                 self._meta, divisions)
        else:
            msg = "n must be 0 <= n < {0}".format(self.npartitions)
            raise ValueError(msg)

    def cache(self, cache=Cache):
        """ Evaluate Dataframe and store in local cache

        Uses chest by default to store data on disk
        """
        warnings.warn("Deprecation Warning: The `cache` method is deprecated, "
                      "and will be removed in the next release. To achieve "
                      "the same behavior, either write to disk or use "
                      "`Client.persist`, from `dask.distributed`.")
        if callable(cache):
            cache = cache()

        # Evaluate and store in cache
        name = 'cache' + uuid.uuid1().hex
        dsk = dict(((name, i), (setitem, cache, (tuple, list(key)), key))
                   for i, key in enumerate(self._keys()))
        self._get(merge(dsk, self.dask), list(dsk.keys()))

        # Create new dataFrame pointing to that cache
        name = 'from-cache-' + self._name
        dsk2 = dict(((name, i), (getitem, cache, (tuple, list(key))))
                    for i, key in enumerate(self._keys()))
        return new_dd_object(dsk2, name, self._meta, self.divisions)

    @derived_from(pd.DataFrame)
    def drop_duplicates(self, split_every=None, split_out=1, **kwargs):
        # Let pandas error on bad inputs
        self._meta_nonempty.drop_duplicates(**kwargs)
        if 'subset' in kwargs and kwargs['subset'] is not None:
            split_out_setup = split_out_on_cols
            split_out_setup_kwargs = {'cols': kwargs['subset']}
        else:
            split_out_setup = split_out_setup_kwargs = None

        if kwargs.get('keep', True) is False:
            raise NotImplementedError("drop_duplicates with keep=False")

        chunk = M.drop_duplicates
        return aca(self, chunk=chunk, aggregate=chunk, meta=self._meta,
                   token='drop-duplicates', split_every=split_every,
                   split_out=split_out, split_out_setup=split_out_setup,
                   split_out_setup_kwargs=split_out_setup_kwargs, **kwargs)

    def __len__(self):
        return self.reduction(len, np.sum, token='len', meta=int,
                              split_every=False).compute()

    @insert_meta_param_description(pad=12)
    def map_partitions(self, func, *args, **kwargs):
        """ Apply Python function on each DataFrame partition.

        Parameters
        ----------
        func : function
            Function applied to each partition.
        args, kwargs :
            Arguments and keywords to pass to the function. The partition will
            be the first argument, and these will be passed *after*.
        $META

        Examples
        --------
        Given a DataFrame, Series, or Index, such as:

        >>> import dask.dataframe as dd
        >>> df = pd.DataFrame({'x': [1, 2, 3, 4, 5],
        ...                    'y': [1., 2., 3., 4., 5.]})
        >>> ddf = dd.from_pandas(df, npartitions=2)

        One can use ``map_partitions`` to apply a function on each partition.
        Extra arguments and keywords can optionally be provided, and will be
        passed to the function after the partition.

        Here we apply a function with arguments and keywords to a DataFrame,
        resulting in a Series:

        >>> def myadd(df, a, b=1):
        ...     return df.x + df.y + a + b
        >>> res = ddf.map_partitions(myadd, 1, b=2)
        >>> res.dtype
        dtype('float64')

        By default, dask tries to infer the output metadata by running your
        provided function on some fake data. This works well in many cases, but
        can sometimes be expensive, or even fail. To avoid this, you can
        manually specify the output metadata with the ``meta`` keyword. This
        can be specified in many forms, for more information see
        ``dask.dataframe.utils.make_meta``.

        Here we specify the output is a Series with no name, and dtype
        ``float64``:

        >>> res = ddf.map_partitions(myadd, 1, b=2, meta=(None, 'f8'))

        Here we map a function that takes in a DataFrame, and returns a
        DataFrame with a new column:

        >>> res = ddf.map_partitions(lambda df: df.assign(z=df.x * df.y))
        >>> res.dtypes
        x      int64
        y    float64
        z    float64
        dtype: object

        As before, the output metadata can also be specified manually. This
        time we pass in a ``dict``, as the output is a DataFrame:

        >>> res = ddf.map_partitions(lambda df: df.assign(z=df.x * df.y),
        ...                          meta={'x': 'i8', 'y': 'f8', 'z': 'f8'})

        In the case where the metadata doesn't change, you can also pass in
        the object itself directly:

        >>> res = ddf.map_partitions(lambda df: df.head(), meta=df)
        """
        return map_partitions(func, self, *args, **kwargs)

    @insert_meta_param_description(pad=12)
    def map_overlap(self, func, before, after, *args, **kwargs):
        """Apply a function to each partition, sharing rows with adjacent partitions.

        This can be useful for implementing windowing functions such as
        ``df.rolling(...).mean()`` or ``df.diff()``.

        Parameters
        ----------
        func : function
            Function applied to each partition.
        before : int
            The number of rows to prepend to partition ``i`` from the end of
            partition ``i - 1``.
        after : int
            The number of rows to append to partition ``i`` from the beginning
            of partition ``i + 1``.
        args, kwargs :
            Arguments and keywords to pass to the function. The partition will
            be the first argument, and these will be passed *after*.
        $META

        Notes
        -----
        Given positive integers ``before`` and ``after``, and a function
        ``func``, ``map_overlap`` does the following:

        1. Prepend ``before`` rows to each partition ``i`` from the end of
           partition ``i - 1``. The first partition has no rows prepended.

        2. Append ``after`` rows to each partition ``i`` from the beginning of
           partition ``i + 1``. The last partition has no rows appended.

        3. Apply ``func`` to each partition, passing in any extra ``args`` and
           ``kwargs`` if provided.

        4. Trim ``before`` rows from the beginning of all but the first
           partition.

        5. Trim ``after`` rows from the end of all but the last partition.

        Note that the index and divisions are assumed to remain unchanged.

        Examples
        --------
        Given a DataFrame, Series, or Index, such as:

        >>> import dask.dataframe as dd
        >>> df = pd.DataFrame({'x': [1, 2, 4, 7, 11],
        ...                    'y': [1., 2., 3., 4., 5.]})
        >>> ddf = dd.from_pandas(df, npartitions=2)

        A rolling sum with a trailing moving window of size 2 can be computed by
        overlapping 2 rows before each partition, and then mapping calls to
        ``df.rolling(2).sum()``:

        >>> ddf.compute()
            x    y
        0   1  1.0
        1   2  2.0
        2   4  3.0
        3   7  4.0
        4  11  5.0
        >>> ddf.map_overlap(lambda df: df.rolling(2).sum(), 2, 0).compute()
              x    y
        0   NaN  NaN
        1   3.0  3.0
        2   6.0  5.0
        3  11.0  7.0
        4  18.0  9.0

        The pandas ``diff`` method computes a discrete difference shifted by a
        number of periods (can be positive or negative). This can be
        implemented by mapping calls to ``df.diff`` to each partition after
        prepending/appending that many rows, depending on sign:

        >>> def diff(df, periods=1):
        ...     before, after = (periods, 0) if periods > 0 else (0, -periods)
        ...     return df.map_overlap(lambda df, periods=1: df.diff(periods),
        ...                           periods, 0, periods=periods)
        >>> diff(ddf, 1).compute()
             x    y
        0  NaN  NaN
        1  1.0  1.0
        2  2.0  1.0
        3  3.0  1.0
        4  4.0  1.0
        """
        from .rolling import map_overlap
        return map_overlap(func, self, before, after, *args, **kwargs)

    @insert_meta_param_description(pad=12)
    def reduction(self, chunk, aggregate=None, combine=None, meta=no_default,
                  token=None, split_every=None, chunk_kwargs=None,
                  aggregate_kwargs=None, combine_kwargs=None, **kwargs):
        """Generic row-wise reductions.

        Parameters
        ----------
        chunk : callable
            Function to operate on each partition. Should return a
            ``pandas.DataFrame``, ``pandas.Series``, or a scalar.
        aggregate : callable, optional
            Function to operate on the concatenated result of ``chunk``. If not
            specified, defaults to ``chunk``. Used to do the final aggregation
            in a tree reduction.

            The input to ``aggregate`` depends on the output of ``chunk``.
            If the output of ``chunk`` is a:

            - scalar: Input is a Series, with one row per partition.
            - Series: Input is a DataFrame, with one row per partition. Columns
              are the rows in the output series.
            - DataFrame: Input is a DataFrame, with one row per partition.
              Columns are the columns in the output dataframes.

            Should return a ``pandas.DataFrame``, ``pandas.Series``, or a
            scalar.
        combine : callable, optional
            Function to operate on intermediate concatenated results of
            ``chunk`` in a tree-reduction. If not provided, defaults to
            ``aggregate``. The input/output requirements should match that of
            ``aggregate`` described above.
        $META
        token : str, optional
            The name to use for the output keys.
        split_every : int, optional
            Group partitions into groups of this size while performing a
            tree-reduction. If set to False, no tree-reduction will be used,
            and all intermediates will be concatenated and passed to
            ``aggregate``. Default is 8.
        chunk_kwargs : dict, optional
            Keyword arguments to pass on to ``chunk`` only.
        aggregate_kwargs : dict, optional
            Keyword arguments to pass on to ``aggregate`` only.
        combine_kwargs : dict, optional
            Keyword arguments to pass on to ``combine`` only.
        kwargs :
            All remaining keywords will be passed to ``chunk``, ``combine``,
            and ``aggregate``.

        Examples
        --------
        >>> import pandas as pd
        >>> import dask.dataframe as dd
        >>> df = pd.DataFrame({'x': range(50), 'y': range(50, 100)})
        >>> ddf = dd.from_pandas(df, npartitions=4)

        Count the number of rows in a DataFrame. To do this, count the number
        of rows in each partition, then sum the results:

        >>> res = ddf.reduction(lambda x: x.count(),
        ...                     aggregate=lambda x: x.sum())
        >>> res.compute()
        x    50
        y    50
        dtype: int64

        Count the number of rows in a Series with elements greater than or
        equal to a value (provided via a keyword).

        >>> def count_greater(x, value=0):
        ...     return (x >= value).sum()
        >>> res = ddf.x.reduction(count_greater, aggregate=lambda x: x.sum(),
        ...                       chunk_kwargs={'value': 25})
        >>> res.compute()
        25

        Aggregate both the sum and count of a Series at the same time:

        >>> def sum_and_count(x):
        ...     return pd.Series({'sum': x.sum(), 'count': x.count()})
        >>> res = ddf.x.reduction(sum_and_count, aggregate=lambda x: x.sum())
        >>> res.compute()
        count      50
        sum      1225
        dtype: int64

        Doing the same, but for a DataFrame. Here ``chunk`` returns a
        DataFrame, meaning the input to ``aggregate`` is a DataFrame with an
        index with non-unique entries for both 'x' and 'y'. We groupby the
        index, and sum each group to get the final result.

        >>> def sum_and_count(x):
        ...     return pd.DataFrame({'sum': x.sum(), 'count': x.count()})
        >>> res = ddf.reduction(sum_and_count,
        ...                     aggregate=lambda x: x.groupby(level=0).sum())
        >>> res.compute()
           count   sum
        x     50  1225
        y     50  3725
        """
        if aggregate is None:
            aggregate = chunk

        if combine is None:
            if combine_kwargs:
                raise ValueError("`combine_kwargs` provided with no `combine`")
            combine = aggregate
            combine_kwargs = aggregate_kwargs

        chunk_kwargs = chunk_kwargs.copy() if chunk_kwargs else {}
        chunk_kwargs['aca_chunk'] = chunk

        combine_kwargs = combine_kwargs.copy() if combine_kwargs else {}
        combine_kwargs['aca_combine'] = combine

        aggregate_kwargs = aggregate_kwargs.copy() if aggregate_kwargs else {}
        aggregate_kwargs['aca_aggregate'] = aggregate

        return aca(self, chunk=_reduction_chunk, aggregate=_reduction_aggregate,
                   combine=_reduction_combine, meta=meta, token=token,
                   split_every=split_every, chunk_kwargs=chunk_kwargs,
                   aggregate_kwargs=aggregate_kwargs,
                   combine_kwargs=combine_kwargs, **kwargs)

    @derived_from(pd.DataFrame)
    def pipe(self, func, *args, **kwargs):
        # Taken from pandas:
        # https://github.com/pydata/pandas/blob/master/pandas/core/generic.py#L2698-L2707
        if isinstance(func, tuple):
            func, target = func
            if target in kwargs:
                raise ValueError('%s is both the pipe target and a keyword '
                                 'argument' % target)
            kwargs[target] = self
            return func(*args, **kwargs)
        else:
            return func(self, *args, **kwargs)

    def random_split(self, frac, random_state=None):
        """ Pseudorandomly split dataframe into different pieces row-wise

        Parameters
        ----------
        frac : list
            List of floats that should sum to one.
        random_state: int or np.random.RandomState
            If int create a new RandomState with this as the seed
        Otherwise draw from the passed RandomState

        Examples
        --------

        50/50 split

        >>> a, b = df.random_split([0.5, 0.5])  # doctest: +SKIP

        80/10/10 split, consistent random_state

        >>> a, b, c = df.random_split([0.8, 0.1, 0.1], random_state=123)  # doctest: +SKIP

        See Also
        --------
        dask.DataFrame.sample
        """
        if not np.allclose(sum(frac), 1):
            raise ValueError("frac should sum to 1")
        state_data = random_state_data(self.npartitions, random_state)
        token = tokenize(self, frac, random_state)
        name = 'split-' + token
        dsk = {(name, i): (pd_split, (self._name, i), frac, state)
               for i, state in enumerate(state_data)}

        out = []
        for i in range(len(frac)):
            name2 = 'split-%d-%s' % (i, token)
            dsk2 = {(name2, j): (getitem, (name, j), i)
                    for j in range(self.npartitions)}
            out.append(type(self)(merge(self.dask, dsk, dsk2), name2,
                                  self._meta, self.divisions))
        return out

    def head(self, n=5, npartitions=1, compute=True):
        """ First n rows of the dataset

        Parameters
        ----------
        n : int, optional
            The number of rows to return. Default is 5.
        npartitions : int, optional
            Elements are only taken from the first ``npartitions``, with a
            default of 1. If there are fewer than ``n`` rows in the first
            ``npartitions`` a warning will be raised and any found rows
            returned. Pass -1 to use all partitions.
        compute : bool, optional
            Whether to compute the result, default is True.
        """
        if npartitions <= -1:
            npartitions = self.npartitions
        if npartitions > self.npartitions:
            msg = "only {} partitions, head received {}"
            raise ValueError(msg.format(self.npartitions, npartitions))

        name = 'head-%d-%d-%s' % (npartitions, n, self._name)

        if npartitions > 1:
            name_p = 'head-partial-%d-%s' % (n, self._name)

            dsk = {}
            for i in range(npartitions):
                dsk[(name_p, i)] = (M.head, (self._name, i), n)

            concat = (_concat, [(name_p, i) for i in range(npartitions)])
            dsk[(name, 0)] = (safe_head, concat, n)
        else:
            dsk = {(name, 0): (safe_head, (self._name, 0), n)}

        result = new_dd_object(merge(self.dask, dsk), name, self._meta,
                               [self.divisions[0], self.divisions[npartitions]])

        if compute:
            result = result.compute()
        return result

    def tail(self, n=5, compute=True):
        """ Last n rows of the dataset

        Caveat, the only checks the last n rows of the last partition.
        """
        name = 'tail-%d-%s' % (n, self._name)
        dsk = {(name, 0): (M.tail, (self._name, self.npartitions - 1), n)}

        result = new_dd_object(merge(self.dask, dsk), name,
                               self._meta, self.divisions[-2:])

        if compute:
            result = result.compute()
        return result

    @property
    def loc(self):
        """ Purely label-location based indexer for selection by label.

        >>> df.loc["b"]  # doctest: +SKIP
        >>> df.loc["b":"d"]  # doctest: +SKIP"""
        from .indexing import _LocIndexer
        return _LocIndexer(self)

    # NOTE: `iloc` is not implemented because of performance concerns.
    # see https://github.com/dask/dask/pull/507

    def repartition(self, divisions=None, npartitions=None, force=False):
        """ Repartition dataframe along new divisions

        Parameters
        ----------
        divisions : list, optional
            List of partitions to be used. If specified npartitions will be
            ignored.
        npartitions : int, optional
            Number of partitions of output, must be less than npartitions of
            input. Only used if divisions isn't specified.
        force : bool, default False
            Allows the expansion of the existing divisions.
            If False then the new divisions lower and upper bounds must be
            the same as the old divisions.

        Examples
        --------
        >>> df = df.repartition(npartitions=10)  # doctest: +SKIP
        >>> df = df.repartition(divisions=[0, 5, 10, 20])  # doctest: +SKIP
        """
        if npartitions is not None and divisions is not None:
            warnings.warn("When providing both npartitions and divisions to "
                          "repartition only npartitions is used.")

        if npartitions is not None:
            if npartitions > self.npartitions:
                raise ValueError("Can only repartition to fewer partitions")
            return repartition_npartitions(self, npartitions)
        elif divisions is not None:
            return repartition(self, divisions, force=force)
        else:
            raise ValueError(
                "Provide either divisions= or npartitions= to repartition")

    @derived_from(pd.DataFrame)
    def fillna(self, value=None, method=None, limit=None, axis=None):
        axis = self._validate_axis(axis)
        if method is None and limit is not None:
            raise NotImplementedError("fillna with set limit and method=None")
        if isinstance(value, _Frame):
            test_value = value._meta_nonempty.values[0]
        else:
            test_value = value
        meta = self._meta_nonempty.fillna(value=test_value, method=method,
                                          limit=limit, axis=axis)

        if axis == 1 or method is None:
            return self.map_partitions(M.fillna, value, method=method,
                                       limit=limit, axis=axis, meta=meta)

        if method in ('pad', 'ffill'):
            method = 'ffill'
            skip_check = 0
            before, after = 1 if limit is None else limit, 0
        else:
            method = 'bfill'
            skip_check = self.npartitions - 1
            before, after = 0, 1 if limit is None else limit

        if limit is None:
            name = 'fillna-chunk-' + tokenize(self, method)
            dsk = {(name, i): (methods.fillna_check, (self._name, i),
                               method, i != skip_check)
                   for i in range(self.npartitions)}
            parts = new_dd_object(merge(dsk, self.dask), name, meta,
                                  self.divisions)
        else:
            parts = self

        return parts.map_overlap(M.fillna, before, after, method=method,
                                 limit=limit, meta=meta)

    @derived_from(pd.DataFrame)
    def ffill(self, axis=None, limit=None):
        return self.fillna(method='ffill', limit=limit, axis=axis)

    @derived_from(pd.DataFrame)
    def bfill(self, axis=None, limit=None):
        return self.fillna(method='bfill', limit=limit, axis=axis)

    def sample(self, frac, replace=False, random_state=None):
        """ Random sample of items

        Parameters
        ----------
        frac : float, optional
            Fraction of axis items to return.
        replace: boolean, optional
            Sample with or without replacement. Default = False.
        random_state: int or ``np.random.RandomState``
            If int we create a new RandomState with this as the seed
            Otherwise we draw from the passed RandomState

        See Also
        --------
            dask.DataFrame.random_split, pd.DataFrame.sample
        """

        if random_state is None:
            random_state = np.random.RandomState()

        name = 'sample-' + tokenize(self, frac, replace, random_state)

        state_data = random_state_data(self.npartitions, random_state)
        dsk = {(name, i): (methods.sample, (self._name, i), state, frac, replace)
               for i, state in enumerate(state_data)}

        return new_dd_object(merge(self.dask, dsk), name,
                             self._meta, self.divisions)

    def to_hdf(self, path_or_buf, key, mode='a', append=False, get=None, **kwargs):
        """ See dd.to_hdf docstring for more information """
        from .io import to_hdf
        return to_hdf(self, path_or_buf, key, mode, append, get=get, **kwargs)

    def to_parquet(self, path, *args, **kwargs):
        """ See dd.to_parquet docstring for more information """
        from .io import to_parquet
        return to_parquet(path, self, *args, **kwargs)

    def to_csv(self, filename, **kwargs):
        """ See dd.to_csv docstring for more information """
        from .io import to_csv
        return to_csv(self, filename, **kwargs)

    def to_delayed(self):
        """ See dd.to_delayed docstring for more information """
        return to_delayed(self)

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self)
        else:
            return lambda self, other: elemwise(op, self, other)

    def rolling(self, window, min_periods=None, freq=None, center=False,
                win_type=None, axis=0):
        """Provides rolling transformations.

        Parameters
        ----------
        window : int
           Size of the moving window. This is the number of observations used
           for calculating the statistic. The window size must not be so large
           as to span more than one adjacent partition.
        min_periods : int, default None
            Minimum number of observations in window required to have a value
            (otherwise result is NA).
        center : boolean, default False
            Set the labels at the center of the window.
        win_type : string, default None
            Provide a window type. The recognized window types are identical
            to pandas.
        axis : int, default 0

        Returns
        -------
        a Rolling object on which to call a method to compute a statistic

        Notes
        -----
        The `freq` argument is not supported.
        """
        from dask.dataframe.rolling import Rolling

        if not isinstance(window, int):
            raise ValueError('window must be an integer')
        if window < 0:
            raise ValueError('window must be >= 0')

        if min_periods is not None:
            if not isinstance(min_periods, int):
                raise ValueError('min_periods must be an integer')
            if min_periods < 0:
                raise ValueError('min_periods must be >= 0')

        return Rolling(self, window=window, min_periods=min_periods,
                       freq=freq, center=center, win_type=win_type, axis=axis)

    @derived_from(pd.DataFrame)
    def diff(self, periods=1, axis=0):
        axis = self._validate_axis(axis)
        if not isinstance(periods, int):
            raise TypeError("periods must be an integer")

        if axis == 1:
            return self.map_partitions(M.diff, token='diff', periods=periods,
                                       axis=1)

        before, after = (periods, 0) if periods > 0 else (0, -periods)
        return self.map_overlap(M.diff, before, after, token='diff',
                                periods=periods)

    @derived_from(pd.DataFrame)
    def shift(self, periods=1, freq=None, axis=0):
        axis = self._validate_axis(axis)
        if not isinstance(periods, int):
            raise TypeError("periods must be an integer")

        if axis == 1:
            return self.map_partitions(M.shift, token='shift', periods=periods,
                                       freq=freq, axis=1)

        if freq is None:
            before, after = (periods, 0) if periods > 0 else (0, -periods)
            return self.map_overlap(M.shift, before, after, token='shift',
                                    periods=periods)

        # Let pandas error on invalid arguments
        meta = self._meta_nonempty.shift(periods, freq=freq)
        out = self.map_partitions(M.shift, token='shift', periods=periods,
                                  freq=freq, meta=meta)
        return maybe_shift_divisions(out, periods, freq=freq)

    def _reduction_agg(self, name, axis=None, skipna=True,
                       split_every=False):
        axis = self._validate_axis(axis)

        meta = getattr(self._meta_nonempty, name)(axis=axis, skipna=skipna)
        token = self._token_prefix + name

        method = getattr(M, name)
        if axis == 1:
            return self.map_partitions(method, meta=meta,
                                       token=token, skipna=skipna, axis=axis)
        else:
            return self.reduction(method, meta=meta, token=token,
                                  skipna=skipna, axis=axis,
                                  split_every=split_every)

    @derived_from(pd.DataFrame)
    def abs(self):
        meta = self._meta_nonempty.abs()
        return self.map_partitions(M.abs, meta=meta)

    @derived_from(pd.DataFrame)
    def all(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('all', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def any(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('any', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def sum(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('sum', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def prod(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('prod', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def max(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('max', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def min(self, axis=None, skipna=True, split_every=False):
        return self._reduction_agg('min', axis=axis, skipna=skipna,
                                   split_every=split_every)

    @derived_from(pd.DataFrame)
    def idxmax(self, axis=None, skipna=True, split_every=False):
        fn = 'idxmax'
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.idxmax(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(M.idxmax, self, meta=meta,
                                  token=self._token_prefix + fn,
                                  skipna=skipna, axis=axis)
        else:
            scalar = not isinstance(meta, pd.Series)
            return aca([self], chunk=idxmaxmin_chunk, aggregate=idxmaxmin_agg,
                       combine=idxmaxmin_combine, meta=meta,
                       aggregate_kwargs={'scalar': scalar},
                       token=self._token_prefix + fn, split_every=split_every,
                       skipna=skipna, fn=fn)

    @derived_from(pd.DataFrame)
    def idxmin(self, axis=None, skipna=True, split_every=False):
        fn = 'idxmin'
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.idxmax(axis=axis)
        if axis == 1:
            return map_partitions(M.idxmin, self, meta=meta,
                                  token=self._token_prefix + fn,
                                  skipna=skipna, axis=axis)
        else:
            scalar = not isinstance(meta, pd.Series)
            return aca([self], chunk=idxmaxmin_chunk, aggregate=idxmaxmin_agg,
                       combine=idxmaxmin_combine, meta=meta,
                       aggregate_kwargs={'scalar': scalar},
                       token=self._token_prefix + fn, split_every=split_every,
                       skipna=skipna, fn=fn)

    @derived_from(pd.DataFrame)
    def count(self, axis=None, split_every=False):
        axis = self._validate_axis(axis)
        token = self._token_prefix + 'count'
        if axis == 1:
            meta = self._meta_nonempty.count(axis=axis)
            return self.map_partitions(M.count, meta=meta, token=token,
                                       axis=axis)
        else:
            meta = self._meta_nonempty.count()
            return self.reduction(M.count, aggregate=M.sum, meta=meta,
                                  token=token, split_every=split_every)

    @derived_from(pd.DataFrame)
    def mean(self, axis=None, skipna=True, split_every=False):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.mean(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(M.mean, self, meta=meta,
                                  token=self._token_prefix + 'mean',
                                  axis=axis, skipna=skipna)
        else:
            num = self._get_numeric_data()
            s = num.sum(skipna=skipna, split_every=split_every)
            n = num.count(split_every=split_every)
            name = self._token_prefix + 'mean-%s' % tokenize(self, axis, skipna)
            return map_partitions(methods.mean_aggregate, s, n,
                                  token=name, meta=meta)

    @derived_from(pd.DataFrame)
    def var(self, axis=None, skipna=True, ddof=1, split_every=False):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.var(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(M.var, self, meta=meta,
                                  token=self._token_prefix + 'var',
                                  axis=axis, skipna=skipna, ddof=ddof)
        else:
            num = self._get_numeric_data()
            x = 1.0 * num.sum(skipna=skipna, split_every=split_every)
            x2 = 1.0 * (num ** 2).sum(skipna=skipna, split_every=split_every)
            n = num.count(split_every=split_every)
            name = self._token_prefix + 'var'
            return map_partitions(methods.var_aggregate, x2, x, n,
                                  token=name, meta=meta, ddof=ddof)

    @derived_from(pd.DataFrame)
    def std(self, axis=None, skipna=True, ddof=1, split_every=False):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.std(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(M.std, self, meta=meta,
                                  token=self._token_prefix + 'std',
                                  axis=axis, skipna=skipna, ddof=ddof)
        else:
            v = self.var(skipna=skipna, ddof=ddof, split_every=split_every)
            name = self._token_prefix + 'std'
            return map_partitions(np.sqrt, v, meta=meta, token=name)

    @derived_from(pd.DataFrame)
    def sem(self, axis=None, skipna=None, ddof=1, split_every=False):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.sem(axis=axis, skipna=skipna, ddof=ddof)
        if axis == 1:
            return map_partitions(M.sem, self, meta=meta,
                                  token=self._token_prefix + 'sem',
                                  axis=axis, skipna=skipna, ddof=ddof)
        else:
            num = self._get_numeric_data()
            v = num.var(skipna=skipna, ddof=ddof, split_every=split_every)
            n = num.count(split_every=split_every)
            name = self._token_prefix + 'sem'
            return map_partitions(np.sqrt, v / n, meta=meta, token=name)

    def quantile(self, q=0.5, axis=0):
        """ Approximate row-wise and precise column-wise quantiles of DataFrame

        Parameters
        ----------

        q : list/array of floats, default 0.5 (50%)
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        axis : {0, 1, 'index', 'columns'} (default 0)
            0 or 'index' for row-wise, 1 or 'columns' for column-wise
        """
        axis = self._validate_axis(axis)
        keyname = 'quantiles-concat--' + tokenize(self, q, axis)

        if axis == 1:
            if isinstance(q, list):
                # Not supported, the result will have current index as columns
                raise ValueError("'q' must be scalar when axis=1 is specified")
            if LooseVersion(pd.__version__) >= '0.19':
                name = q
            else:
                name = None
            meta = pd.Series([], dtype='f8', name=name)
            return map_partitions(M.quantile, self, q, axis,
                                  token=keyname, meta=meta)
        else:
            meta = self._meta.quantile(q, axis=axis)
            num = self._get_numeric_data()
            quantiles = tuple(quantile(self[c], q) for c in num.columns)

            dask = {}
            dask = merge(dask, *[_q.dask for _q in quantiles])
            qnames = [(_q._name, 0) for _q in quantiles]

            if isinstance(quantiles[0], Scalar):
                dask[(keyname, 0)] = (pd.Series, qnames, num.columns)
                divisions = (min(num.columns), max(num.columns))
                return Series(dask, keyname, meta, divisions)
            else:
                dask[(keyname, 0)] = (methods.concat, qnames, 1)
                return DataFrame(dask, keyname, meta, quantiles[0].divisions)

    @derived_from(pd.DataFrame)
    def describe(self, split_every=False):
        # currently, only numeric describe is supported
        num = self._get_numeric_data()

        stats = [num.count(split_every=split_every),
                 num.mean(split_every=split_every),
                 num.std(split_every=split_every),
                 num.min(split_every=split_every),
                 num.quantile([0.25, 0.5, 0.75]),
                 num.max(split_every=split_every)]
        stats_names = [(s._name, 0) for s in stats]

        name = 'describe--' + tokenize(self, split_every)
        dsk = merge(num.dask, *(s.dask for s in stats))
        dsk[(name, 0)] = (methods.describe_aggregate, stats_names)

        return new_dd_object(dsk, name, num._meta, divisions=[None, None])

    def _cum_agg(self, token, chunk, aggregate, axis, skipna=True,
                 chunk_kwargs=None):
        """ Wrapper for cumulative operation """

        axis = self._validate_axis(axis)

        if axis == 1:
            name = '{0}{1}(axis=1)'.format(self._token_prefix, token)
            return self.map_partitions(chunk, token=name, **chunk_kwargs)
        else:
            # cumulate each partitions
            name1 = '{0}{1}-map'.format(self._token_prefix, token)
            cumpart = map_partitions(chunk, self, token=name1, meta=self,
                                     **chunk_kwargs)

            name2 = '{0}{1}-take-last'.format(self._token_prefix, token)
            cumlast = map_partitions(_take_last, cumpart, skipna,
                                     meta=pd.Series([]), token=name2)

            name = '{0}{1}'.format(self._token_prefix, token)
            cname = '{0}{1}-cum-last'.format(self._token_prefix, token)

            # aggregate cumulated partisions and its previous last element
            dask = {}
            dask[(name, 0)] = (cumpart._name, 0)

            for i in range(1, self.npartitions):
                # store each cumulative step to graph to reduce computation
                if i == 1:
                    dask[(cname, i)] = (cumlast._name, i - 1)
                else:
                    # aggregate with previous cumulation results
                    dask[(cname, i)] = (aggregate, (cname, i - 1),
                                        (cumlast._name, i - 1))
                dask[(name, i)] = (aggregate, (cumpart._name, i), (cname, i))
            return new_dd_object(merge(dask, cumpart.dask, cumlast.dask),
                                 name, chunk(self._meta), self.divisions)

    @derived_from(pd.DataFrame)
    def cumsum(self, axis=None, skipna=True):
        return self._cum_agg('cumsum',
                             chunk=M.cumsum,
                             aggregate=operator.add,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cumprod(self, axis=None, skipna=True):
        return self._cum_agg('cumprod',
                             chunk=M.cumprod,
                             aggregate=operator.mul,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cummax(self, axis=None, skipna=True):
        return self._cum_agg('cummax',
                             chunk=M.cummax,
                             aggregate=methods.cummax_aggregate,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cummin(self, axis=None, skipna=True):
        return self._cum_agg('cummin',
                             chunk=M.cummin,
                             aggregate=methods.cummin_aggregate,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def where(self, cond, other=np.nan):
        # cond and other may be dask instance,
        # passing map_partitions via keyword will not be aligned
        return map_partitions(M.where, self, cond, other)

    @derived_from(pd.DataFrame)
    def mask(self, cond, other=np.nan):
        return map_partitions(M.mask, self, cond, other)

    @derived_from(pd.DataFrame)
    def notnull(self):
        return self.map_partitions(M.notnull)

    @derived_from(pd.DataFrame)
    def isnull(self):
        return self.map_partitions(M.isnull)

    @derived_from(pd.DataFrame)
    def astype(self, dtype):
        return self.map_partitions(M.astype, dtype=dtype,
                                   meta=self._meta.astype(dtype))

    @derived_from(pd.Series)
    def append(self, other):
        # because DataFrame.append will override the method,
        # wrap by pd.Series.append docstring

        if isinstance(other, (list, dict)):
            msg = "append doesn't support list or dict input"
            raise NotImplementedError(msg)

        if not isinstance(other, _Frame):
            from .io import from_pandas
            other = from_pandas(other, 1)

        from .multi import _append
        if self.known_divisions and other.known_divisions:
            if self.divisions[-1] < other.divisions[0]:
                divisions = self.divisions[:-1] + other.divisions
                return _append(self, other, divisions)
            else:
                msg = ("Unable to append two dataframes to each other with known "
                       "divisions if those divisions are not ordered. "
                       "The divisions/index of the second dataframe must be "
                       "greater than the divisions/index of the first dataframe.")
                raise ValueError(msg)
        else:
            divisions = [None] * (self.npartitions + other.npartitions + 1)
            return _append(self, other, divisions)

    @derived_from(pd.DataFrame)
    def align(self, other, join='outer', axis=None, fill_value=None):
        meta1, meta2 = _emulate(M.align, self, other, join, axis=axis,
                                fill_value=fill_value)
        aligned = self.map_partitions(M.align, other, join=join, axis=axis,
                                      fill_value=fill_value)

        token = tokenize(self, other, join, axis, fill_value)

        name1 = 'align1-' + token
        dsk1 = dict(((name1, i), (getitem, key, 0))
                    for i, key in enumerate(aligned._keys()))
        dsk1.update(aligned.dask)
        result1 = new_dd_object(dsk1, name1, meta1, aligned.divisions)

        name2 = 'align2-' + token
        dsk2 = dict(((name2, i), (getitem, key, 1))
                    for i, key in enumerate(aligned._keys()))
        dsk2.update(aligned.dask)
        result2 = new_dd_object(dsk2, name2, meta2, aligned.divisions)

        return result1, result2

    @derived_from(pd.DataFrame)
    def combine(self, other, func, fill_value=None, overwrite=True):
        return self.map_partitions(M.combine, other, func,
                                   fill_value=fill_value, overwrite=overwrite)

    @derived_from(pd.DataFrame)
    def combine_first(self, other):
        return self.map_partitions(M.combine_first, other)

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """
        raise NotImplementedError

    @derived_from(pd.DataFrame)
    def resample(self, rule, how=None, closed=None, label=None):
        from .tseries.resample import _resample
        return _resample(self, rule, how=how, closed=closed, label=label)

    @derived_from(pd.DataFrame)
    def first(self, offset):
        # Let pandas error on bad args
        self._meta_nonempty.first(offset)

        if not self.known_divisions:
            raise ValueError("`first` is not implemented for unknown divisions")

        offset = pd.tseries.frequencies.to_offset(offset)
        date = self.divisions[0] + offset
        end = self.loc._partition_of_index_value(date)

        include_right = offset.isAnchored() or not hasattr(offset, '_inc')

        if end == self.npartitions - 1:
            divs = self.divisions
        else:
            divs = self.divisions[:end + 1] + (date,)

        name = 'first-' + tokenize(self, offset)
        dsk = {(name, i): (self._name, i) for i in range(end)}
        dsk[(name, end)] = (methods.boundary_slice, (self._name, end),
                            None, date, include_right, True, 'ix')
        return new_dd_object(merge(self.dask, dsk), name, self, divs)

    @derived_from(pd.DataFrame)
    def last(self, offset):
        # Let pandas error on bad args
        self._meta_nonempty.first(offset)

        if not self.known_divisions:
            raise ValueError("`last` is not implemented for unknown divisions")

        offset = pd.tseries.frequencies.to_offset(offset)
        date = self.divisions[-1] - offset
        start = self.loc._partition_of_index_value(date)

        if start == 0:
            divs = self.divisions
        else:
            divs = (date,) + self.divisions[start + 1:]

        name = 'last-' + tokenize(self, offset)
        dsk = {(name, i + 1): (self._name, j + 1)
               for i, j in enumerate(range(start, self.npartitions))}
        dsk[(name, 0)] = (methods.boundary_slice, (self._name, start),
                          date, None, True, False, 'ix')
        return new_dd_object(merge(self.dask, dsk), name, self, divs)

    def nunique_approx(self, split_every=None):
        """Approximate number of unique rows.

        This method uses the HyperLogLog algorithm for cardinality
        estimation to compute the approximate number of unique rows.
        The approximate error is 0.406%.

        Parameters
        ----------
        split_every : int, optional
            Group partitions into groups of this size while performing a
            tree-reduction. If set to False, no tree-reduction will be used.
            Default is 8.

        Returns
        -------
        a float representing the approximate number of elements
        """
        from . import hyperloglog # here to avoid circular import issues

        return aca([self], chunk=hyperloglog.compute_hll_array,
                   combine=hyperloglog.reduce_state,
                   aggregate=hyperloglog.estimate_count,
                   split_every=split_every, b=16, meta=float)

    @property
    def values(self):
        """ Return a dask.array of the values of this dataframe

        Warning: This creates a dask.array without precise shape information.
        Operations that depend on shape information, like slicing or reshaping,
        will not work.
        """
        from ..array.core import Array
        name = 'values-' + tokenize(self)
        chunks = ((np.nan,) * self.npartitions,)
        x = self._meta.values
        if isinstance(self, DataFrame):
            chunks = chunks + ((x.shape[1],),)
            suffix = (0,)
        else:
            suffix = ()
        dsk = {(name, i) + suffix: (getattr, key, 'values')
               for (i, key) in enumerate(self._keys())}
        return Array(merge(self.dask, dsk), name, chunks, x.dtype)


normalize_token.register((Scalar, _Frame), lambda a: a._name)


class Series(_Frame):
    """ Out-of-core Series object

    Mimics ``pandas.Series``.

    Parameters
    ----------

    dsk: dict
        The dask graph to compute this Series
    _name: str
        The key prefix that specifies which keys in the dask comprise this
        particular Series
    meta: pandas.Series
        An empty ``pandas.Series`` with names, dtypes, and index matching the
        expected output.
    divisions: tuple of index values
        Values along which we partition our blocks on the index

    See Also
    --------

    dask.dataframe.DataFrame
    """

    _partition_type = pd.Series
    _token_prefix = 'series-'

    def __array_wrap__(self, array, context=None):
        if isinstance(context, tuple) and len(context) > 0:
            index = context[1][0].index

        return pd.Series(array, index=index, name=self.name)

    @property
    def name(self):
        return self._meta.name

    @name.setter
    def name(self, name):
        self._meta.name = name
        renamed = _rename_dask(self, name)
        # update myself
        self.dask.update(renamed.dask)
        self._name = renamed._name

    @property
    def ndim(self):
        """ Return dimensionality """
        return 1

    @property
    def dtype(self):
        """ Return data type """
        return self._meta.dtype

    @cache_readonly
    def dt(self):
        from .accessor import DatetimeAccessor
        return DatetimeAccessor(self)

    @cache_readonly
    def cat(self):
        from .accessor import CategoricalAccessor
        return CategoricalAccessor(self)

    @cache_readonly
    def str(self):
        from .accessor import StringAccessor
        return StringAccessor(self)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        if not hasattr(self._meta, 'cat'):
            o.remove('cat')  # cat only in `dir` if available
        return list(o)

    @property
    def nbytes(self):
        return self.reduction(methods.nbytes, np.sum, token='nbytes',
                              meta=int, split_every=False)

    @derived_from(pd.Series)
    def round(self, decimals=0):
        return elemwise(M.round, self, decimals)

    @derived_from(pd.DataFrame)
    def to_timestamp(self, freq=None, how='start', axis=0):
        df = elemwise(M.to_timestamp, self, freq, how, axis)
        df.divisions = tuple(pd.Index(self.divisions).to_timestamp())
        return df

    def quantile(self, q=0.5):
        """ Approximate quantiles of Series

        q : list/array of floats, default 0.5 (50%)
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        """
        return quantile(self, q)

    def _repartition_quantiles(self, npartitions, upsample=1.0):
        """ Approximate quantiles of Series used for repartitioning
        """
        from .partitionquantiles import partition_quantiles
        return partition_quantiles(self, npartitions, upsample=upsample)

    def __getitem__(self, key):
        if isinstance(key, Series) and self.divisions == key.divisions:
            name = 'index-%s' % tokenize(self, key)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                    (key._name, i)))
                       for i in range(self.npartitions))
            return Series(merge(self.dask, key.dask, dsk), name,
                          self._meta, self.divisions)
        raise NotImplementedError()

    @derived_from(pd.DataFrame)
    def _get_numeric_data(self, how='any', subset=None):
        return self

    @derived_from(pd.Series)
    def iteritems(self):
        for i in range(self.npartitions):
            s = self.get_partition(i).compute()
            for item in s.iteritems():
                yield item

    @classmethod
    def _validate_axis(cls, axis=0):
        if axis not in (0, 'index', None):
            raise ValueError('No axis named {0}'.format(axis))
        # convert to numeric axis
        return {None: 0, 'index': 0}.get(axis, axis)

    @derived_from(pd.Series)
    def groupby(self, index, **kwargs):
        from dask.dataframe.groupby import SeriesGroupBy
        return SeriesGroupBy(self, index, **kwargs)

    @derived_from(pd.Series)
    def count(self, split_every=False):
        return super(Series, self).count(split_every=split_every)

    def unique(self, split_every=None, split_out=1):
        """
        Return Series of unique values in the object. Includes NA values.

        Returns
        -------
        uniques : Series
        """
        return aca(self, chunk=methods.unique, aggregate=methods.unique,
                   meta=self._meta, token='unique', split_every=split_every,
                   series_name=self.name, split_out=split_out)

    @derived_from(pd.Series)
    def nunique(self, split_every=None):
        return self.drop_duplicates(split_every=split_every).count()

    @derived_from(pd.Series)
    def value_counts(self, split_every=None, split_out=1):
        return aca(self, chunk=M.value_counts,
                   aggregate=methods.value_counts_aggregate,
                   combine=methods.value_counts_combine,
                   meta=self._meta.value_counts(), token='value-counts',
                   split_every=split_every, split_out=split_out,
                   split_out_setup=split_out_on_index)

    @derived_from(pd.Series)
    def nlargest(self, n=5, split_every=None):
        return aca(self, chunk=M.nlargest, aggregate=M.nlargest,
                   meta=self._meta, token='series-nlargest',
                   split_every=split_every, n=n)

    @derived_from(pd.Series)
    def nsmallest(self, n=5, split_every=None):
        return aca(self, chunk=M.nsmallest, aggregate=M.nsmallest,
                   meta=self._meta, token='series-nsmallest',
                   split_every=split_every, n=n)

    @derived_from(pd.Series)
    def isin(self, other):
        return elemwise(M.isin, self, list(other))

    @derived_from(pd.Series)
    def map(self, arg, na_action=None, meta=no_default):
        if not (isinstance(arg, (pd.Series, dict)) or callable(arg)):
            raise TypeError("arg must be pandas.Series, dict or callable."
                            " Got {0}".format(type(arg)))
        name = 'map-' + tokenize(self, arg, na_action)
        dsk = dict(((name, i), (M.map, k, arg, na_action)) for i, k in
                   enumerate(self._keys()))
        dsk.update(self.dask)
        if meta is no_default:
            meta = _emulate(M.map, self, arg, na_action=na_action)
        else:
            meta = make_meta(meta)

        return Series(dsk, name, meta, self.divisions)

    @derived_from(pd.Series)
    def dropna(self):
        return self.map_partitions(M.dropna)

    @derived_from(pd.Series)
    def between(self, left, right, inclusive=True):
        return self.map_partitions(M.between, left=left,
                                   right=right, inclusive=inclusive)

    @derived_from(pd.Series)
    def clip(self, lower=None, upper=None, out=None):
        if out is not None:
            raise ValueError("'out' must be None")
        # np.clip may pass out
        return self.map_partitions(M.clip, lower=lower, upper=upper)

    @derived_from(pd.Series)
    def clip_lower(self, threshold):
        return self.map_partitions(M.clip_lower, threshold=threshold)

    @derived_from(pd.Series)
    def clip_upper(self, threshold):
        return self.map_partitions(M.clip_upper, threshold=threshold)

    @derived_from(pd.Series)
    def align(self, other, join='outer', axis=None, fill_value=None):
        return super(Series, self).align(other, join=join, axis=axis,
                                         fill_value=fill_value)

    @derived_from(pd.Series)
    def combine(self, other, func, fill_value=None):
        return self.map_partitions(M.combine, other, func,
                                   fill_value=fill_value)

    @derived_from(pd.Series)
    def combine_first(self, other):
        return self.map_partitions(M.combine_first, other)

    def to_bag(self, index=False):
        from .io import to_bag
        return to_bag(self, index)

    @derived_from(pd.Series)
    def to_frame(self, name=None):
        return self.map_partitions(M.to_frame, name,
                                   meta=self._meta.to_frame(name))

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """

        def meth(self, other, level=None, fill_value=None, axis=0):
            if level is not None:
                raise NotImplementedError('level must be None')
            axis = self._validate_axis(axis)
            meta = _emulate(op, self, other, axis=axis, fill_value=fill_value)
            return map_partitions(op, self, other, meta=meta,
                                  axis=axis, fill_value=fill_value)
        meth.__doc__ = op.__doc__
        bind_method(cls, name, meth)

    @classmethod
    def _bind_comparison_method(cls, name, comparison):
        """ bind comparison method like DataFrame.add to this class """

        def meth(self, other, level=None, axis=0):
            if level is not None:
                raise NotImplementedError('level must be None')
            axis = self._validate_axis(axis)
            return elemwise(comparison, self, other, axis=axis)

        meth.__doc__ = comparison.__doc__
        bind_method(cls, name, meth)

    @insert_meta_param_description(pad=12)
    def apply(self, func, convert_dtype=True, meta=no_default,
              name=no_default, args=(), **kwds):
        """ Parallel version of pandas.Series.apply

        Parameters
        ----------
        func : function
            Function to apply
        convert_dtype : boolean, default True
            Try to find better dtype for elementwise function results.
            If False, leave as dtype=object.
        $META
        name : list, scalar or None, optional
            Deprecated, use `meta` instead. If list is given, the result is a
            DataFrame which columns is specified list. Otherwise, the result is
            a Series which name is given scalar or None (no name). If name
            keyword is not given, dask tries to infer the result type using its
            beginning of data. This inference may take some time and lead to
            unexpected result.
        args : tuple
            Positional arguments to pass to function in addition to the value.

        Additional keyword arguments will be passed as keywords to the function.

        Returns
        -------
        applied : Series or DataFrame if func returns a Series.

        Examples
        --------
        >>> import dask.dataframe as dd
        >>> s = pd.Series(range(5), name='x')
        >>> ds = dd.from_pandas(s, npartitions=2)

        Apply a function elementwise across the Series, passing in extra
        arguments in ``args`` and ``kwargs``:

        >>> def myadd(x, a, b=1):
        ...     return x + a + b
        >>> res = ds.apply(myadd, args=(2,), b=1.5)

        By default, dask tries to infer the output metadata by running your
        provided function on some fake data. This works well in many cases, but
        can sometimes be expensive, or even fail. To avoid this, you can
        manually specify the output metadata with the ``meta`` keyword. This
        can be specified in many forms, for more information see
        ``dask.dataframe.utils.make_meta``.

        Here we specify the output is a Series with name ``'x'``, and dtype
        ``float64``:

        >>> res = ds.apply(myadd, args=(2,), b=1.5, meta=('x', 'f8'))

        In the case where the metadata doesn't change, you can also pass in
        the object itself directly:

        >>> res = ds.apply(lambda x: x + 1, meta=ds)

        See Also
        --------
        dask.Series.map_partitions
        """
        if name is not no_default:
            warnings.warn("`name` is deprecated, please use `meta` instead")
            if meta is no_default and isinstance(name, (pd.DataFrame, pd.Series)):
                meta = name

        if meta is no_default:
            msg = ("`meta` is not specified, inferred from partial data. "
                   "Please provide `meta` if the result is unexpected.\n"
                   "  Before: .apply(func)\n"
                   "  After:  .apply(func, meta={'x': 'f8', 'y': 'f8'}) for dataframe result\n"
                   "  or:     .apply(func, meta=('x', 'f8'))            for series result")
            warnings.warn(msg)

            meta = _emulate(M.apply, self._meta_nonempty, func,
                            convert_dtype=convert_dtype,
                            args=args, **kwds)

        return map_partitions(M.apply, self, func,
                              convert_dtype, args, meta=meta, **kwds)

    @derived_from(pd.Series)
    def cov(self, other, min_periods=None, split_every=False):
        from .multi import concat
        if not isinstance(other, Series):
            raise TypeError("other must be a dask.dataframe.Series")
        df = concat([self, other], axis=1)
        return cov_corr(df, min_periods, scalar=True, split_every=split_every)

    @derived_from(pd.Series)
    def corr(self, other, method='pearson', min_periods=None,
             split_every=False):
        from .multi import concat
        if not isinstance(other, Series):
            raise TypeError("other must be a dask.dataframe.Series")
        if method != 'pearson':
            raise NotImplementedError("Only Pearson correlation has been "
                                      "implemented")
        df = concat([self, other], axis=1)
        return cov_corr(df, min_periods, corr=True, scalar=True,
                        split_every=split_every)

    @derived_from(pd.Series)
    def autocorr(self, lag=1, split_every=False):
        if not isinstance(lag, int):
            raise TypeError("lag must be an integer")
        return self.corr(self if lag == 0 else self.shift(lag),
                         split_every=split_every)


class Index(Series):

    _partition_type = pd.Index
    _token_prefix = 'index-'

    @property
    def index(self):
        msg = "'{0}' object has no attribute 'index'"
        raise AttributeError(msg.format(self.__class__.__name__))

    def __array_wrap__(self, array, context=None):
        return pd.Index(array, name=self.name)

    def head(self, n=5, compute=True):
        """ First n items of the Index.

        Caveat, this only checks the first partition.
        """
        name = 'head-%d-%s' % (n, self._name)
        dsk = {(name, 0): (operator.getitem, (self._name, 0), slice(0, n))}

        result = new_dd_object(merge(self.dask, dsk), name,
                               self._meta, self.divisions[:2])

        if compute:
            result = result.compute()
        return result

    @derived_from(pd.Index)
    def max(self, split_every=False):
        return self.reduction(M.max, meta=self._meta_nonempty.max(),
                              token=self._token_prefix + 'max',
                              split_every=split_every)

    @derived_from(pd.Index)
    def min(self, split_every=False):
        return self.reduction(M.min, meta=self._meta_nonempty.min(),
                              token=self._token_prefix + 'min',
                              split_every=split_every)

    def count(self, split_every=False):
        return self.reduction(methods.index_count, np.sum,
                              token='index-count', meta=int,
                              split_every=split_every)

    @derived_from(pd.Index)
    def shift(self, periods=1, freq=None):
        if isinstance(self._meta, pd.PeriodIndex):
            if freq is not None:
                raise ValueError("PeriodIndex doesn't accept `freq` argument")
            meta = self._meta_nonempty.shift(periods)
            out = self.map_partitions(M.shift, periods, meta=meta,
                                      token='shift')
        else:
            # Pandas will raise for other index types that don't implement shift
            meta = self._meta_nonempty.shift(periods, freq=freq)
            out = self.map_partitions(M.shift, periods, token='shift',
                                      meta=meta, freq=freq)
        if freq is None:
            freq = meta.freq
        return maybe_shift_divisions(out, periods, freq=freq)


class DataFrame(_Frame):
    """
    Implements out-of-core DataFrame as a sequence of pandas DataFrames

    Parameters
    ----------

    dask: dict
        The dask graph to compute this DataFrame
    name: str
        The key prefix that specifies which keys in the dask comprise this
        particular DataFrame
    meta: pandas.DataFrame
        An empty ``pandas.DataFrame`` with names, dtypes, and index matching
        the expected output.
    divisions: tuple of index values
        Values along which we partition our blocks on the index
    """

    _partition_type = pd.DataFrame
    _token_prefix = 'dataframe-'

    def __array_wrap__(self, array, context=None):
        if isinstance(context, tuple) and len(context) > 0:
            index = context[1][0].index

        return pd.DataFrame(array, index=index, columns=self.columns)

    @property
    def columns(self):
        return self._meta.columns

    @columns.setter
    def columns(self, columns):
        renamed = _rename_dask(self, columns)
        self._meta = renamed._meta
        self._name = renamed._name
        self.dask.update(renamed.dask)

    def __getitem__(self, key):
        name = 'getitem-%s' % tokenize(self, key)
        if np.isscalar(key) or isinstance(key, tuple):

            if isinstance(self._meta.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                if key not in self._meta.columns:
                    return self.loc[key]

            # error is raised from pandas
            meta = self._meta[_extract_meta(key)]
            dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                       for i in range(self.npartitions))
            return new_dd_object(merge(self.dask, dsk), name,
                                 meta, self.divisions)
        elif isinstance(key, slice):
            return self.loc[key]

        if isinstance(key, list):
            # error is raised from pandas
            meta = self._meta[_extract_meta(key)]

            dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                       for i in range(self.npartitions))
            return new_dd_object(merge(self.dask, dsk), name,
                                 meta, self.divisions)
        if isinstance(key, Series):
            # do not perform dummy calculation, as columns will not be changed.
            #
            if self.divisions != key.divisions:
                from .multi import _maybe_align_partitions
                self, key = _maybe_align_partitions([self, key])
            dsk = {(name, i): (M._getitem_array, (self._name, i), (key._name, i))
                   for i in range(self.npartitions)}
            return new_dd_object(merge(self.dask, key.dask, dsk), name,
                                 self, self.divisions)
        raise NotImplementedError(key)

    def __setitem__(self, key, value):
        if isinstance(key, (tuple, list)):
            df = self.assign(**{k: value[c]
                                for k, c in zip(key, value.columns)})
        else:
            df = self.assign(**{key: value})

        self.dask = df.dask
        self._name = df._name
        self._meta = df._meta

    def __delitem__(self, key):
        result = self.drop([key], axis=1)
        self.dask = result.dask
        self._name = result._name
        self._meta = result._meta

    def __setattr__(self, key, value):
        try:
            columns = object.__getattribute__(self, '_meta').columns
        except AttributeError:
            columns = ()

        if key in columns:
            self[key] = value
        else:
            object.__setattr__(self, key, value)

    def __getattr__(self, key):
        if key in self.columns:
            meta = self._meta[key]
            name = 'getitem-%s' % tokenize(self, key)
            dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                       for i in range(self.npartitions))
            return new_dd_object(merge(self.dask, dsk), name,
                                 meta, self.divisions)
        raise AttributeError("'DataFrame' object has no attribute %r" % key)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(c for c in self.columns if
                 (isinstance(c, pd.compat.string_types) and
                  pd.compat.isidentifier(c)))
        return list(o)

    @property
    def ndim(self):
        """ Return dimensionality """
        return 2

    @property
    def dtypes(self):
        """ Return data types """
        return self._meta.dtypes

    @derived_from(pd.DataFrame)
    def get_dtype_counts(self):
        return self._meta.get_dtype_counts()

    @derived_from(pd.DataFrame)
    def get_ftype_counts(self):
        return self._meta.get_ftype_counts()

    @derived_from(pd.DataFrame)
    def select_dtypes(self, include=None, exclude=None):
        cs = self._meta.select_dtypes(include=include, exclude=exclude).columns
        return self[list(cs)]

    def set_index(self, other, drop=True, sorted=False, **kwargs):
        """
        Set the DataFrame index (row labels) using an existing column

        This realigns the dataset to be sorted by a new column.  This can have a
        significant impact on performance, because joins, groupbys, lookups, etc.
        are all much faster on that column.  However, this performance increase
        comes with a cost, sorting a parallel dataset requires expensive shuffles.
        Often we ``set_index`` once directly after data ingest and filtering and
        then perform many cheap computations off of the sorted dataset.

        This function operates exactly like ``pandas.set_index`` except with
        different performance costs (it is much more expensive).  Under normal
        operation this function does an initial pass over the index column to
        compute approximate qunatiles to serve as future divisions.  It then passes
        over the data a second time, splitting up each input partition into several
        pieces and sharing those pieces to all of the output partitions now in
        sorted order.

        In some cases we can alleviate those costs, for example if your dataset is
        sorted already then we can avoid making many small pieces or if you know
        good values to split the new index column then we can avoid the initial
        pass over the data.  For example if your new index is a datetime index and
        your data is already sorted by day then this entire operation can be done
        for free.  You can control these options with the following parameters.

        Parameters
        ----------
        df: Dask DataFrame
        index: string or Dask Series
        npartitions: int
            The ideal number of output partitions
        shuffle: string, optional
            Either ``'disk'`` for single-node operation or ``'tasks'`` for
            distributed operation.  Will be inferred by your current scheduler.
        sorted: bool, optional
            If the index column is already sorted in increasing order.
            Defaults to False
        divisions: list, optional
            Known values on which to separate index values of the partitions.
            See http://dask.pydata.org/en/latest/dataframe-design.html#partitions
            Defaults to computing this with a single pass over the data
        compute: bool
            Whether or not to trigger an immediate computation. Defaults to True.

        Examples
        --------
        >>> df2 = df.set_index('x')  # doctest: +SKIP
        >>> df2 = df.set_index(d.x)  # doctest: +SKIP
        >>> df2 = df.set_index(d.timestamp, sorted=True)  # doctest: +SKIP

        A common case is when we have a datetime column that we know to be
        sorted and is cleanly divided by day.  We can set this index for free
        by specifying both that the column is pre-sorted and the particular
        divisions along which is is separated

        >>> import pandas as pd
        >>> divisions = pd.date_range('2000', '2010', freq='1D')
        >>> df2 = df.set_index('timestamp', sorted=True, divisions=divisions)  # doctest: +SKIP
        """
        if sorted:
            return set_sorted_index(self, other, drop=drop, **kwargs)
        else:
            from .shuffle import set_index
            return set_index(self, other, drop=drop, **kwargs)

    def set_partition(self, column, divisions, **kwargs):
        """ Set explicit divisions for new column index

        >>> df2 = df.set_partition('new-index-column', divisions=[10, 20, 50])  # doctest: +SKIP

        See Also
        --------
        set_index
        """
        raise Exception("Deprecated, use set_index(..., divisions=...) instead")

    @derived_from(pd.DataFrame)
    def nlargest(self, n=5, columns=None, split_every=None):
        token = 'dataframe-nlargest'
        return aca(self, chunk=M.nlargest, aggregate=M.nlargest,
                   meta=self._meta, token=token, split_every=split_every,
                   n=n, columns=columns)

    @derived_from(pd.DataFrame)
    def nsmallest(self, n=5, columns=None, split_every=None):
        token = 'dataframe-nsmallest'
        return aca(self, chunk=M.nsmallest, aggregate=M.nsmallest,
                   meta=self._meta, token=token, split_every=split_every,
                   n=n, columns=columns)

    @derived_from(pd.DataFrame)
    def groupby(self, key, **kwargs):
        from dask.dataframe.groupby import DataFrameGroupBy
        return DataFrameGroupBy(self, key, **kwargs)

    def categorize(self, columns=None, **kwargs):
        """
        Convert columns of the DataFrame to category dtype

        Parameters
        ----------
        columns : list, optional
            A list of column names to convert to the category type. By
            default any column with an object dtype is converted to a
            categorical.
        kwargs
            Keyword arguments are passed on to compute.

        Notes
        -----
        When dealing with columns of repeated text values converting to
        categorical type is often much more performant, both in terms of memory
        and in writing to disk or communication over the network.

        See also
        --------
        dask.dataframes.categorical.categorize
        """
        from dask.dataframe.categorical import categorize
        return categorize(self, columns, **kwargs)

    @derived_from(pd.DataFrame)
    def assign(self, **kwargs):
        for k, v in kwargs.items():
            if not (isinstance(v, (Series, Scalar, pd.Series)) or
                    np.isscalar(v)):
                raise TypeError("Column assignment doesn't support type "
                                "{0}".format(type(v).__name__))
        pairs = list(sum(kwargs.items(), ()))

        # Figure out columns of the output
        df2 = self._meta.assign(**_extract_meta(kwargs))
        return elemwise(methods.assign, self, *pairs, meta=df2)

    @derived_from(pd.DataFrame)
    def rename(self, index=None, columns=None):
        if index is not None:
            raise ValueError("Cannot rename index.")

        # *args here is index, columns but columns arg is already used
        return self.map_partitions(M.rename, None, columns)

    def query(self, expr, **kwargs):
        """ Blocked version of pd.DataFrame.query

        This is like the sequential version except that this will also happen
        in many threads.  This may conflict with ``numexpr`` which will use
        multiple threads itself.  We recommend that you set numexpr to use a
        single thread

            import numexpr
            numexpr.set_nthreads(1)

        The original docstring follows below:\n
        """ + (pd.DataFrame.query.__doc__
               if pd.DataFrame.query.__doc__ is not None else '')

        name = 'query-%s' % tokenize(self, expr)
        if kwargs:
            name = name + '--' + tokenize(kwargs)
            dsk = dict(((name, i), (apply, M.query,
                                    ((self._name, i), (expr,), kwargs)))
                       for i in range(self.npartitions))
        else:
            dsk = dict(((name, i), (M.query, (self._name, i), expr))
                       for i in range(self.npartitions))

        meta = self._meta.query(expr, **kwargs)
        return new_dd_object(merge(dsk, self.dask), name,
                             meta, self.divisions)

    @derived_from(pd.DataFrame)
    def eval(self, expr, inplace=None, **kwargs):
        if '=' in expr and inplace in (True, None):
            raise NotImplementedError("Inplace eval not supported."
                                      " Please use inplace=False")
        meta = self._meta.eval(expr, inplace=inplace, **kwargs)
        return self.map_partitions(M.eval, expr, meta=meta, inplace=inplace, **kwargs)

    @derived_from(pd.DataFrame)
    def dropna(self, how='any', subset=None):
        return self.map_partitions(M.dropna, how=how, subset=subset)

    @derived_from(pd.DataFrame)
    def clip(self, lower=None, upper=None, out=None):
        if out is not None:
            raise ValueError("'out' must be None")
        return self.map_partitions(M.clip, lower=lower, upper=upper)

    @derived_from(pd.DataFrame)
    def clip_lower(self, threshold):
        return self.map_partitions(M.clip_lower, threshold=threshold)

    @derived_from(pd.DataFrame)
    def clip_upper(self, threshold):
        return self.map_partitions(M.clip_upper, threshold=threshold)

    @derived_from(pd.DataFrame)
    def to_timestamp(self, freq=None, how='start', axis=0):
        df = elemwise(M.to_timestamp, self, freq, how, axis)
        df.divisions = tuple(pd.Index(self.divisions).to_timestamp())
        return df

    def to_castra(self, fn=None, categories=None, sorted_index_column=None,
                  compute=True, get=get_sync):
        """ Write DataFrame to Castra on-disk store

        See https://github.com/blosc/castra for details

        See Also
        --------
        Castra.to_dask
        """
        from .io import to_castra
        return to_castra(self, fn, categories, sorted_index_column,
                         compute=compute, get=get)

    def to_bag(self, index=False):
        """Convert to a dask Bag of tuples of each row.

        Parameters
        ----------
        index : bool, optional
            If True, the index is included as the first element of each tuple.
            Default is False.
        """
        from .io import to_bag
        return to_bag(self, index)

    def _get_numeric_data(self, how='any', subset=None):
        # calculate columns to avoid unnecessary calculation
        numerics = self._meta._get_numeric_data()

        if len(numerics.columns) < len(self.columns):
            name = self._token_prefix + '-get_numeric_data'
            return self.map_partitions(M._get_numeric_data,
                                       meta=numerics, token=name)
        else:
            # use myself if all numerics
            return self

    @classmethod
    def _validate_axis(cls, axis=0):
        if axis not in (0, 1, 'index', 'columns', None):
            raise ValueError('No axis named {0}'.format(axis))
        # convert to numeric axis
        return {None: 0, 'index': 0, 'columns': 1}.get(axis, axis)

    @derived_from(pd.DataFrame)
    def drop(self, labels, axis=0, errors='raise'):
        axis = self._validate_axis(axis)
        if axis == 1:
            return self.map_partitions(M.drop, labels, axis=axis, errors=errors)
        raise NotImplementedError("Drop currently only works for axis=1")

    @derived_from(pd.DataFrame)
    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False, suffixes=('_x', '_y'),
              indicator=False, npartitions=None, shuffle=None):

        if not isinstance(right, (DataFrame, pd.DataFrame)):
            raise ValueError('right must be DataFrame')

        from .multi import merge
        return merge(self, right, how=how, on=on, left_on=left_on,
                     right_on=right_on, left_index=left_index,
                     right_index=right_index, suffixes=suffixes,
                     npartitions=npartitions, indicator=indicator,
                     shuffle=shuffle)

    @derived_from(pd.DataFrame)
    def join(self, other, on=None, how='left',
             lsuffix='', rsuffix='', npartitions=None, shuffle=None):

        if not isinstance(other, (DataFrame, pd.DataFrame)):
            raise ValueError('other must be DataFrame')

        from .multi import merge
        return merge(self, other, how=how,
                     left_index=on is None, right_index=True,
                     left_on=on, suffixes=[lsuffix, rsuffix],
                     npartitions=npartitions, shuffle=shuffle)

    @derived_from(pd.DataFrame)
    def append(self, other):
        if isinstance(other, Series):
            msg = ('Unable to appending dd.Series to dd.DataFrame.'
                   'Use pd.Series to append as row.')
            raise ValueError(msg)
        elif isinstance(other, pd.Series):
            other = other.to_frame().T
        return super(DataFrame, self).append(other)

    @derived_from(pd.DataFrame)
    def iterrows(self):
        for i in range(self.npartitions):
            df = self.get_partition(i).compute()
            for row in df.iterrows():
                yield row

    @derived_from(pd.DataFrame)
    def itertuples(self):
        for i in range(self.npartitions):
            df = self.get_partition(i).compute()
            for row in df.itertuples():
                yield row

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """

        # name must be explicitly passed for div method whose name is truediv

        def meth(self, other, axis='columns', level=None, fill_value=None):
            if level is not None:
                raise NotImplementedError('level must be None')

            axis = self._validate_axis(axis)

            if axis in (1, 'columns'):
                # When axis=1 and other is a series, `other` is transposed
                # and the operator is applied broadcast across rows. This
                # isn't supported with dd.Series.
                if isinstance(other, Series):
                    msg = 'Unable to {0} dd.Series with axis=1'.format(name)
                    raise ValueError(msg)
                elif isinstance(other, pd.Series):
                    # Special case for pd.Series to avoid unwanted partitioning
                    # of other. We pass it in as a kwarg to prevent this.
                    meta = _emulate(op, self, other=other, axis=axis,
                                    fill_value=fill_value)
                    return map_partitions(op, self, other=other, meta=meta,
                                          axis=axis, fill_value=fill_value)

            meta = _emulate(op, self, other, axis=axis, fill_value=fill_value)
            return map_partitions(op, self, other, meta=meta,
                                  axis=axis, fill_value=fill_value)
        meth.__doc__ = op.__doc__
        bind_method(cls, name, meth)

    @classmethod
    def _bind_comparison_method(cls, name, comparison):
        """ bind comparison method like DataFrame.add to this class """

        def meth(self, other, axis='columns', level=None):
            if level is not None:
                raise NotImplementedError('level must be None')
            axis = self._validate_axis(axis)
            return elemwise(comparison, self, other, axis=axis)

        meth.__doc__ = comparison.__doc__
        bind_method(cls, name, meth)

    @insert_meta_param_description(pad=12)
    def apply(self, func, axis=0, args=(), meta=no_default,
              columns=no_default, **kwds):
        """ Parallel version of pandas.DataFrame.apply

        This mimics the pandas version except for the following:

        1.  Only ``axis=1`` is supported (and must be specified explicitly).
        2.  The user should provide output metadata via the `meta` keyword.

        Parameters
        ----------
        func : function
            Function to apply to each column/row
        axis : {0 or 'index', 1 or 'columns'}, default 0
            - 0 or 'index': apply function to each column (NOT SUPPORTED)
            - 1 or 'columns': apply function to each row
        $META
        columns : list, scalar or None
            Deprecated, please use `meta` instead. If list is given, the result
            is a DataFrame which columns is specified list. Otherwise, the
            result is a Series which name is given scalar or None (no name). If
            name keyword is not given, dask tries to infer the result type
            using its beginning of data. This inference may take some time and
            lead to unexpected result
        args : tuple
            Positional arguments to pass to function in addition to the array/series

        Additional keyword arguments will be passed as keywords to the function

        Returns
        -------
        applied : Series or DataFrame

        Examples
        --------
        >>> import dask.dataframe as dd
        >>> df = pd.DataFrame({'x': [1, 2, 3, 4, 5],
        ...                    'y': [1., 2., 3., 4., 5.]})
        >>> ddf = dd.from_pandas(df, npartitions=2)

        Apply a function to row-wise passing in extra arguments in ``args`` and
        ``kwargs``:

        >>> def myadd(row, a, b=1):
        ...     return row.sum() + a + b
        >>> res = ddf.apply(myadd, axis=1, args=(2,), b=1.5)

        By default, dask tries to infer the output metadata by running your
        provided function on some fake data. This works well in many cases, but
        can sometimes be expensive, or even fail. To avoid this, you can
        manually specify the output metadata with the ``meta`` keyword. This
        can be specified in many forms, for more information see
        ``dask.dataframe.utils.make_meta``.

        Here we specify the output is a Series with name ``'x'``, and dtype
        ``float64``:

        >>> res = ddf.apply(myadd, axis=1, args=(2,), b=1.5, meta=('x', 'f8'))

        In the case where the metadata doesn't change, you can also pass in
        the object itself directly:

        >>> res = ddf.apply(lambda row: row + 1, axis=1, meta=ddf)

        See Also
        --------
        dask.DataFrame.map_partitions
        """

        axis = self._validate_axis(axis)

        if axis == 0:
            msg = ("dd.DataFrame.apply only supports axis=1\n"
                   "  Try: df.apply(func, axis=1)")
            raise NotImplementedError(msg)

        if columns is not no_default:
            warnings.warn("`columns` is deprecated, please use `meta` instead")
            if meta is no_default and isinstance(columns, (pd.DataFrame, pd.Series)):
                meta = columns

        if meta is no_default:
            msg = ("`meta` is not specified, inferred from partial data. "
                   "Please provide `meta` if the result is unexpected.\n"
                   "  Before: .apply(func)\n"
                   "  After:  .apply(func, meta={'x': 'f8', 'y': 'f8'}) for dataframe result\n"
                   "  or:     .apply(func, meta=('x', 'f8'))            for series result")
            warnings.warn(msg)

            meta = _emulate(M.apply, self._meta_nonempty, func,
                            axis=axis, args=args, **kwds)

        return map_partitions(M.apply, self, func, axis,
                              False, False, None, args, meta=meta, **kwds)

    @derived_from(pd.DataFrame)
    def applymap(self, func, meta='__no_default__'):
        return elemwise(M.applymap, self, func, meta=meta)

    @derived_from(pd.DataFrame)
    def round(self, decimals=0):
        return elemwise(M.round, self, decimals)

    @derived_from(pd.DataFrame)
    def cov(self, min_periods=None, split_every=False):
        return cov_corr(self, min_periods, split_every=split_every)

    @derived_from(pd.DataFrame)
    def corr(self, method='pearson', min_periods=None, split_every=False):
        if method != 'pearson':
            raise NotImplementedError("Only Pearson correlation has been "
                                      "implemented")
        return cov_corr(self, min_periods, True, split_every=split_every)

    def info(self, buf=None, verbose=False, memory_usage=False):
        """
        Concise summary of a Dask DataFrame.
        """

        if buf is None:
            import sys
            buf = sys.stdout

        lines = [str(type(self))]

        if len(self.columns) == 0:
            lines.append('Index: 0 entries')
            lines.append('Empty %s' % type(self).__name__)
            put_lines(buf, lines)
            return

        # Group and execute the required computations
        computations = {}
        if verbose:
            computations.update({'index': self.index, 'count': self.count()})
        if memory_usage:
            computations.update({'memory_usage': self.map_partitions(M.memory_usage, index=True)})
        computations = dict(zip(computations.keys(), da.compute(*computations.values())))

        column_template = "{0:<%d} {1}" % (self.columns.str.len().max() + 5)

        if verbose:
            index = computations['index']
            counts = computations['count']
            lines.append(index.summary())
            column_template = column_template.format('{0}', '{1} non-null {2}')
            column_info = [column_template.format(*x) for x in zip(self.columns, counts, self.dtypes)]
        else:
            column_info = [column_template.format(*x) for x in zip(self.columns, self.dtypes)]

        lines.append('Data columns (total {} columns):'.format(len(self.columns)))
        lines.extend(column_info)
        dtype_counts = ['%s(%d)' % k for k in sorted(self.dtypes.value_counts().iteritems(), key=str)]
        lines.append('dtypes: {}'.format(', '.join(dtype_counts)))

        if memory_usage:
            memory_int = computations['memory_usage'].sum()
            lines.append('memory usage: {}\n'.format(memory_repr(memory_int)))

        put_lines(buf, lines)

    def pivot_table(self, index=None, columns=None,
                    values=None, aggfunc='mean'):
        """
        Create a spreadsheet-style pivot table as a DataFrame. Target ``columns``
        must have category dtype to infer result's ``columns``.
        ``index``, ``columns``, ``values`` and ``aggfunc`` must be all scalar.

        Parameters
        ----------
        values : scalar
            column to aggregate
        index : scalar
            column to be index
        columns : scalar
            column to be columns
        aggfunc : {'mean', 'sum', 'count'}, default 'mean'

        Returns
        -------
        table : DataFrame
        """
        from .reshape import pivot_table
        return pivot_table(self, index=index, columns=columns, values=values,
                           aggfunc=aggfunc)

    def to_records(self, index=False):
        from .io import to_records
        return to_records(self)


# bind operators
for op in [operator.abs, operator.add, operator.and_, operator_div,
           operator.eq, operator.gt, operator.ge, operator.inv,
           operator.lt, operator.le, operator.mod, operator.mul,
           operator.ne, operator.neg, operator.or_, operator.pow,
           operator.sub, operator.truediv, operator.floordiv, operator.xor]:
    _Frame._bind_operator(op)
    Scalar._bind_operator(op)

for name in ['add', 'sub', 'mul', 'div',
             'truediv', 'floordiv', 'mod', 'pow',
             'radd', 'rsub', 'rmul', 'rdiv',
             'rtruediv', 'rfloordiv', 'rmod', 'rpow']:
    meth = getattr(pd.DataFrame, name)
    DataFrame._bind_operator_method(name, meth)

    meth = getattr(pd.Series, name)
    Series._bind_operator_method(name, meth)

for name in ['lt', 'gt', 'le', 'ge', 'ne', 'eq']:
    meth = getattr(pd.DataFrame, name)
    DataFrame._bind_comparison_method(name, meth)

    meth = getattr(pd.Series, name)
    Series._bind_comparison_method(name, meth)


def elemwise_property(attr, s):
    meta = pd.Series([], dtype=getattr(s._meta, attr).dtype)
    return map_partitions(getattr, s, attr, meta=meta)


for name in ['nanosecond', 'microsecond', 'millisecond', 'second', 'minute',
             'hour', 'day', 'dayofweek', 'dayofyear', 'week', 'weekday',
             'weekofyear', 'month', 'quarter', 'year']:
    setattr(Index, name, property(partial(elemwise_property, name)))


def elemwise(op, *args, **kwargs):
    """ Elementwise operation for dask.Dataframes """
    meta = kwargs.pop('meta', no_default)

    _name = funcname(op) + '-' + tokenize(op, kwargs, *args)

    args = _maybe_from_pandas(args)

    from .multi import _maybe_align_partitions
    args = _maybe_align_partitions(args)
    dasks = [arg for arg in args if isinstance(arg, (_Frame, Scalar))]
    dfs = [df for df in dasks if isinstance(df, _Frame)]
    divisions = dfs[0].divisions
    n = len(divisions) - 1

    other = [(i, arg) for i, arg in enumerate(args)
             if not isinstance(arg, (_Frame, Scalar))]

    # adjust the key length of Scalar
    keys = [d._keys() * n if isinstance(d, Scalar)
            else d._keys() for d in dasks]

    if other:
        dsk = dict(((_name, i),
                   (apply, partial_by_order, list(frs),
                   {'function': op, 'other': other}))
                   for i, frs in enumerate(zip(*keys)))
    else:
        dsk = dict(((_name, i), (op,) + frs) for i, frs in enumerate(zip(*keys)))
    dsk = merge(dsk, *[d.dask for d in dasks])

    if meta is no_default:
        if len(dfs) >= 2 and len(dasks) != len(dfs):
            # should not occur in current funcs
            msg = 'elemwise with 2 or more DataFrames and Scalar is not supported'
            raise NotImplementedError(msg)
        meta = _emulate(op, *args, **kwargs)

    return new_dd_object(dsk, _name, meta, divisions)


def _maybe_from_pandas(dfs):
    from .io import from_pandas
    dfs = [from_pandas(df, 1) if isinstance(df, (pd.Series, pd.DataFrame))
           else df for df in dfs]
    return dfs


def hash_shard(df, nparts, split_out_setup=None, split_out_setup_kwargs=None):
    if split_out_setup:
        h = split_out_setup(df, **(split_out_setup_kwargs or {}))
    else:
        h = df
    h = hash_pandas_object(h, index=False)
    if isinstance(h, pd.Series):
        h = h._values
    h %= nparts
    return {i: df.iloc[h == i] for i in range(nparts)}


def split_out_on_index(df):
    h = df.index
    if isinstance(h, pd.MultiIndex):
        h = pd.DataFrame([], index=h).reset_index()
    return h


def split_out_on_cols(df, cols=None):
    return df[cols]


@insert_meta_param_description
def apply_concat_apply(args, chunk=None, aggregate=None, combine=None,
                       meta=no_default, token=None, chunk_kwargs=None,
                       aggregate_kwargs=None, combine_kwargs=None,
                       split_every=None, split_out=None, split_out_setup=None,
                       split_out_setup_kwargs=None, **kwargs):
    """Apply a function to blocks, then concat, then apply again

    Parameters
    ----------
    args :
        Positional arguments for the `chunk` function. All `dask.dataframe`
        objects should be partitioned and indexed equivalently.
    chunk : function [block-per-arg] -> block
        Function to operate on each block of data
    aggregate : function concatenated-block -> block
        Function to operate on the concatenated result of chunk
    combine : function concatenated-block -> block, optional
        Function to operate on intermediate concatenated results of chunk
        in a tree-reduction. If not provided, defaults to aggregate.
    $META
    token : str, optional
        The name to use for the output keys.
    chunk_kwargs : dict, optional
        Keywords for the chunk function only.
    aggregate_kwargs : dict, optional
        Keywords for the aggregate function only.
    combine_kwargs : dict, optional
        Keywords for the combine function only.
    split_every : int, optional
        Group partitions into groups of this size while performing a
        tree-reduction. If set to False, no tree-reduction will be used,
        and all intermediates will be concatenated and passed to ``aggregate``.
        Default is 8.
    split_out : int, optional
        Number of output partitions. Split occurs after first chunk reduction.
    split_out_setup : callable, optional
        If provided, this function is called on each chunk before performing
        the hash-split. It should return a pandas object, where each row
        (excluding the index) is hashed. If not provided, the chunk is hashed
        as is.
    split_out_setup_kwargs : dict, optional
        Keywords for the `split_out_setup` function only.
    kwargs :
        All remaining keywords will be passed to ``chunk``, ``aggregate``, and
        ``combine``.

    Examples
    --------
    >>> def chunk(a_block, b_block):
    ...     pass

    >>> def agg(df):
    ...     pass

    >>> apply_concat_apply([a, b], chunk=chunk, aggregate=agg)  # doctest: +SKIP
    """
    if chunk_kwargs is None:
        chunk_kwargs = dict()
    if aggregate_kwargs is None:
        aggregate_kwargs = dict()
    chunk_kwargs.update(kwargs)
    aggregate_kwargs.update(kwargs)

    if combine is None:
        if combine_kwargs:
            raise ValueError("`combine_kwargs` provided with no `combine`")
        combine = aggregate
        combine_kwargs = aggregate_kwargs
    else:
        if combine_kwargs is None:
            combine_kwargs = dict()
        combine_kwargs.update(kwargs)

    if not isinstance(args, (tuple, list)):
        args = [args]

    npartitions = set(arg.npartitions for arg in args
                      if isinstance(arg, _Frame))
    if len(npartitions) > 1:
        raise ValueError("All arguments must have same number of partitions")
    npartitions = npartitions.pop()

    if split_every is None:
        split_every = 8
    elif split_every is False:
        split_every = npartitions
    elif split_every < 2 or not isinstance(split_every, int):
        raise ValueError("split_every must be an integer >= 2")

    token_key = tokenize(token or (chunk, aggregate), meta, args,
                         chunk_kwargs, aggregate_kwargs, combine_kwargs,
                         split_every, split_out, split_out_setup,
                         split_out_setup_kwargs)

    # Chunk
    a = '{0}-chunk-{1}'.format(token or funcname(chunk), token_key)
    if len(args) == 1 and isinstance(args[0], _Frame) and not chunk_kwargs:
        dsk = {(a, 0, i, 0): (chunk, key) for i, key in enumerate(args[0]._keys())}
    else:
        dsk = {(a, 0, i, 0): (apply, chunk,
                              [(x._name, i) if isinstance(x, _Frame)
                               else x for x in args], chunk_kwargs)
               for i in range(args[0].npartitions)}

    # Split
    if split_out and split_out > 1:
        split_prefix = 'split-%s' % token_key
        shard_prefix = 'shard-%s' % token_key
        for i in range(args[0].npartitions):
            dsk[(split_prefix, i)] = (hash_shard, (a, 0, i, 0), split_out,
                                      split_out_setup, split_out_setup_kwargs)
            for j in range(split_out):
                dsk[(shard_prefix, 0, i, j)] = (getitem, (split_prefix, i), j)
        a = shard_prefix
    else:
        split_out = 1

    # Combine
    b = '{0}-combine-{1}'.format(token or funcname(combine), token_key)
    k = npartitions
    depth = 0
    while k > split_every:
        for part_i, inds in enumerate(partition_all(split_every, range(k))):
            for j in range(split_out):
                conc = (_concat, [(a, depth, i, j) for i in inds])
                if combine_kwargs:
                    dsk[(b, depth + 1, part_i, j)] = (apply, combine, [conc], combine_kwargs)
                else:
                    dsk[(b, depth + 1, part_i, j)] = (combine, conc)
        k = part_i + 1
        a = b
        depth += 1

    # Aggregate
    for j in range(split_out):
        b = '{0}-agg-{1}'.format(token or funcname(aggregate), token_key)
        conc = (_concat, [(a, depth, i, j) for i in range(k)])
        if aggregate_kwargs:
            dsk[(b, j)] = (apply, aggregate, [conc], aggregate_kwargs)
        else:
            dsk[(b, j)] = (aggregate, conc)

    if meta is no_default:
        meta_chunk = _emulate(apply, chunk, args, chunk_kwargs)
        meta = _emulate(apply, aggregate, [_concat([meta_chunk])],
                        aggregate_kwargs)
    meta = make_meta(meta)

    for arg in args:
        if isinstance(arg, _Frame):
            dsk.update(arg.dask)

    divisions = [None] * (split_out + 1)

    return new_dd_object(dsk, b, meta, divisions)


aca = apply_concat_apply


def _extract_meta(x, nonempty=False):
    """
    Extract internal cache data (``_meta``) from dd.DataFrame / dd.Series
    """
    if isinstance(x, (_Frame, Scalar)):
        return x._meta_nonempty if nonempty else x._meta
    elif isinstance(x, list):
        return [_extract_meta(_x, nonempty) for _x in x]
    elif isinstance(x, tuple):
        return tuple([_extract_meta(_x, nonempty) for _x in x])
    elif isinstance(x, dict):
        res = {}
        for k in x:
            res[k] = _extract_meta(x[k], nonempty)
        return res
    else:
        return x


def _emulate(func, *args, **kwargs):
    """
    Apply a function using args / kwargs. If arguments contain dd.DataFrame /
    dd.Series, using internal cache (``_meta``) for calculation
    """
    with raise_on_meta_error(funcname(func)):
        return func(*_extract_meta(args, True), **_extract_meta(kwargs, True))


@insert_meta_param_description
def map_partitions(func, *args, **kwargs):
    """ Apply Python function on each DataFrame partition.

    Parameters
    ----------
    func : function
        Function applied to each partition.
    args, kwargs :
        Arguments and keywords to pass to the function.  At least one of the
        args should be a Dask.dataframe.
    $META
    """
    meta = kwargs.pop('meta', no_default)
    if meta is not no_default:
        meta = make_meta(meta)

    assert callable(func)
    if 'token' in kwargs:
        name = kwargs.pop('token')
        token = tokenize(meta, *args, **kwargs)
    else:
        name = funcname(func)
        token = tokenize(func, meta, *args, **kwargs)
    name = '{0}-{1}'.format(name, token)

    from .multi import _maybe_align_partitions
    args = _maybe_from_pandas(args)
    args = _maybe_align_partitions(args)

    if meta is no_default:
        meta = _emulate(func, *args, **kwargs)

    if all(isinstance(arg, Scalar) for arg in args):
        dask = {(name, 0):
                (apply, func, (tuple, [(arg._name, 0) for arg in args]), kwargs)}
        return Scalar(merge(dask, *[arg.dask for arg in args]), name, meta)
    elif not isinstance(meta, (pd.Series, pd.DataFrame, pd.Index)):
        # If `meta` is not a pandas object, the concatenated results will be a
        # different type
        meta = _concat([meta])
    meta = make_meta(meta)

    dfs = [df for df in args if isinstance(df, _Frame)]
    dsk = {}
    for i in range(dfs[0].npartitions):
        values = [(arg._name, i if isinstance(arg, _Frame) else 0)
                  if isinstance(arg, (_Frame, Scalar)) else arg for arg in args]
        dsk[(name, i)] = (apply_and_enforce, func, values, kwargs, meta)

    dasks = [arg.dask for arg in args if isinstance(arg, (_Frame, Scalar))]
    return new_dd_object(merge(dsk, *dasks), name, meta, args[0].divisions)


def apply_and_enforce(func, args, kwargs, meta):
    """Apply a function, and enforce the output to match meta

    Ensures the output has the same columns, even if empty."""
    df = func(*args, **kwargs)
    if isinstance(df, (pd.DataFrame, pd.Series, pd.Index)):
        if len(df) == 0:
            return meta
        c = meta.columns if isinstance(df, pd.DataFrame) else meta.name
        return _rename(c, df)
    return df


def _rename(columns, df):
    """
    Rename columns of pd.DataFrame or name of pd.Series.
    Not for dd.DataFrame or dd.Series.

    Parameters
    ----------
    columns : tuple, string, pd.DataFrame or pd.Series
        Column names, Series name or pandas instance which has the
        target column names / name.
    df : pd.DataFrame or pd.Series
        target DataFrame / Series to be renamed
    """
    assert not isinstance(df, _Frame)

    if columns is no_default:
        return df

    if isinstance(columns, Iterator):
        columns = list(columns)

    if isinstance(df, pd.DataFrame):
        if isinstance(columns, pd.DataFrame):
            columns = columns.columns
        if not isinstance(columns, pd.Index):
            columns = pd.Index(columns)
        if (len(columns) == len(df.columns) and
                type(columns) is type(df.columns) and
                columns.equals(df.columns)):
            # if target is identical, rename is not necessary
            return df
        # deep=False doesn't doesn't copy any data/indices, so this is cheap
        df = df.copy(deep=False)
        df.columns = columns
        return df
    elif isinstance(df, (pd.Series, pd.Index)):
        if isinstance(columns, (pd.Series, pd.Index)):
            columns = columns.name
        if df.name == columns:
            return df
        return df.rename(columns)
    # map_partition may pass other types
    return df


def _rename_dask(df, names):
    """
    Destructively rename columns of dd.DataFrame or name of dd.Series.
    Not for pd.DataFrame or pd.Series.

    Internaly used to overwrite dd.DataFrame.columns and dd.Series.name
    We can't use map_partition because it applies function then rename

    Parameters
    ----------
    df : dd.DataFrame or dd.Series
        target DataFrame / Series to be renamed
    names : tuple, string
        Column names/Series name
    """

    assert isinstance(df, _Frame)
    metadata = _rename(names, df._meta)
    name = 'rename-{0}'.format(tokenize(df, metadata))

    dsk = {}
    for i in range(df.npartitions):
        dsk[name, i] = (_rename, metadata, (df._name, i))
    return new_dd_object(merge(dsk, df.dask), name, metadata, df.divisions)


def quantile(df, q):
    """Approximate quantiles of Series.

    Parameters
    ----------
    q : list/array of floats
        Iterable of numbers ranging from 0 to 100 for the desired quantiles
    """
    assert isinstance(df, Series)
    from dask.array.percentile import _percentile, merge_percentiles

    # currently, only Series has quantile method
    if isinstance(df, Index):
        meta = pd.Series(df._meta_nonempty).quantile(q)
    else:
        meta = df._meta_nonempty.quantile(q)

    if isinstance(meta, pd.Series):
        # Index.quantile(list-like) must be pd.Series, not pd.Index
        df_name = df.name
        finalize_tsk = lambda tsk: (pd.Series, tsk, q, None, df_name)
        return_type = Series
    else:
        finalize_tsk = lambda tsk: (getitem, tsk, 0)
        return_type = Scalar
        q = [q]

    # pandas uses quantile in [0, 1]
    # numpy / everyone else uses [0, 100]
    qs = np.asarray(q) * 100
    token = tokenize(df, qs)

    if len(qs) == 0:
        name = 'quantiles-' + token
        empty_index = pd.Index([], dtype=float)
        return Series({(name, 0): pd.Series([], name=df.name, index=empty_index)},
                      name, df._meta, [None, None])
    else:
        new_divisions = [np.min(q), np.max(q)]

    name = 'quantiles-1-' + token
    val_dsk = dict(((name, i), (_percentile, (getattr, key, 'values'), qs))
                   for i, key in enumerate(df._keys()))
    name2 = 'quantiles-2-' + token
    len_dsk = dict(((name2, i), (len, key)) for i, key in enumerate(df._keys()))

    name3 = 'quantiles-3-' + token
    merge_dsk = {(name3, 0): finalize_tsk((merge_percentiles, qs,
                                           [qs] * df.npartitions,
                                           sorted(val_dsk), sorted(len_dsk)))}
    dsk = merge(df.dask, val_dsk, len_dsk, merge_dsk)
    return return_type(dsk, name3, meta, new_divisions)


def cov_corr(df, min_periods=None, corr=False, scalar=False, split_every=False):
    """DataFrame covariance and pearson correlation.

    Computes pairwise covariance or correlation of columns, excluding NA/null
    values.

    Parameters
    ----------
    df : DataFrame
    min_periods : int, optional
        Minimum number of observations required per pair of columns
        to have a valid result.
    corr : bool, optional
        If True, compute the Pearson correlation. If False [default], compute
        the covariance.
    scalar : bool, optional
        If True, compute covariance between two variables as a scalar. Only
        valid if `df` has 2 columns.  If False [default], compute the entire
        covariance/correlation matrix.
    split_every : int, optional
        Group partitions into groups of this size while performing a
        tree-reduction. If set to False, no tree-reduction will be used.
        Default is False.
    """
    if min_periods is None:
        min_periods = 2
    elif min_periods < 2:
        raise ValueError("min_periods must be >= 2")

    if split_every is False:
        split_every = df.npartitions
    elif split_every < 2 or not isinstance(split_every, int):
        raise ValueError("split_every must be an integer >= 2")

    df = df._get_numeric_data()

    if scalar and len(df.columns) != 2:
        raise ValueError("scalar only valid for 2 column dataframe")

    token = tokenize(df, min_periods, scalar, split_every)

    funcname = 'corr' if corr else 'cov'
    a = '{0}-chunk-{1}'.format(funcname, df._name)
    dsk = {(a, i): (cov_corr_chunk, f, corr)
           for (i, f) in enumerate(df._keys())}

    prefix = '{0}-combine-{1}-'.format(funcname, df._name)
    k = df.npartitions
    b = a
    depth = 0
    while k > split_every:
        b = prefix + str(depth)
        for part_i, inds in enumerate(partition_all(split_every, range(k))):
            dsk[(b, part_i)] = (cov_corr_combine, [(a, i) for i in inds], corr)
        k = part_i + 1
        a = b
        depth += 1

    name = '{0}-{1}'.format(funcname, token)
    dsk[(name, 0)] = (cov_corr_agg, [(a, i) for i in range(k)],
                      df.columns, min_periods, corr, scalar)
    dsk.update(df.dask)
    if scalar:
        return Scalar(dsk, name, 'f8')
    meta = make_meta([(c, 'f8') for c in df.columns], index=df.columns)
    return DataFrame(dsk, name, meta, (df.columns[0], df.columns[-1]))


def cov_corr_chunk(df, corr=False):
    """Chunk part of a covariance or correlation computation"""
    mat = df.values
    mask = np.isfinite(mat)
    keep = np.bitwise_and(mask[:, None, :], mask[:, :, None])

    x = np.where(keep, mat[:, None, :], np.nan)
    sums = np.nansum(x, 0)
    counts = keep.astype('int').sum(0)
    cov = df.cov().values
    dtype = [('sum', sums.dtype), ('count', counts.dtype), ('cov', cov.dtype)]
    if corr:
        m = np.nansum((x - sums / np.where(counts, counts, np.nan)) ** 2, 0)
        dtype.append(('m', m.dtype))

    out = np.empty(counts.shape, dtype=dtype)
    out['sum'] = sums
    out['count'] = counts
    out['cov'] = cov * (counts - 1)
    if corr:
        out['m'] = m
    return out


def cov_corr_combine(data, corr=False):
    data = np.concatenate(data).reshape((len(data),) + data[0].shape)
    sums = np.nan_to_num(data['sum'])
    counts = data['count']

    cum_sums = np.cumsum(sums, 0)
    cum_counts = np.cumsum(counts, 0)

    s1 = cum_sums[:-1]
    s2 = sums[1:]
    n1 = cum_counts[:-1]
    n2 = counts[1:]
    d = (s2 / n2) - (s1 / n1)

    C = (np.nansum((n1 * n2) / (n1 + n2) * (d * d.transpose((0, 2, 1))), 0) +
         np.nansum(data['cov'], 0))

    out = np.empty(C.shape, dtype=data.dtype)
    out['sum'] = cum_sums[-1]
    out['count'] = cum_counts[-1]
    out['cov'] = C

    if corr:
        nobs = np.where(cum_counts[-1], cum_counts[-1], np.nan)
        mu = cum_sums[-1] / nobs
        counts_na = np.where(counts, counts, np.nan)
        m = np.nansum(data['m'] + counts * (sums / counts_na - mu) ** 2,
                      axis=0)
        out['m'] = m
    return out


def cov_corr_agg(data, cols, min_periods=2, corr=False, scalar=False):
    out = cov_corr_combine(data, corr)
    counts = out['count']
    C = out['cov']
    C[counts < min_periods] = np.nan
    if corr:
        m2 = out['m']
        den = np.sqrt(m2 * m2.T)
    else:
        den = np.where(counts, counts, np.nan) - 1
    mat = C / den
    if scalar:
        return mat[0, 1]
    return pd.DataFrame(mat, columns=cols, index=cols)


def pd_split(df, p, random_state=None):
    """ Split DataFrame into multiple pieces pseudorandomly

    >>> df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
    ...                    'b': [2, 3, 4, 5, 6, 7]})

    >>> a, b = pd_split(df, [0.5, 0.5], random_state=123)  # roughly 50/50 split
    >>> a
       a  b
    1  2  3
    2  3  4
    5  6  7
    >>> b
       a  b
    0  1  2
    3  4  5
    4  5  6
    """
    p = list(p)
    index = pseudorandom(len(df), p, random_state)
    return [df.iloc[index == i] for i in range(len(p))]


def _take_last(a, skipna=True):
    """
    take last row (Series) of DataFrame / last value of Series
    considering NaN.

    Parameters
    ----------
    a : pd.DataFrame or pd.Series
    skipna : bool, default True
        Whether to exclude NaN

    """
    if skipna is False:
        return a.iloc[-1]
    else:
        # take last valid value excluding NaN, NaN location may be different
        # in each columns
        group_dummy = np.ones(len(a.index))
        last_row = a.groupby(group_dummy).last()
        if isinstance(a, pd.DataFrame):
            return pd.Series(last_row.values[0], index=a.columns)
        else:
            return last_row.values[0]


def repartition_divisions(a, b, name, out1, out2, force=False):
    """ dask graph to repartition dataframe by new divisions

    Parameters
    ----------

    a : tuple
        old divisions
    b : tuple, list
        new divisions
    name : str
        name of old dataframe
    out1 : str
        name of temporary splits
    out2 : str
        name of new dataframe
    force : bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.


    Examples
    --------

    >>> repartition_divisions([1, 3, 7], [1, 4, 6, 7], 'a', 'b', 'c')  # doctest: +SKIP
    {('b', 0): (<function boundary_slice at ...>, ('a', 0), 1, 3, False),
     ('b', 1): (<function boundary_slice at ...>, ('a', 1), 3, 4, False),
     ('b', 2): (<function boundary_slice at ...>, ('a', 1), 4, 6, False),
     ('b', 3): (<function boundary_slice at ...>, ('a', 1), 6, 7, False)
     ('c', 0): (<function concat at ...>,
                (<type 'list'>, [('b', 0), ('b', 1)])),
     ('c', 1): ('b', 2),
     ('c', 2): ('b', 3)}
    """

    if not isinstance(b, (list, tuple)):
        raise ValueError('New division must be list or tuple')
    b = list(b)

    if len(b) < 2:
        # minimum division is 2 elements, like [0, 0]
        raise ValueError('New division must be longer than 2 elements')

    if b != sorted(b):
        raise ValueError('New division must be sorted')
    if len(b[:-1]) != len(list(unique(b[:-1]))):
        msg = 'New division must be unique, except for the last element'
        raise ValueError(msg)

    if force:
        if a[0] < b[0]:
            msg = ('left side of the new division must be equal or smaller '
                   'than old division')
            raise ValueError(msg)
        if a[-1] > b[-1]:
            msg = ('right side of the new division must be equal or larger '
                   'than old division')
            raise ValueError(msg)
    else:
        if a[0] != b[0]:
            msg = 'left side of old and new divisions are different'
            raise ValueError(msg)
        if a[-1] != b[-1]:
            msg = 'right side of old and new divisions are different'
            raise ValueError(msg)

    def _is_single_last_div(x):
        """Whether last division only contains single label"""
        return len(x) >= 2 and x[-1] == x[-2]

    c = [a[0]]
    d = dict()
    low = a[0]

    i, j = 1, 1     # indices for old/new divisions
    k = 0           # index for temp divisions

    last_elem = _is_single_last_div(a)

    # process through old division
    # left part of new division can be processed in this loop
    while (i < len(a) and j < len(b)):
        if a[i] < b[j]:
            # tuple is something like:
            # (methods.boundary_slice, ('from_pandas-#', 0), 3, 4, False))
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, a[i], False)
            low = a[i]
            i += 1
        elif a[i] > b[j]:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            j += 1
        else:
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), low, b[j], False)
            low = b[j]
            i += 1
            j += 1
        c.append(low)
        k += 1

    # right part of new division can remain
    if a[-1] < b[-1] or b[-1] == b[-2]:
        for _j in range(j, len(b)):
            # always use right-most of old division
            # because it may contain last element
            m = len(a) - 2
            d[(out1, k)] = (methods.boundary_slice, (name, m), low, b[_j], False)
            low = b[_j]
            c.append(low)
            k += 1
    else:
        # even if new division is processed through,
        # right-most element of old division can remain
        if last_elem and i < len(a):
            d[(out1, k)] = (methods.boundary_slice, (name, i - 1), a[i], a[i], False)
            k += 1
        c.append(a[-1])

    # replace last element of tuple with True
    d[(out1, k - 1)] = d[(out1, k - 1)][:-1] + (True,)

    i, j = 0, 1

    last_elem = _is_single_last_div(c)

    while j < len(b):
        tmp = []
        while c[i] < b[j]:
            tmp.append((out1, i))
            i += 1
        if last_elem and c[i] == b[-1] and (b[-1] != b[-2] or j == len(b) - 1) and i < k:
            # append if last split is not included
            tmp.append((out1, i))
            i += 1
        if len(tmp) == 0:
            # dummy slice to return empty DataFrame or Series,
            # which retain original data attributes (columns / name)
            d[(out2, j - 1)] = (methods.boundary_slice, (name, 0), a[0], a[0], False)
        elif len(tmp) == 1:
            d[(out2, j - 1)] = tmp[0]
        else:
            if not tmp:
                raise ValueError('check for duplicate partitions\nold:\n%s\n\n'
                                 'new:\n%s\n\ncombined:\n%s'
                                 % (pformat(a), pformat(b), pformat(c)))
            d[(out2, j - 1)] = (pd.concat, tmp)
        j += 1
    return d


def repartition_npartitions(df, npartitions):
    """ Repartition dataframe to a smaller number of partitions """
    npartitions_ratio = df.npartitions / npartitions
    new_partitions_boundaries = [int(new_partition_index * npartitions_ratio)
                                 for new_partition_index in range(npartitions + 1)]
    new_name = 'repartition-%d-%s' % (npartitions, tokenize(df))
    dsk = {}
    for new_partition_index in range(npartitions):
        value = (pd.concat, [(df._name, old_partition_index)
                             for old_partition_index in
                             range(new_partitions_boundaries[new_partition_index],
                                   new_partitions_boundaries[new_partition_index + 1])])
        dsk[new_name, new_partition_index] = value
    divisions = [df.divisions[new_partition_index]
                 for new_partition_index in new_partitions_boundaries]
    return DataFrame(merge(df.dask, dsk), new_name, df._meta, divisions)


def repartition(df, divisions=None, force=False):
    """ Repartition dataframe along new divisions

    Dask.DataFrame objects are partitioned along their index.  Often when
    multiple dataframes interact we need to align these partitionings.  The
    ``repartition`` function constructs a new DataFrame object holding the same
    data but partitioned on different values.  It does this by performing a
    sequence of ``loc`` and ``concat`` calls to split and merge the previous
    generation of partitions.

    Parameters
    ----------

    divisions : list
        List of partitions to be used
    force : bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.

    Examples
    --------

    >>> df = df.repartition([0, 5, 10, 20])  # doctest: +SKIP

    Also works on Pandas objects

    >>> ddf = dd.repartition(df, [0, 5, 10, 20])  # doctest: +SKIP
    """
    token = tokenize(df, divisions)
    if isinstance(df, _Frame):
        tmp = 'repartition-split-' + token
        out = 'repartition-merge-' + token
        dsk = repartition_divisions(df.divisions, divisions,
                                    df._name, tmp, out, force=force)
        return new_dd_object(merge(df.dask, dsk), out,
                             df._meta, divisions)
    elif isinstance(df, (pd.Series, pd.DataFrame)):
        name = 'repartition-dataframe-' + token
        from .utils import shard_df_on_index
        dfs = shard_df_on_index(df, divisions[1:-1])
        dsk = dict(((name, i), df) for i, df in enumerate(dfs))
        return new_dd_object(dsk, name, df, divisions)
    raise ValueError('Data must be DataFrame or Series')


def set_sorted_index(df, index, drop=True, divisions=None, **kwargs):
    if not isinstance(index, Series):
        meta = df._meta.set_index(index, drop=drop)
    else:
        meta = df._meta.set_index(index._meta, drop=drop)

    result = map_partitions(M.set_index, df, index, drop=drop, meta=meta)

    if not divisions:
        divisions = compute_divisions(result, **kwargs)

    result.divisions = divisions
    return result


def compute_divisions(df, **kwargs):
    mins = df.index.map_partitions(M.min, meta=df.index)
    maxes = df.index.map_partitions(M.max, meta=df.index)
    mins, maxes = compute(mins, maxes, **kwargs)

    if (sorted(mins) != list(mins) or
            sorted(maxes) != list(maxes) or
            any(a > b for a, b in zip(mins, maxes))):
        raise ValueError("Partitions must be sorted ascending with the index",
                         mins, maxes)

    divisions = tuple(mins) + (list(maxes)[-1],)
    return divisions


def _reduction_chunk(x, aca_chunk=None, **kwargs):
    o = aca_chunk(x, **kwargs)
    # Return a dataframe so that the concatenated version is also a dataframe
    return o.to_frame().T if isinstance(o, pd.Series) else o


def _reduction_combine(x, aca_combine=None, **kwargs):
    if isinstance(x, list):
        x = pd.Series(x)
    o = aca_combine(x, **kwargs)
    # Return a dataframe so that the concatenated version is also a dataframe
    return o.to_frame().T if isinstance(o, pd.Series) else o


def _reduction_aggregate(x, aca_aggregate=None, **kwargs):
    if isinstance(x, list):
        x = pd.Series(x)
    return aca_aggregate(x, **kwargs)


def idxmaxmin_chunk(x, fn=None, skipna=True):
    idx = getattr(x, fn)(skipna=skipna)
    minmax = 'max' if fn == 'idxmax' else 'min'
    value = getattr(x, minmax)(skipna=skipna)
    if isinstance(x, pd.DataFrame):
        return pd.DataFrame({'idx': idx, 'value': value})
    return pd.DataFrame({'idx': [idx], 'value': [value]})


def idxmaxmin_row(x, fn=None, skipna=True):
    x = x.set_index('idx')
    idx = getattr(x.value, fn)(skipna=skipna)
    minmax = 'max' if fn == 'idxmax' else 'min'
    value = getattr(x.value, minmax)(skipna=skipna)
    return pd.DataFrame({'idx': [idx], 'value': [value]})


def idxmaxmin_combine(x, fn=None, skipna=True):
    return (x.groupby(level=0)
             .apply(idxmaxmin_row, fn=fn, skipna=skipna)
             .reset_index(level=1, drop=True))


def idxmaxmin_agg(x, fn=None, skipna=True, scalar=False):
    res = idxmaxmin_combine(x, fn, skipna=skipna)['idx']
    if scalar:
        return res[0]
    res.name = None
    return res


def safe_head(df, n):
    r = df.head(n=n)
    if len(r) != n:
        msg = ("Insufficient elements for `head`. {0} elements "
               "requested, only {1} elements available. Try passing larger "
               "`npartitions` to `head`.")
        warnings.warn(msg.format(n, len(r)))
    return r


def maybe_shift_divisions(df, periods, freq):
    """Maybe shift divisions by periods of size freq

    Used to shift the divisions for the `shift` method. If freq isn't a fixed
    size (not anchored or relative), then the divisions are shifted
    appropriately. Otherwise the divisions are cleared.

    Parameters
    ----------
    df : dd.DataFrame, dd.Series, or dd.Index
    periods : int
        The number of periods to shift.
    freq : DateOffset, timedelta, or time rule string
        The frequency to shift by.
    """
    if isinstance(freq, str):
        freq = pd.tseries.frequencies.to_offset(freq)
    if (isinstance(freq, pd.DateOffset) and
            (freq.isAnchored() or not hasattr(freq, 'delta'))):
        # Can't infer divisions on relative or anchored offsets, as
        # divisions may now split identical index value.
        # (e.g. index_partitions = [[1, 2, 3], [3, 4, 5]])
        return df.clear_divisions()
    if df.known_divisions:
        divs = pd.Series(range(len(df.divisions)), index=df.divisions)
        divisions = divs.shift(periods, freq=freq).index
        return type(df)(df.dask, df._name, df._meta, divisions)
    return df


def to_delayed(df):
    """ Create Dask Delayed objects from a Dask Dataframe

    Returns a list of delayed values, one value per partition.

    Examples
    --------
    >>> partitions = df.to_delayed()  # doctest: +SKIP
    """
    from ..delayed import Delayed
    return [Delayed(k, [df.dask]) for k in df._keys()]


if PY3:
    _Frame.to_delayed.__doc__ = to_delayed.__doc__
