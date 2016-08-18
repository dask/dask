from __future__ import absolute_import, division, print_function

from collections import Iterator
from copy import copy
import operator
from operator import getitem, setitem
from pprint import pformat
import uuid
import warnings

from toolz import merge, partial, first, partition, unique
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
from ..compatibility import apply, operator_div, bind_method
from ..utils import (repr_long_list, IndexCallable,
                     pseudorandom, derived_from, different_seeds, funcname, memory_repr, put_lines)
from ..base import Base, compute, tokenize, normalize_token
from ..async import get_sync
from .indexing import (_partition_of_index_value, _loc, _try_loc,
                       _coerce_loc_index, _maybe_partial_time_string)
from .utils import meta_nonempty, make_meta, insert_meta_param_description

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
        self._meta = make_meta(meta)

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
            try:
                meta = op(self._meta_nonempty)
            except:
                raise ValueError("Metadata inference failed in operator "
                                 "{0}.".format(funcname(op)))
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

    try:
        other_meta = make_meta(other)
        other_meta_nonempty = meta_nonempty(other_meta)
        if inv:
            meta = op(other_meta_nonempty, self._meta_nonempty)
        else:
            meta = op(self._meta_nonempty, other_meta_nonempty)
    except:
        raise ValueError("Metadata inference failed in operator "
                         "{0}.".format(funcname(op)))

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
    _name: str
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

    # constructor properties
    # http://pandas.pydata.org/pandas-docs/stable/internals.html#override-constructor-properties

    @property
    def _constructor_sliced(self):
        """Constructor used when a result has one lower dimension(s) as the original"""
        raise NotImplementedError

    @property
    def _constructor(self):
        """Constructor used when a result has the same dimension(s) as the original"""
        raise NotImplementedError

    @property
    def npartitions(self):
        """Return number of partitions"""
        return len(self.divisions) - 1

    @property
    def _pd(self):
        warnings.warn('Deprecation warning: use `_meta` instead')
        return self._meta

    @property
    def _pd_nonempty(self):
        warnings.warn('Deprecation warning: use `_meta_nonempty` instead')
        return self._meta_nonempty

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

    @property
    def index(self):
        """Return dask Index instance"""
        name = self._name + '-index'
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))

        return Index(merge(dsk, self.dask), name,
                     self._meta.index, self.divisions)

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    def clear_divisions(self):
        divisions = (None,) * (self.npartitions + 1)
        return type(self)(self.dask, self._name, self._meta, divisions)

    def get_division(self, n):
        warnings.warn("Deprecation warning: use `get_partition` instead")
        return self.get_partition(self, n)

    def get_partition(self, n):
        """Get a dask DataFrame/Series representing the `nth` partition."""
        if 0 <= n < self.npartitions:
            name = 'get-partition-%s-%s' % (str(n), self._name)
            dsk = {(name, 0): (self._name, n)}
            divisions = self.divisions[n:n+2]
            return self._constructor(merge(self.dask, dsk), name,
                                     self._meta, divisions)
        else:
            msg = "n must be 0 <= n < {0}".format(self.npartitions)
            raise ValueError(msg)

    def cache(self, cache=Cache):
        """ Evaluate Dataframe and store in local cache

        Uses chest by default to store data on disk
        """
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
        return self._constructor(dsk2, name, self._meta, self.divisions)

    @derived_from(pd.DataFrame)
    def drop_duplicates(self, **kwargs):
        assert all(k in ('keep', 'subset', 'take_last') for k in kwargs)
        chunk = lambda s: s.drop_duplicates(**kwargs)
        return aca(self, chunk=chunk, aggregate=chunk, meta=self._meta,
                   token='drop-duplicates')

    def __len__(self):
        return reduction(self, len, np.sum, token='len', meta=int).compute()

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

        Here we map a funtion that takes in a DataFrame, and returns a
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

    def random_split(self, p, random_state=None):
        """ Pseudorandomly split dataframe into different pieces row-wise

        Parameters
        ----------
        frac : float, optional
            Fraction of axis items to return.
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
        seeds = different_seeds(self.npartitions, random_state)
        dsk_full = dict(((self._name + '-split-full', i),
                         (pd_split, (self._name, i), p, seed))
                       for i, seed in enumerate(seeds))

        dsks = [dict(((self._name + '-split-%d' % i, j),
                      (getitem, (self._name + '-split-full', j), i))
                      for j in range(self.npartitions))
                      for i in range(len(p))]
        return [type(self)(merge(self.dask, dsk_full, dsk),
                           self._name + '-split-%d' % i,
                           self._meta, self.divisions)
                for i, dsk in enumerate(dsks)]

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
            raise ValueError("only {} partitions, head "
                "received {}".format(self.npartitions, npartitions))

        name = 'head-%d-%d-%s' % (npartitions, n, self._name)

        if npartitions > 1:
            name_p = 'head-partial-%d-%s' % (n, self._name)

            dsk = {}
            for i in range(npartitions):
                dsk[(name_p, i)] = (self._partition_type.head, (self._name, i), n)

            concat = (_concat, ([(name_p, i) for i in range(npartitions)]))
            dsk[(name, 0)] = (safe_head, self._partition_type.head, concat, n)
        else:
            dsk = {(name, 0): (safe_head, self._partition_type.head, (self._name, 0), n)}

        result = self._constructor(merge(self.dask, dsk), name, self._meta,
                                   [self.divisions[0], self.divisions[npartitions]])

        if compute:
            result = result.compute()
        return result

    def tail(self, n=5, compute=True):
        """ Last n rows of the dataset

        Caveat, the only checks the last n rows of the last partition.
        """
        name = 'tail-%d-%s' % (n, self._name)
        dsk = {(name, 0): (lambda x, n: x.tail(n=n),
                (self._name, self.npartitions - 1), n)}

        result = self._constructor(merge(self.dask, dsk), name,
                                   self._meta, self.divisions[-2:])

        if compute:
            result = result.compute()
        return result

    def _loc(self, ind):
        """ Helper function for the .loc accessor """
        if isinstance(ind, Series):
            return self._loc_series(ind)
        if self.known_divisions:
            ind = _maybe_partial_time_string(self._meta.index, ind, kind='loc')
            if isinstance(ind, slice):
                return self._loc_slice(ind)
            else:
                return self._loc_element(ind)
        else:
            return map_partitions(_try_loc, self, ind, meta=self)

    def _loc_series(self, ind):
        if not self.divisions == ind.divisions:
            raise ValueError("Partitions of dataframe and index not the same")
        return map_partitions(lambda df, ind: df.loc[ind],
                              self, ind, token='loc-series', meta=self)

    def _loc_element(self, ind):
        name = 'loc-%s' % tokenize(ind, self)
        part = _partition_of_index_value(self.divisions, ind)
        if ind < self.divisions[0] or ind > self.divisions[-1]:
            raise KeyError('the label [%s] is not in the index' % str(ind))
        dsk = {(name, 0): (lambda df: df.loc[ind:ind], (self._name, part))}

        return self._constructor(merge(self.dask, dsk), name, self, [ind, ind])

    def _loc_slice(self, ind):
        name = 'loc-%s' % tokenize(ind, self)
        assert ind.step in (None, 1)

        if ind.start:
            start = _partition_of_index_value(self.divisions, ind.start)
        else:
            start = 0
        if ind.stop is not None:
            stop = _partition_of_index_value(self.divisions, ind.stop)
        else:
            stop = self.npartitions - 1
        istart = _coerce_loc_index(self.divisions, ind.start)
        istop = _coerce_loc_index(self.divisions, ind.stop)
        if stop == start:
            dsk = {(name, 0): (_loc, (self._name, start), ind.start, ind.stop)}
            divisions = [istart, istop]
        else:
            dsk = merge(
              {(name, 0): (_loc, (self._name, start), ind.start, None)},
              dict(((name, i), (self._name, start + i))
                  for i in range(1, stop - start)),
              {(name, stop - start): (_loc, (self._name, stop), None, ind.stop)})

            divisions = ((max(istart, self.divisions[start])
                          if ind.start is not None
                          else self.divisions[0],) +
                         self.divisions[start+1:stop+1] +
                         (min(istop, self.divisions[stop+1])
                          if ind.stop is not None
                          else self.divisions[-1],))

        assert len(divisions) == len(dsk) + 1
        return self._constructor(merge(self.dask, dsk), name,
                                 self._meta, divisions)

    @property
    def loc(self):
        """ Purely label-location based indexer for selection by label.

        >>> df.loc["b"]  # doctest: +SKIP
        >>> df.loc["b":"d"]  # doctest: +SKIP"""
        return IndexCallable(self._loc)

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

    @derived_from(pd.Series)
    def fillna(self, value):
        return self.map_partitions(self._partition_type.fillna, value=value)

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
            random_state = np.random.randint(np.iinfo(np.int32).max)

        name = 'sample-' + tokenize(self, frac, replace, random_state)
        func = getattr(self._partition_type, 'sample')

        seeds = different_seeds(self.npartitions, random_state)

        dsk = dict(((name, i),
                   (apply, func, (tuple, [(self._name, i)]),
                       {'frac': frac, 'random_state': seed,
                        'replace': replace}))
                   for i, seed in zip(range(self.npartitions), seeds))

        return self._constructor(merge(self.dask, dsk), name,
                                 self._meta, self.divisions)

    def to_hdf(self, path_or_buf, key, mode='a', append=False, get=None, **kwargs):
        """ Export frame to hdf file(s)

        Export dataframe to one or multiple hdf5 files or nodes.

        Exported hdf format is pandas' hdf table format only.
        Data saved by this function should be read by pandas dataframe
        compatible reader.

        By providing a single asterisk in either the path_or_buf or key
        parameters you direct dask to save each partition to a different file
        or node (respectively). The asterisk will be replaced with a zero
        padded partition number, as this is the default implementation of
        name_function.

        When writing to a single hdf node in a single hdf file, all hdf save
        tasks are required to execute in a specific order, often becoming the
        bottleneck of the entire execution graph. Saving to multiple nodes or
        files removes that restriction (order is still preserved by enforcing
        order on output, using name_function) and enables executing save tasks
        in parallel.

        Parameters
        ----------
        path_or_buf: HDFStore object or string
        Destination file(s). If string, can contain a single asterisk to
        save each partition to a different file. Only one asterisk is
        allowed in both path_or_buf and key parameters.
        key: string
        A node / group path in file, can contain a single asterisk to save
        each partition to a different hdf node in a single file. Only one
        asterisk is allowed in both path_or_buf and key parameters.
        format: optional, default 'table'
            Default hdf storage format, currently only pandas' 'table' format
            is supported.
        mode: optional, {'a', 'w', 'r+'}, default 'a'

          ``'a'``
              Append: Add data to existing file(s) or create new.
          ``'w'``
              Write: overwrite any existing files with new ones.
          ``'r+'``
              Append to existing files, files must already exist.
        append: optional, default False
        If False, overwrites existing node with the same name otherwise
        appends to it.
        complevel: optional, 0-9, default 0
            compression level, higher means better compression ratio and
            possibly more CPU time. Depends on complib.
        complib: {'zlib', 'bzip2', 'lzo', 'blosc', None}, default None
        If complevel > 0 compress using this compression library when
        possible
        fletcher32: bool, default False
        If True and compression is used, additionally apply the fletcher32
        checksum.
        get: callable, optional
        A scheduler `get` function to use. If not provided, the default is
        to check the global settings first, and then fall back to defaults
        for the collections.
        dask_kwargs: dict, optional
        A dictionary of keyword arguments passed to the `get` function
        used.
        name_function: callable, optional, default None
        A callable called for each partition that accepts a single int
        representing the partition number. name_function must return a
        string representation of a partition's index in a way that will
        preserve the partition's location after a string sort.

        If None, a default name_function is used. The default name_function
        will return a zero padded string of received int. See
        dask.utils.build_name_function for more info.
        compute: bool, default True
            If True, execute computation of resulting dask graph.
            If False, return a Delayed object.
        lock: bool, None or lock object, default None
        In to_hdf locks are needed for two reasons. First, to protect
        against writing to the same file from multiple processes or threads
        simultaneously.  Second, default libhdf5 is not thread safe, so we
        must additionally lock on it's usage. By default if lock is None
        lock will be determined optimally based on path_or_buf, key and the
        scheduler used. Manually setting this parameter is usually not
        required to improve performance.

        Alternatively, you can specify specific values:
        If False, no locking will occur. If True, default lock object will
        be created (multiprocessing.Manager.Lock on multiprocessing
        scheduler, Threading.Lock otherwise), This can be used to force
        using a lock in scenarios the default behavior will be to avoid
        locking. Else, value is assumed to implement the lock interface,
        and will be the lock object used.

        See Also
        --------
            dask.DataFrame.read_hdf: reading hdf files
            dask.Series.read_hdf: reading hdf files

        Examples
        --------
        Saving data to a single file:

        >>> df.to_hdf('output.hdf', '/data')            # doctest: +SKIP

        Saving data to multiple nodes:

        >>> with pd.HDFStore('output.hdf') as fh:
        ...     df.to_hdf(fh, '/data*')
        ...     fh.keys()                               # doctest: +SKIP
        ['/data0', '/data1']

        Or multiple files:

        >>> df.to_hdf('output_*.hdf', '/data')          # doctest: +SKIP

        Saving multiple files with the multiprocessing scheduler and manually
        disabling locks:

        >>> df.to_hdf('output_*.hdf', '/data',
        ...   get=dask.multiprocessing.get, lock=False) # doctest: +SKIP
        """

        from .io import to_hdf
        return to_hdf(self, path_or_buf, key, mode, append, get=get, **kwargs)

    @derived_from(pd.DataFrame)
    def to_csv(self, filename, **kwargs):
        from .io import to_csv
        return to_csv(self, filename, **kwargs)

    def to_imperative(self):
        warnings.warn("Deprecation warning: moved to to_delayed")
        return self.to_delayed()

    def to_delayed(self):
        """ Convert dataframe into dask Values

        Returns a list of values, one value per partition.
        """
        from ..delayed import Delayed
        return [Delayed(k, [self.dask]) for k in self._keys()]

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self)
        else:
            return lambda self, other: elemwise(op, self, other)

    def _aca_agg(self, token, func, aggfunc=None, **kwargs):
        """ Wrapper for aggregations """
        raise NotImplementedError

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
    def sum(self, axis=None, skipna=True):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.sum(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_sum, self, meta=meta,
                                  token=self._token_prefix + 'sum',
                                  axis=axis, skipna=skipna)
        else:
            return self._aca_agg(token='sum', func=_sum,
                                 skipna=skipna, axis=axis,
                                 meta=meta)

    @derived_from(pd.DataFrame)
    def max(self, axis=None, skipna=True):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.max(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_max, self, meta=meta,
                                  token=self._token_prefix + 'max',
                                  skipna=skipna, axis=axis)
        else:
            return self._aca_agg(token='max', func=_max,
                                 skipna=skipna, axis=axis,
                                 meta=meta)

    @derived_from(pd.DataFrame)
    def min(self, axis=None, skipna=True):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.min(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_min, self, meta=meta,
                                  token=self._token_prefix + 'min',
                                  skipna=skipna, axis=axis)
        else:
            return self._aca_agg(token='min', func=_min,
                                 skipna=skipna, axis=axis,
                                 meta=meta)

    @derived_from(pd.DataFrame)
    def idxmax(self, axis=None, skipna=True):
        fn = 'idxmax'
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.idxmax(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_idxmax, self, meta=meta,
                                  token=self._token_prefix + fn,
                                  skipna=skipna, axis=axis)
        else:
            return aca([self], chunk=idxmaxmin_chunk, aggregate=idxmaxmin_agg,
                        meta=meta, token=self._token_prefix + fn, skipna=skipna,
                        known_divisions=self.known_divisions, fn=fn, axis=axis)

    @derived_from(pd.DataFrame)
    def idxmin(self, axis=None, skipna=True):
        fn = 'idxmin'
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.idxmax(axis=axis)
        if axis == 1:
            return map_partitions(_idxmin, self, meta=meta,
                                  token=self._token_prefix + fn,
                                  skipna=skipna, axis=axis)
        else:
            return aca([self], chunk=idxmaxmin_chunk, aggregate=idxmaxmin_agg,
                        meta=meta, token=self._token_prefix + fn, skipna=skipna,
                        known_divisions=self.known_divisions, fn=fn, axis=axis)

    @derived_from(pd.DataFrame)
    def count(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            meta = self._meta_nonempty.count(axis=axis)
            return map_partitions(_count, self, meta=meta,
                                  token=self._token_prefix + 'count',
                                  axis=axis)
        else:
            meta = self._meta_nonempty.count()
            return self._aca_agg(token='count', func=_count,
                                 aggfunc=lambda x: x.sum(),
                                 meta=meta)

    @derived_from(pd.DataFrame)
    def mean(self, axis=None, skipna=True):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.mean(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_mean, self, meta=meta,
                                  token=self._token_prefix + 'mean',
                                  axis=axis, skipna=skipna)
        else:
            num = self._get_numeric_data()
            s = num.sum(skipna=skipna)
            n = num.count()

            def f(s, n):
                try:
                    return s / n
                except ZeroDivisionError:
                    return np.nan
            name = self._token_prefix + 'mean-%s' % tokenize(self, axis, skipna)
            return map_partitions(f, s, n, token=name, meta=meta)

    @derived_from(pd.DataFrame)
    def var(self, axis=None, skipna=True, ddof=1):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.var(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_var, self, meta=meta,
                                  token=self._token_prefix + 'var',
                                  axis=axis, skipna=skipna, ddof=ddof)
        else:
            num = self._get_numeric_data()
            x = 1.0 * num.sum(skipna=skipna)
            x2 = 1.0 * (num ** 2).sum(skipna=skipna)
            n = num.count()

            def f(x2, x, n):
                try:
                    result = (x2 / n) - (x / n)**2
                    if ddof:
                        result = result * n / (n - ddof)
                    return result
                except ZeroDivisionError:
                    return np.nan
            name = self._token_prefix + 'var-%s' % tokenize(self, axis, skipna, ddof)
            return map_partitions(f, x2, x, n, token=name, meta=meta)

    @derived_from(pd.DataFrame)
    def std(self, axis=None, skipna=True, ddof=1):
        axis = self._validate_axis(axis)
        meta = self._meta_nonempty.std(axis=axis, skipna=skipna)
        if axis == 1:
            return map_partitions(_std, self, meta=meta,
                                  token=self._token_prefix + 'std',
                                  axis=axis, skipna=skipna, ddof=ddof)
        else:
            v = self.var(skipna=skipna, ddof=ddof)
            name = self._token_prefix + 'std-finish--%s' % tokenize(self, axis,
                                                            skipna, ddof)
            return map_partitions(np.sqrt, v, meta=meta, token=name)

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
        name = 'quantiles-concat--' + tokenize(self, q, axis)

        if axis == 1:
            if isinstance(q, list):
                # Not supported, the result will have current index as columns
                raise ValueError("'q' must be scalar when axis=1 is specified")
            meta = pd.Series([], dtype='f8')
            return map_partitions(pd.DataFrame.quantile, self, q, axis,
                                  token=name, meta=meta)
        else:
            meta = self._meta.quantile(q, axis=axis)
            num = self._get_numeric_data()
            quantiles = tuple(quantile(self[c], q) for c in num.columns)

            dask = {}
            dask = merge(dask, *[q.dask for q in quantiles])
            qnames = [(q._name, 0) for q in quantiles]

            if isinstance(quantiles[0], Scalar):
                dask[(name, 0)] = (pd.Series, (list, qnames), num.columns)
                divisions = (min(num.columns), max(num.columns))
                return Series(dask, name, meta, divisions)
            else:
                from .multi import _pdconcat
                dask[(name, 0)] = (_pdconcat, (list, qnames), 1)
                return DataFrame(dask, name, meta, quantiles[0].divisions)

    @derived_from(pd.DataFrame)
    def describe(self):
        name = 'describe--' + tokenize(self)

        # currently, only numeric describe is supported
        num = self._get_numeric_data()

        stats = [num.count(), num.mean(), num.std(), num.min(),
                 num.quantile([0.25, 0.5, 0.75]), num.max()]
        stats_names = [(s._name, 0) for s in stats]

        def build_partition(values):
            assert len(values) == 6
            count, mean, std, min, q, max = values
            part1 = self._partition_type([count, mean, std, min],
                                         index=['count', 'mean', 'std', 'min'])
            q.index = ['25%', '50%', '75%']
            part3 = self._partition_type([max], index=['max'])
            return pd.concat([part1, q, part3])

        dsk = dict()
        dsk[(name, 0)] = (build_partition, (list, stats_names))
        dsk = merge(dsk, num.dask, *[s.dask for s in stats])

        return self._constructor(dsk, name, num._meta,
                                 divisions=[None, None])

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
            return self._constructor(merge(dask, cumpart.dask, cumlast.dask),
                                     name, chunk(self._meta), self.divisions)

    @derived_from(pd.DataFrame)
    def cumsum(self, axis=None, skipna=True):
        cumsum = lambda x, **kwargs: x.cumsum(**kwargs)
        return self._cum_agg('cumsum',
                             chunk=cumsum,
                             aggregate=operator.add,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cumprod(self, axis=None, skipna=True):
        cumprod = lambda x, **kwargs: x.cumprod(**kwargs)
        return self._cum_agg('cumprod',
                             chunk=cumprod,
                             aggregate=operator.mul,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cummax(self, axis=None, skipna=True):
        def aggregate(x, y):
            if isinstance(x, (pd.Series, pd.DataFrame)):
                return x.where((x > y) | x.isnull(), y, axis=x.ndim - 1)
            else:       # scalar
                return x if x > y else y
        cummax = lambda x, **kwargs: x.cummax(**kwargs)
        return self._cum_agg('cummax',
                             chunk=cummax,
                             aggregate=aggregate,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def cummin(self, axis=None, skipna=True):
        def aggregate(x, y):
            if isinstance(x, (pd.Series, pd.DataFrame)):
                return x.where((x < y) | x.isnull(), y, axis=x.ndim - 1)
            else:       # scalar
                return x if x < y else y
        cummin = lambda x, **kwargs: x.cummin(**kwargs)
        return self._cum_agg('cummin',
                             chunk=cummin,
                             aggregate=aggregate,
                             axis=axis, skipna=skipna,
                             chunk_kwargs=dict(axis=axis, skipna=skipna))

    @derived_from(pd.DataFrame)
    def where(self, cond, other=np.nan):
        # cond and other may be dask instance,
        # passing map_partitions via keyword will not be aligned
        return map_partitions(self._partition_type.where, self, cond, other)

    @derived_from(pd.DataFrame)
    def mask(self, cond, other=np.nan):
        return map_partitions(self._partition_type.mask, self, cond, other)

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

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """
        raise NotImplementedError


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

    def __init__(self, dsk, _name, meta, divisions):
        super(Series, self).__init__()
        self.dask = dsk
        self._name = _name
        self._meta = make_meta(meta)
        self.divisions = tuple(divisions)

    @property
    def _constructor_sliced(self):
        return Scalar

    @property
    def _constructor(self):
        return Series

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

    @property
    def cat(self):
        return self._meta.cat

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        if not hasattr(self._meta, 'cat'):
            o.remove('cat')  # cat only in `dir` if available
        return list(o)

    @property
    def column_info(self):
        """ Return Series.name """
        warnings.warn('column_info is deprecated, use name')
        return self.name

    @property
    def nbytes(self):
        return reduction(self, lambda s: s.nbytes, np.sum, token='nbytes',
                         meta=int)

    def __array__(self, dtype=None, **kwargs):
        x = np.array(self.compute())
        if dtype and x.dtype != dtype:
            x = x.astype(dtype)
        return x

    def __array_wrap__(self, array, context=None):
        return pd.Series(array, name=self.name)

    @cache_readonly
    def dt(self):
        return DatetimeAccessor(self)

    @cache_readonly
    def str(self):
        return StringAccessor(self)

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

    @derived_from(pd.Series)
    def resample(self, rule, how=None, closed=None, label=None):
        from .tseries.resample import _resample
        return _resample(self, rule, how=how, closed=closed, label=label)

    def __getitem__(self, key):
        if isinstance(key, Series) and self.divisions == key.divisions:
            name = 'index-%s' % tokenize(self, key)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return Series(merge(self.dask, key.dask, dsk), name,
                          self.name, self.divisions)
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

    def _aca_agg(self, token, func, aggfunc=None, meta=no_default, **kwargs):
        """ Wrapper for aggregations """
        if aggfunc is None:
            aggfunc = func

        return aca([self], chunk=func,
                   aggregate=lambda x, **kwargs: aggfunc(pd.Series(x), **kwargs),
                   meta=meta, token=self._token_prefix + token,
                   **kwargs)

    @derived_from(pd.Series)
    def groupby(self, index, **kwargs):
        from dask.dataframe.groupby import SeriesGroupBy
        return SeriesGroupBy(self, index, **kwargs)

    @derived_from(pd.Series)
    def sum(self, axis=None, skipna=True):
        return super(Series, self).sum(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def max(self, axis=None, skipna=True):
        return super(Series, self).max(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def min(self, axis=None, skipna=True):
        return super(Series, self).min(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def count(self):
        return super(Series, self).count()

    @derived_from(pd.Series)
    def mean(self, axis=None, skipna=True):
        return super(Series, self).mean(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def var(self, axis=None, ddof=1, skipna=True):
        return super(Series, self).var(axis=axis, ddof=ddof, skipna=skipna)

    @derived_from(pd.Series)
    def std(self, axis=None, ddof=1, skipna=True):
        return super(Series, self).std(axis=axis, ddof=ddof, skipna=skipna)

    @derived_from(pd.Series)
    def cumsum(self, axis=None, skipna=True):
        return super(Series, self).cumsum(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def cumprod(self, axis=None, skipna=True):
        return super(Series, self).cumprod(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def cummax(self, axis=None, skipna=True):
        return super(Series, self).cummax(axis=axis, skipna=skipna)

    @derived_from(pd.Series)
    def cummin(self, axis=None, skipna=True):
        return super(Series, self).cummin(axis=axis, skipna=skipna)

    def unique(self):
        """
        Return Series of unique values in the object. Includes NA values.

        Returns
        -------
        uniques : Series
        """
        # unique returns np.ndarray, it must be wrapped
        name = self.name
        chunk = lambda x: pd.Series(pd.Series.unique(x), name=name)
        return aca(self, chunk=chunk, aggregate=chunk,
                   meta=self._meta, token='unique')

    @derived_from(pd.Series)
    def nunique(self):
        return self.drop_duplicates().count()

    @derived_from(pd.Series)
    def value_counts(self):
        chunk = lambda s: s.value_counts()
        agg = lambda s: s.groupby(level=0).sum().sort_values(ascending=False)
        meta = self._meta.value_counts()
        return aca(self, chunk=chunk, aggregate=agg, meta=meta,
                   token='value-counts')

    @derived_from(pd.Series)
    def nlargest(self, n=5):
        token = 'series-nlargest-n={0}'.format(n)
        f = lambda s: s.nlargest(n)
        return aca(self, f, f, meta=self._meta, token=token)

    @derived_from(pd.Series)
    def isin(self, other):
        return elemwise(pd.Series.isin, self, list(other))

    @derived_from(pd.Series)
    def map(self, arg, na_action=None, meta=no_default):
        if not (isinstance(arg, (pd.Series, dict)) or callable(arg)):
            raise TypeError("arg must be pandas.Series, dict or callable."
                            " Got {0}".format(type(arg)))
        name = 'map-' + tokenize(self, arg, na_action)
        dsk = dict(((name, i), (pd.Series.map, k, arg, na_action)) for i, k in
                   enumerate(self._keys()))
        dsk.update(self.dask)
        if meta is no_default:
            try:
                meta = self._meta_nonempty.map(arg, na_action=na_action)
            except Exception:
                raise ValueError("Metadata inference failed, please provide "
                                "`meta` keyword")
        else:
            meta = make_meta(meta)

        return Series(dsk, name, meta, self.divisions)

    @derived_from(pd.Series)
    def astype(self, dtype):
        return self.map_partitions(pd.Series.astype, dtype=dtype,
                                   meta=self._meta.astype(dtype))

    @derived_from(pd.Series)
    def dropna(self):
        return self.map_partitions(pd.Series.dropna)

    @derived_from(pd.Series)
    def between(self, left, right, inclusive=True):
        return self.map_partitions(pd.Series.between, left=left,
                                   right=right, inclusive=inclusive)

    @derived_from(pd.Series)
    def clip(self, lower=None, upper=None):
        return self.map_partitions(pd.Series.clip, lower=lower, upper=upper)

    @derived_from(pd.Series)
    def notnull(self):
        return self.map_partitions(pd.Series.notnull)

    @derived_from(pd.Series)
    def isnull(self):
        return self.map_partitions(pd.Series.isnull)

    def to_bag(self, index=False):
        """Convert to a dask Bag.

        Parameters
        ----------
        index : bool, optional
            If True, the elements are tuples of ``(index, value)``, otherwise
            they're just the ``value``.  Default is False.
        """
        from .io import to_bag
        return to_bag(self, index)

    @derived_from(pd.Series)
    def to_frame(self, name=None):
        return self.map_partitions(pd.Series.to_frame, name,
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

            try:
                meta = _emulate(pd.Series.apply, self._meta_nonempty, func,
                                convert_dtype=convert_dtype,
                                args=args, **kwds)
            except Exception:
                raise ValueError("Metadata inference failed, please provide "
                                 "`meta` keyword")

        return map_partitions(pd.Series.apply, self, func,
                              convert_dtype, args, meta=meta, **kwds)

    @derived_from(pd.Series)
    def cov(self, other, min_periods=None):
        from .multi import concat
        if not isinstance(other, Series):
            raise TypeError("other must be a dask.dataframe.Series")
        df = concat([self, other], axis=1)
        return cov_corr(df, min_periods, scalar=True)

    @derived_from(pd.Series)
    def corr(self, other, method='pearson', min_periods=None):
        from .multi import concat
        if not isinstance(other, Series):
            raise TypeError("other must be a dask.dataframe.Series")
        if method != 'pearson':
            raise NotImplementedError("Only Pearson correlation has been "
                                      "implemented")
        df = concat([self, other], axis=1)
        return cov_corr(df, min_periods, corr=True, scalar=True)


class Index(Series):

    _partition_type = pd.Index
    _token_prefix = 'index-'

    @property
    def index(self):
        msg = "'{0}' object has no attribute 'index'"
        raise AttributeError(msg.format(self.__class__.__name__))

    @property
    def _constructor(self):
        return Index

    def head(self, n=5, compute=True):
        """ First n items of the Index.

        Caveat, this only checks the first partition.
        """
        name = 'head-%d-%s' % (n, self._name)
        dsk = {(name, 0): (lambda x, n: x[:n], (self._name, 0), n)}

        result = self._constructor(merge(self.dask, dsk), name,
                                   self._meta, self.divisions[:2])

        if compute:
            result = result.compute()
        return result

    def nunique(self):
        return self.drop_duplicates().count()

    @derived_from(pd.Index)
    def max(self):
        # it doesn't support axis and skipna kwds
        return self._aca_agg(token='max', func=_max,
                             meta=self._meta_nonempty.max())

    @derived_from(pd.Index)
    def min(self):
        return self._aca_agg(token='min', func=_min,
                             meta=self._meta_nonempty.min())

    def count(self):
        f = lambda x: pd.notnull(x).sum()
        return reduction(self, f, np.sum, token='index-count', meta=int)


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

    def __init__(self, dsk, name, meta, divisions):
        super(DataFrame, self).__init__()
        self.dask = dsk
        self._name = name
        self._meta = make_meta(meta)
        self.divisions = tuple(divisions)

    @property
    def _constructor_sliced(self):
        return Series

    @property
    def _constructor(self):
        return DataFrame

    @property
    def columns(self):
        return self._meta.columns

    @columns.setter
    def columns(self, columns):
        # if length mismatches, error is raised from pandas
        self._meta.columns = columns
        renamed = _rename_dask(self, columns)
        # update myself
        self.dask.update(renamed.dask)
        self._name = renamed._name

    def __getitem__(self, key):
        name = 'getitem-%s' % tokenize(self, key)
        if np.isscalar(key):

            if isinstance(self._meta.index, (pd.DatetimeIndex, pd.PeriodIndex)):
                if key not in self._meta.columns:
                    return self._loc(key)

            # error is raised from pandas
            meta = self._meta[_extract_meta(key)]
            dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                       for i in range(self.npartitions))
            return self._constructor_sliced(merge(self.dask, dsk), name,
                                            meta, self.divisions)
        elif isinstance(key, slice):
            return self._loc(key)

        if isinstance(key, list):
            # error is raised from pandas
            meta = self._meta[_extract_meta(key)]

            dsk = dict(((name, i), (operator.getitem,
                                    (self._name, i), (list, key)))
                        for i in range(self.npartitions))
            return self._constructor(merge(self.dask, dsk), name,
                                     meta, self.divisions)
        if isinstance(key, Series):
            # do not perform dummy calculation, as columns will not be changed.
            #
            if self.divisions != key.divisions:
                from .multi import _maybe_align_partitions
                self, key = _maybe_align_partitions([self, key])
            dsk = dict(((name, i), (self._partition_type._getitem_array,
                                     (self._name, i),
                                     (key._name, i)))
                        for i in range(self.npartitions))
            return self._constructor(merge(self.dask, key.dask, dsk), name,
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

    def __getattr__(self, key):
        if key in self.columns:
            meta = self._meta[key]
            name = 'getitem-%s' % tokenize(self, key)
            dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                       for i in range(self.npartitions))
            return self._constructor_sliced(merge(self.dask, dsk), name,
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
    def notnull(self):
        return self.map_partitions(pd.DataFrame.notnull)

    @derived_from(pd.DataFrame)
    def isnull(self):
        return self.map_partitions(pd.DataFrame.isnull)

    def set_index(self, other, drop=True, sorted=False, **kwargs):
        """ Set the DataFrame index (row labels) using an existing column

        This operation in dask.dataframe is expensive.  If the input column is
        sorted then we accomplish the set_index in a single full read of that
        column.  However, if the input column is not sorted then this operation
        triggers a full shuffle, which can take a while and only works on a
        single machine (not distributed).

        Parameters
        ----------
        other: Series or label
        drop: boolean, default True
            Delete columns to be used as the new index
        sorted: boolean, default False
            Set to True if the new index column is already sorted

        Examples
        --------
        >>> df.set_index('x')  # doctest: +SKIP
        >>> df.set_index(d.x)  # doctest: +SKIP
        >>> df.set_index(d.timestamp, sorted=True)  # doctest: +SKIP
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
        from .shuffle import set_partition
        return set_partition(self, column, divisions, **kwargs)

    @property
    def column_info(self):
        """ Return DataFrame.columns """
        warnings.warn('column_info is deprecated, use columns')
        return self.columns

    @derived_from(pd.DataFrame)
    def nlargest(self, n=5, columns=None):
        token = 'dataframe-nlargest-n={0}'.format(n)
        f = lambda df: df.nlargest(n, columns)
        return aca(self, f, f, meta=self._meta, token=token)

    @derived_from(pd.DataFrame)
    def reset_index(self):
        out = self.map_partitions(self._partition_type.reset_index)
        out.divisions = [None] * (self.npartitions + 1)
        return out

    @derived_from(pd.DataFrame)
    def groupby(self, key, **kwargs):
        from dask.dataframe.groupby import DataFrameGroupBy
        return DataFrameGroupBy(self, key, **kwargs)

    def categorize(self, columns=None, **kwargs):
        """
        Convert columns of the DataFrame to catefory dtype

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
        return elemwise(_assign, self, *pairs, meta=df2)

    @derived_from(pd.DataFrame)
    def rename(self, index=None, columns=None):
        if index is not None:
            raise ValueError("Cannot rename index.")

        # *args here is index, columns but columns arg is already used
        return self.map_partitions(pd.DataFrame.rename, None, columns)

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
            dsk = dict(((name, i), (apply, pd.DataFrame.query,
                                    ((self._name, i), (expr,), kwargs)))
                       for i in range(self.npartitions))
        else:
            dsk = dict(((name, i), (pd.DataFrame.query, (self._name, i), expr))
                       for i in range(self.npartitions))

        meta = self._meta.query(expr, **kwargs)
        return self._constructor(merge(dsk, self.dask), name,
                                 meta, self.divisions)

    @derived_from(pd.DataFrame)
    def eval(self, expr, inplace=None, **kwargs):
        if '=' in expr and inplace in (True, None):
            raise NotImplementedError("Inplace eval not supported."
            " Please use inplace=False")
        meta = self._meta.eval(expr, inplace=inplace, **kwargs)
        return self.map_partitions(_eval, expr, meta=meta, inplace=inplace, **kwargs)

    @derived_from(pd.DataFrame)
    def dropna(self, how='any', subset=None):
        # for cloudpickle
        def f(df, how=how, subset=subset):
            return df.dropna(how=how, subset=subset)
        return self.map_partitions(f, how=how, subset=subset)

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
            return self.map_partitions(pd.DataFrame._get_numeric_data,
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

    def _aca_agg(self, token, func, aggfunc=None, meta=no_default, **kwargs):
        """ Wrapper for aggregations """
        if aggfunc is None:
            aggfunc = func

        def aggregate(x, **kwargs):
            return x.groupby(level=0).apply(aggfunc, **kwargs)

        # groupby.aggregation doesn't support skipna,
        # using gropuby.apply(aggfunc) is a workaround to handle each group as df
        return aca([self], chunk=func, aggregate=aggregate,
                   meta=meta, token=self._token_prefix + token,
                   **kwargs)

    @derived_from(pd.DataFrame)
    def drop(self, labels, axis=0, dtype=None):
        if axis != 1:
            raise NotImplementedError("Drop currently only works for axis=1")

        if dtype is not None:
            return elemwise(drop_columns, self, labels, dtype)
        else:
            return elemwise(pd.DataFrame.drop, self, labels, axis)

    @derived_from(pd.DataFrame)
    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False,
              suffixes=('_x', '_y'), npartitions=None, shuffle=None):

        if not isinstance(right, (DataFrame, pd.DataFrame)):
            raise ValueError('right must be DataFrame')

        from .multi import merge
        return merge(self, right, how=how, on=on,
                     left_on=left_on, right_on=right_on,
                     left_index=left_index, right_index=right_index,
                     suffixes=suffixes, npartitions=npartitions, shuffle=shuffle)

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

            if axis == 1:
                # when axis=1, series will be added to each row
                # it not supported for dd.Series.
                # dd.DataFrame is not affected as op is applied elemwise
                if isinstance(other, Series):
                    msg = 'Unable to {0} dd.Series with axis=1'.format(name)
                    raise ValueError(msg)

            meta = _emulate(op, self, other, axis=axis, fill_value=fill_value)
            return map_partitions(op, self, other, meta=meta,
                                  axis=axis, fill_value=fill_value)
        meth.__doc__ = op.__doc__
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
            raise NotImplementedError(
                    "dd.DataFrame.apply only supports axis=1\n"
                    "  Try: df.apply(func, axis=1)")

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

            try:
                meta = _emulate(pd.DataFrame.apply, self._meta_nonempty, func,
                                axis=axis, args=args, **kwds)
            except Exception:
                raise ValueError("Metadata inference failed, please provide "
                                 "`meta` keyword")

        return map_partitions(pd.DataFrame.apply, self, func, axis,
                              False, False, None, args, meta=meta, **kwds)

    @derived_from(pd.DataFrame)
    def cov(self, min_periods=None):
        return cov_corr(self, min_periods)

    @derived_from(pd.DataFrame)
    def corr(self, method='pearson', min_periods=None):
        if method != 'pearson':
            raise NotImplementedError("Only Pearson correlation has been "
                                      "implemented")
        return cov_corr(self, min_periods, True)

    @derived_from(pd.DataFrame)
    def astype(self, dtype):
        meta = self._meta.astype(dtype)
        return self.map_partitions(pd.DataFrame.astype, meta=meta, dtype=dtype)

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
            computations.update({'memory_usage': self.map_partitions(pd.DataFrame.memory_usage, index=True)})
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
        dtype_counts = ['%s(%d)' % k for k in sorted(self.dtypes.value_counts().iteritems())]
        lines.append('dtypes: {}'.format(', '.join(dtype_counts)))

        if memory_usage:
            memory_int = computations['memory_usage'].sum()
            lines.append('memory usage: {}\n'.format(memory_repr(memory_int)))

        put_lines(buf, lines)


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


def elemwise_property(attr, s):
    meta = pd.Series([], dtype=getattr(s._meta, attr).dtype)
    return map_partitions(getattr, s, attr, meta=meta)

for name in ['nanosecond', 'microsecond', 'millisecond', 'second', 'minute',
             'hour', 'day', 'dayofweek', 'dayofyear', 'week', 'weekday',
             'weekofyear', 'month', 'quarter', 'year']:
    setattr(Index, name, property(partial(elemwise_property, name)))


def _assign(df, *pairs):
    kwargs = dict(partition(2, pairs))
    return df.assign(**kwargs)


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


def remove_empties(seq):
    """ Remove items of length 0

    >>> remove_empties([1, 2, ('empty', np.nan), 4, 5])
    [1, 2, 4, 5]

    >>> remove_empties([('empty', np.nan)])
    [nan]

    >>> remove_empties([])
    []
    """
    if not seq:
        return seq

    seq2 = [x for x in seq
              if not (isinstance(x, tuple) and x and x[0] == 'empty')]
    if seq2:
        return seq2
    else:
        return [seq[0][1]]


def empty_safe(func, arg):
    """

    >>> empty_safe(sum, [1, 2, 3])
    6
    >>> empty_safe(sum, [])
    ('empty', 0)
    """
    if len(arg) == 0:
        return ('empty', func(arg))
    else:
        return func(arg)


def reduction(x, chunk, aggregate, token=None, meta=no_default):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    token_key = tokenize(x, token or (chunk, aggregate))
    token = token or 'reduction'
    a = '{0}--chunk-{1}'.format(token, token_key)
    dsk = dict(((a, i), (empty_safe, chunk, (x._name, i)))
               for i in range(x.npartitions))

    b = '{0}--aggregation-{1}'.format(token, token_key)
    dsk2 = {(b, 0): (aggregate, (remove_empties,
                     [(a, i) for i in range(x.npartitions)]))}

    if meta is no_default:
        try:
            meta_chunk = _emulate(empty_safe, chunk, x)
            meta = _emulate(aggregate, remove_empties([meta_chunk]))
        except Exception:
            raise ValueError("Metadata inference failed, please provide "
                             "`meta` keyword")
    meta = make_meta(meta)

    return Scalar(merge(x.dask, dsk, dsk2), b, meta)


def _maybe_from_pandas(dfs):
    from .io import from_pandas
    dfs = [from_pandas(df, 1) if isinstance(df, (pd.Series, pd.DataFrame))
           else df for df in dfs]
    return dfs


@insert_meta_param_description
def apply_concat_apply(args, chunk=None, aggregate=None,
                       meta=no_default, token=None, chunk_kwargs=None,
                       aggregate_kwargs=None, **kwargs):
    """ Apply a function to blocks, the concat, then apply again

    Parameters
    ----------
    args :
        Positional arguments for the `chunk` function. All `dask.dataframe`
        objects should be partitioned and indexed equivalently.
    chunk : function [block-per-arg] -> block
        Function to operate on each block of data
    aggregate : function concatenated-block -> block
        Function to operate on the concatenated result of chunk
    $META
    token : str, optional
        The name to use for the output keys.
    chunk_kwargs : dict, optional
        Keywords for the chunk function only.
    aggregate_kwargs : dict, optional
        Keywords for the aggregate function only.
    kwargs :
        All remaining keywords will be passed to both ``chunk`` and
        ``aggregate``.

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

    if not isinstance(args, (tuple, list)):
        args = [args]

    assert all(arg.npartitions == args[0].npartitions
               for arg in args if isinstance(arg, _Frame))

    token_key = tokenize(token or (chunk, aggregate), meta, *args)
    token = token or 'apply-concat-apply'

    a = '{0}--first-{1}'.format(token, token_key)
    if len(args) == 1 and isinstance(args[0], _Frame) and not chunk_kwargs:
        dsk = dict(((a, i), (chunk, key))
                   for i, key in enumerate(args[0]._keys()))
    else:
        dsk = dict(((a, i), (apply, chunk, [(x._name, i)
                                            if isinstance(x, _Frame)
                                            else x for x in args],
                             chunk_kwargs))
               for i in range(args[0].npartitions))

    b = '{0}--second-{1}'.format(token, token_key)
    conc = (_concat, (list, [(a, i) for i in range(args[0].npartitions)]))
    if not aggregate_kwargs:
        dsk2 = {(b, 0): (aggregate, conc)}
    else:
        dsk2 = {(b, 0): (apply, aggregate, [conc], aggregate_kwargs)}

    if meta is no_default:
        try:
            meta_chunk = _emulate(apply, chunk, args, chunk_kwargs)
            meta = _emulate(apply, aggregate, [meta_chunk], aggregate_kwargs)
        except Exception:
            raise ValueError("Metadata inference failed, please provide "
                             "`meta` keyword")
    meta = make_meta(meta)

    dasks = [a.dask for a in args if isinstance(a, _Frame)]
    return new_dd_object(merge(dsk, dsk2, *dasks), b, meta, [None, None])


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
    return func(*_extract_meta(args, True), **_extract_meta(kwargs, True))


@insert_meta_param_description
def map_partitions(func, *args, **kwargs):
    """ Apply Python function on each DataFrame partition.

    Parameters
    ----------
    func : function
        Function applied to each partition.
    args, kwargs :
        Arguments and keywords to pass to the function.
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
        try:
            meta = _emulate(func, *args, **kwargs)
        except Exception:
            raise ValueError("Metadata inference failed, please provide "
                             "`meta` keyword")

    if all(isinstance(arg, Scalar) for arg in args):
        dask = {(name, 0):
                (apply, func, (tuple, [(arg._name, 0) for arg in args]), kwargs)}
        return Scalar(merge(dask, *[arg.dask for arg in args]), name, meta)

    if isinstance(meta, pd.DataFrame):
        columns = meta.columns
    elif isinstance(meta, (pd.Series, pd.Index)):
        columns = meta.name
    else:
        columns = None

    dfs = [df for df in args if isinstance(df, _Frame)]
    dsk = {}
    for i in range(dfs[0].npartitions):
        values = [(arg._name, i if isinstance(arg, _Frame) else 0)
                  if isinstance(arg, (_Frame, Scalar)) else arg for arg in args]
        values = (apply, func, (tuple, values), kwargs)
        if columns is not None:
            values = (_rename, columns, values)
        dsk[(name, i)] = values

    dasks = [arg.dask for arg in args if isinstance(arg, (_Frame, Scalar))]
    return new_dd_object(merge(dsk, *dasks), name, meta, args[0].divisions)


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

    if isinstance(columns, Iterator):
        columns = list(columns)

    if columns is no_default:
        return df

    if isinstance(df, pd.DataFrame):
        if isinstance(columns, pd.DataFrame):
            columns = columns.columns
        columns = pd.Index(columns)
        if len(columns) == len(df.columns):
            if columns.equals(df.columns):
                # if target is identical, rename is not necessary
                return df
            # each functions must be pure op, do not use df.columns = columns
            return df.rename(columns=dict(zip(df.columns, columns)))
    elif isinstance(df, (pd.Series, pd.Index)):
        if isinstance(columns, (pd.Series, pd.Index)):
            columns = columns.name
        if name == columns:
            return df
        return pd.Series(df, name=columns)
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
    """ Approximate quantiles of Series / single column DataFrame

    Parameters
    ----------
    q : list/array of floats
        Iterable of numbers ranging from 0 to 100 for the desired quantiles
    """
    assert (isinstance(df, DataFrame) and len(df.columns) == 1 or
            isinstance(df, Series))
    from dask.array.percentile import _percentile, merge_percentiles

    # currently, only Series has quantile method
    if isinstance(q, (list, tuple, np.ndarray)):
        # Index.quantile(list-like) must be pd.Series, not pd.Index
        df_name = df.name
        merge_type = lambda v: pd.Series(v, index=q, name=df_name)
        return_type = Series if isinstance(df, Index) else df._constructor
    else:
        typ = df._partition_type
        merge_type = lambda v: typ(v).item()
        return_type = df._constructor_sliced
        q = [q]

    if isinstance(df, Index):
        meta = pd.Series(df._meta_nonempty).quantile(q)
    else:
        meta = df._meta_nonempty.quantile(q)

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
    merge_dsk = {(name3, 0): (merge_type, (merge_percentiles, qs, [qs] * df.npartitions,
                                          sorted(val_dsk), sorted(len_dsk)))}
    dsk = merge(df.dask, val_dsk, len_dsk, merge_dsk)
    return return_type(dsk, name3, meta, new_divisions)


def cov_corr(df, min_periods=None, corr=False, scalar=False):
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
    """
    if min_periods is None:
        min_periods = 2
    elif min_periods < 2:
        raise ValueError("min_periods must be >= 2")
    prefix = 'corr' if corr else 'cov'
    df = df._get_numeric_data()
    name = '{0}-agg-{1}'.format(prefix, tokenize(df, min_periods, scalar))
    if scalar and len(df.columns) != 2:
        raise ValueError("scalar only valid for 2 column dataframe")
    k = '{0}-chunk-{1}'.format(prefix, df._name)
    dsk = dict(((k, i), (cov_corr_chunk, f, corr))
               for (i, f) in enumerate(df._keys()))
    dsk[(name, 0)] = (cov_corr_agg, list(dsk.keys()), df._meta, min_periods,
                      corr, scalar)
    dsk = merge(df.dask, dsk)
    if scalar:
        return Scalar(dsk, name, 'f8')
    meta = make_meta([(c, 'f8') for c in df.columns], index=df._meta.columns)
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
        m = np.nansum((x - sums/np.where(counts, counts, np.nan))**2, 0)
        dtype.append(('m', m.dtype))

    out = np.empty(counts.shape, dtype=dtype)
    out['sum'] = sums
    out['count'] = counts
    out['cov'] = cov * (counts - 1)
    if corr:
        out['m'] = m
    return out


def cov_corr_agg(data, meta, min_periods=2, corr=False, scalar=False):
    """Aggregation part of a covariance or correlation computation"""
    data = np.concatenate(data).reshape((len(data),) + data[0].shape)
    sums = np.nan_to_num(data['sum'])
    counts = data['count']

    cum_sums = np.cumsum(sums, 0)
    cum_counts = np.cumsum(counts, 0)

    s1 = cum_sums[:-1]
    s2 = sums[1:]
    n1 = cum_counts[:-1]
    n2 = counts[1:]
    d = (s2/n2) - (s1/n1)
    C = (np.nansum((n1 * n2)/(n1 + n2) * (d * d.transpose((0, 2, 1))), 0) +
         np.nansum(data['cov'], 0))

    C[cum_counts[-1] < min_periods] = np.nan
    nobs = np.where(cum_counts[-1], cum_counts[-1], np.nan)
    if corr:
        mu = cum_sums[-1] / nobs
        counts_na = np.where(counts, counts, np.nan)
        m2 = np.nansum(data['m'] + counts*(sums/counts_na - mu)**2, axis=0)
        den = np.sqrt(m2 * m2.T)
    else:
        den = nobs - 1
    mat = C/den
    if scalar:
        return mat[0, 1]
    return pd.DataFrame(mat, columns=meta.columns, index=meta.columns)


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
    {('b', 0): (<function _loc at ...>, ('a', 0), 1, 3, False),
     ('b', 1): (<function _loc at ...>, ('a', 1), 3, 4, False),
     ('b', 2): (<function _loc at ...>, ('a', 1), 4, 6, False),
     ('b', 3): (<function _loc at ...>, ('a', 1), 6, 7, False)
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
            # (_loc, ('from_pandas-#', 0), 3, 4, False))
            d[(out1, k)] = (_loc, (name, i - 1), low, a[i], False)
            low = a[i]
            i += 1
        elif a[i] > b[j]:
            d[(out1, k)] = (_loc, (name, i - 1), low, b[j], False)
            low = b[j]
            j += 1
        else:
            d[(out1, k)] = (_loc, (name, i - 1), low, b[j], False)
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
            d[(out1, k)] = (_loc, (name, m), low, b[_j], False)
            low = b[_j]
            c.append(low)
            k += 1
    else:
        # even if new division is processed through,
        # right-most element of old division can remain
        if last_elem and i < len(a):
            d[(out1, k)] = (_loc, (name, i - 1), a[i], a[i], False)
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
            d[(out2, j - 1)] = (_loc, (name, 0), a[0], a[0], False)
        elif len(tmp) == 1:
            d[(out2, j - 1)] = tmp[0]
        else:
            if not tmp:
                raise ValueError('check for duplicate partitions\nold:\n%s\n\n'
                                 'new:\n%s\n\ncombined:\n%s'
                                 % (pformat(a), pformat(b), pformat(c)))
            d[(out2, j - 1)] = (pd.concat, (list, tmp))
        j += 1
    return d


def repartition_npartitions(df, npartitions):
    """ Repartition dataframe to a smaller number of partitions """
    npartitions_ratio = df.npartitions / npartitions
    new_partitions_boundaries = [int(new_partition_index * npartitions_ratio)
                                    for new_partition_index in range(npartitions + 1)]
    new_name = 'repartition-%d-%s' % (npartitions, tokenize(df))
    dsk = {(new_name, new_partition_index):
            (pd.concat,
             [(df._name, old_partition_index)
                for old_partition_index in range(
                    new_partitions_boundaries[new_partition_index],
                    new_partitions_boundaries[new_partition_index + 1])])
            for new_partition_index in range(npartitions)}
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
        return df._constructor(merge(df.dask, dsk), out,
                               df._meta, divisions)
    elif isinstance(df, (pd.Series, pd.DataFrame)):
        name = 'repartition-dataframe-' + token
        from .utils import shard_df_on_index
        dfs = shard_df_on_index(df, divisions[1:-1])
        dsk = dict(((name, i), df) for i, df in enumerate(dfs))
        return new_dd_object(dsk, name, df, divisions)
    raise ValueError('Data must be DataFrame or Series')


class Accessor(object):
    def __init__(self, series):
        if not isinstance(series, Series):
            raise ValueError('Accessor cannot be initialized')
        self._series = series

    def _property_map(self, key):
        out = self.getattr(self._series._meta, key)
        meta = pd.Series([], dtype=out.dtype, name=getattr(out, 'name', None))
        return map_partitions(self.getattr, self._series, key, meta=meta)

    def _function_map(self, key, *args):
        out = self.call(self._series._meta, key, *args)
        meta = pd.Series([], dtype=out.dtype, name=getattr(out, 'name', None))
        return map_partitions(self.call, self._series, key, *args, meta=meta)

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      dir(self.ns)))

    def __getattr__(self, key):
        if key in dir(self.ns):
            if isinstance(getattr(self.ns, key), property):
                return self._property_map(key)
            else:
                return partial(self._function_map, key)
        else:
            raise AttributeError(key)


class DatetimeAccessor(Accessor):
    """ Accessor object for datetimelike properties of the Series values.

    Examples
    --------

    >>> s.dt.microsecond  # doctest: +SKIP
    """
    ns = pd.Series.dt

    @staticmethod
    def getattr(obj, attr):
        return getattr(obj.dt, attr)

    @staticmethod
    def call(obj, attr, *args):
        return getattr(obj.dt, attr)(*args)


class StringAccessor(Accessor):
    """ Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """
    ns = pd.Series.str

    @staticmethod
    def getattr(obj, attr):
        return getattr(obj.str, attr)

    @staticmethod
    def call(obj, attr, *args):
        return getattr(obj.str, attr)(*args)


def set_sorted_index(df, index, drop=True, **kwargs):
    if not isinstance(index, Series):
        meta = df._meta.set_index(index, drop=drop)
    else:
        meta = df._meta.set_index(index._meta, drop=drop)

    result = map_partitions(_set_sorted_index, df, index, drop=drop, meta=meta)

    return compute_divisions(result, **kwargs)


def compute_divisions(df, **kwargs):
    mins = df.index.map_partitions(lambda x: pd.Series(x.min()), meta=df.index)
    maxes = df.index.map_partitions(lambda x: pd.Series(x.max()), meta=df.index)
    mins, maxes = compute(mins, maxes, **kwargs)

    if (sorted(mins) != list(mins) or
            sorted(maxes) != list(maxes) or
            any(a >= b for a, b in zip(mins, maxes))):
        raise ValueError("Partitions must be sorted ascending with the index",
                         mins, maxes)

    divisions = tuple(mins) + (list(maxes)[-1],)

    df = copy(df)
    df.divisions = divisions

    return df


def _set_sorted_index(df, idx, drop):
    return df.set_index(idx, drop=drop)


def _eval(df, expr, **kwargs):
    return df.eval(expr, **kwargs)


def _sum(x, **kwargs):
    return x.sum(**kwargs)


def _min(x, **kwargs):
    return x.min(**kwargs)


def _max(x, **kwargs):
    return x.max(**kwargs)


def _idxmax(x, **kwargs):
    return x.idxmax(**kwargs)


def _idxmin(x, **kwargs):
    return x.idxmin(**kwargs)


def _count(x, **kwargs):
    return x.count(**kwargs)


def _mean(x, **kwargs):
    return x.mean(**kwargs)


def _var(x, **kwargs):
    return x.var(**kwargs)


def _std(x, **kwargs):
    return x.std(**kwargs)


def drop_columns(df, columns, dtype):
    df = df.drop(columns, axis=1)
    df.columns = df.columns.astype(dtype)
    return df


def idxmaxmin_chunk(x, fn, axis=0, skipna=True, **kwargs):
    idx = getattr(x, fn)(axis=axis, skipna=skipna)
    minmax = 'max' if fn == 'idxmax' else 'min'
    value = getattr(x, minmax)(axis=axis, skipna=skipna)
    n = len(x)
    if isinstance(idx, pd.Series):
        chunk = pd.DataFrame({'idx': idx, 'value': value, 'n': [n] * len(idx)})
        chunk['idx'] = chunk['idx'].astype(type(idx.iloc[0]))
    else:
        chunk = pd.DataFrame({'idx': [idx], 'value': [value], 'n': [n]})
        chunk['idx'] = chunk['idx'].astype(type(idx))
    return chunk


def idxmaxmin_row(x, fn, skipna=True):
    idx = x.idx.reset_index(drop=True)
    value = x.value.reset_index(drop=True)
    subidx = getattr(value, fn)(skipna=skipna)

    # if skipna is False, pandas returns NaN so mimic behavior
    if pd.isnull(subidx):
        return subidx

    return idx.iloc[subidx]


def idxmaxmin_agg(x, fn, skipna=True, **kwargs):
    indices = list(set(x.index.tolist()))
    idxmaxmin = [idxmaxmin_row(x.ix[idx], fn, skipna=skipna) for idx in indices]
    if len(idxmaxmin) == 1:
        return idxmaxmin[0]
    else:
        return pd.Series(idxmaxmin, index=indices)


def safe_head(head, df, n):
    r = head(df, n=n)
    if len(r) != n:
        warnings.warn("Insufficient elements for `head`. {0} elements "
             "requested, only {1} elements available. Try passing larger "
             "`npartitions` to `head`.".format(n, len(r)))
    return r
