from __future__ import division

from itertools import count
from math import sqrt
from functools import wraps, reduce
from collections import Iterable
import bisect
import uuid
from hashlib import md5
from toolz import merge, partial, first, partition
from operator import getitem, setitem
from datetime import datetime
import pandas as pd
import numpy as np
import operator
try:
    from chest import Chest as Cache
except ImportError:
    Cache = dict

from .. import array as da
from .. import core
from ..array.core import partial_by_order
from .. import threaded
from ..compatibility import unicode, apply
from ..utils import repr_long_list, IndexCallable, pseudorandom
from .utils import shard_df_on_index, tokenize_dataframe
from ..base import Base, compute


no_default = '__no_default__'


def tokenize(obj):
    """ Deterministic token

    >>> tokenize([1, 2, '3'])
    'b9e8c0d38fb40e66dc4fd00adc3c6553'

    >>> tokenize('Hello') == tokenize('Hello')
    True
    """
    return md5(str(obj).encode()).hexdigest()

def _concat(args):
    """ Generic concat operation """
    if not args:
        return args
    if isinstance(first(core.flatten(args)), np.ndarray):
        return da.core.concatenate3(args)
    if len(args) == 1:
        return args[0]
    if isinstance(args[0], (pd.DataFrame, pd.Series)):
        args2 = [arg for arg in args if len(arg)]
        if not args2:
            return args[0]
        return pd.concat(args2)
    if isinstance(args[0], (pd.Index)):
        args = [arg for arg in args if len(arg)]
        result = pd.concat(map(pd.Series, args))
        result = type(args[0])(result.values)
        result.name = args[0].name
        return result
    return args


tokens = ('-%d' % i for i in count(1))


def optimize(dsk, keys):
    from .optimize import optimize
    return optimize(dsk, keys)


def finalize(self, results):
    return _concat(results)


class Scalar(Base):
    """ A Dask-thing to represent a scalar

    TODO: Clean up this abstraction
    """

    _optimize = staticmethod(optimize)
    _default_get = staticmethod(threaded.get)
    _finalize = staticmethod(finalize)

    def __init__(self, dsk, _name, name=None, divisions=None):
        self.dask = dsk
        self._name = _name
        self.divisions = [None, None]

        # name and divisions are ignored.
        # There are dummies to be compat with Series and DataFrame

    @property
    def _args(self):
        return (self.dask, self._name)

    def _keys(self):
        return [(self._name, 0)]


class _Frame(Base):
    """ Superclass for DataFrame and Series """

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
        return len(self.divisions) - 1

    def _keys(self):
        return [(self._name, i) for i in range(self.npartitions)]

    @property
    def index(self):
        name = self._name + '-index'
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))
        return Index(merge(dsk, self.dask), name, None, self.divisions)

    @property
    def known_divisions(self):
        return len(self.divisions) > 0 and self.divisions[0] is not None

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
        return self._constructor(dsk2, name, self.column_info, self.divisions)

    @wraps(pd.DataFrame.drop_duplicates)
    def drop_duplicates(self):
        chunk = lambda s: s.drop_duplicates()
        return aca(self, chunk=chunk, aggregate=chunk, columns=self.column_info,
                   token='drop-duplicates')

    def __len__(self):
        return reduction(self, len, np.sum).compute()

    def map_partitions(self, func, columns=no_default):
        """ Apply Python function on each DataFrame block

        When using ``map_partitions`` you should provide either the column
        names (if the result is a DataFrame) or the name of the Series (if the
        result is a Series).  The output type will be determined by the type of
        ``columns``.

        >>> df.map_partitions(lambda df: df.x + 1, columns='x')  # doctest: +SKIP

        >>> df.map_partitions(lambda df: df.head(), columns=df.columns)  # doctest: +SKIP

        Parameters
        ----------

        column_info: tuple or string
            Column names or name of the output.
            Defaults to names of the input.
        """
        if columns == no_default:
            columns = self.column_info
        return map_partitions(func, columns, self)

    def random_split(self, p, seed=None):
        """ Pseudorandomly split dataframe into different pieces row-wise

        50/50 split
        >>> a, b = df.random_split([0.5, 0.5])  # doctest: +SKIP

        80/10/10 split, consistent seed
        >>> a, b, c = df.random_split([0.8, 0.1, 0.1], seed=123)  # doctest: +SKIP
        """
        seeds = np.random.RandomState(seed).randint(0, np.iinfo(np.int32).max,
                                                    self.npartitions)
        dsk_full = dict(((self._name + '-split-full', i),
                         (pd_split, (self._name, i), p, seed))
                       for i, seed in enumerate(seeds))

        dsks = [dict(((self._name + '-split-%d' % i, j),
                      (getitem, (self._name + '-split-full', j), i))
                      for j in range(self.npartitions))
                      for i in range(len(p))]
        return [type(self)(merge(self.dask, dsk_full, dsk),
                           self._name + '-split-%d' % i,
                           self.column_info,
                           self.divisions)
                for i, dsk in enumerate(dsks)]

    def head(self, n=5, compute=True):
        """ First n rows of the dataset

        Caveat, the only checks the first n rows of the first partition.
        """
        name = 'head-%d-%s' % (n, self._name)
        dsk = {(name, 0): (lambda x, n: x.head(n=n), (self._name, 0), n)}

        result = self._constructor(merge(self.dask, dsk), name,
                                   self.column_info, self.divisions[:2])

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
                                   self.column_info, self.divisions[-2:])

        if compute:
            result = result.compute()
        return result

    def _loc(self, ind):
        """ Helper function for the .loc accessor """
        if isinstance(ind, Series):
            return self._loc_series(ind)
        elif isinstance(ind, slice):
            return self._loc_slice(ind)
        else:
            return self._loc_element(ind)

    def _loc_series(self, ind):
        if not self.divisions == ind.divisions:
            raise ValueError("Partitions of dataframe and index not the same")
        return map_partitions(lambda df, ind: df.loc[ind],
                              self.columns, self, ind, token='loc-series')

    def _loc_element(self, ind):
        name = 'loc-element-%s-%s' % (str(ind), self._name)
        part = _partition_of_index_value(self.divisions, ind)
        if ind < self.divisions[0] or ind > self.divisions[-1]:
            raise KeyError('the label [%s] is not in the index' % str(ind))
        dsk = {(name, 0): (lambda df: df.loc[ind], (self._name, part))}

        if self.ndim == 1:
            columns = self.column_info
        else:
            columns = ind
        return self._constructor_sliced(merge(self.dask, dsk), name,
                                        columns, [ind, ind])

    def _loc_slice(self, ind):
        name = 'loc-slice-%s-%s' % (str(ind), self._name)
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
                                       self.column_info, divisions)

    @property
    def loc(self):
        return IndexCallable(self._loc)

    @property
    def iloc(self):
        # not implemented because of performance concerns.
        # see https://github.com/ContinuumIO/dask/pull/507
        raise AttributeError("Dask Dataframe does not support iloc")

    def repartition(self, divisions):
        """ Repartition dataframe along new divisions

        >>> df = df.repartition([0, 5, 10, 20])  # doctest: +SKIP
        """
        return repartition(self, divisions)

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, dict):
        self.__dict__ = dict

    @wraps(pd.Series.fillna)
    def fillna(self, value):
        func = getattr(self._partition_type, 'fillna')
        return map_partitions(func, self.column_info, self, value)

    def sample(self, frac, random_state=None):
        """ Random sample of items

        This only implements the ``frac`` option from pandas.

        See Also:
            pd.DataFrame.sample
        """
        if random_state is None:
            random_state = np.random.randint(np.iinfo(np.int32).max)

        name = 'sample-' + tokenize((self._name, frac, random_state))
        func = getattr(self._partition_type, 'sample')

        if not isinstance(random_state, np.random.RandomState):
            random_state = np.random.RandomState(random_state)
        seeds = random_state.randint(np.iinfo(np.int32).max,
                                     size=self.npartitions)

        dsk = dict(((name, i),
                   (apply, func, (tuple, [(self._name, i)]),
                       {'frac': frac, 'random_state': seed}))
                   for i, seed in zip(range(self.npartitions), seeds))

        return self._constructor(merge(self.dask, dsk), name,
                                       self.column_info, self.divisions)

    @wraps(pd.DataFrame.to_hdf)
    def to_hdf(self, path_or_buf, key, mode='a', append=False, complevel=0,
               complib=None, fletcher32=False, **kwargs):
        from .io import to_hdf
        return to_hdf(self, path_or_buf, key, mode, append, complevel, complib,
                fletcher32, **kwargs)

    @property
    def _elemwise_cols(self):
        """passed to elemwise ops, None for Series, columns for DataFrame"""
        return None

    def __abs__(self):
        return elemwise(operator.abs, self, columns=self._elemwise_cols)
    def __add__(self, other):
        return elemwise(operator.add, self, other, columns=self._elemwise_cols)
    def __radd__(self, other):
        return elemwise(operator.add, other, self, columns=self._elemwise_cols)
    def __and__(self, other):
        return elemwise(operator.and_, self, other, columns=self._elemwise_cols)
    def __rand__(self, other):
        return elemwise(operator.and_, other, self, columns=self._elemwise_cols)
    def __div__(self, other):
        return elemwise(operator.div, self, other, columns=self._elemwise_cols)
    def __rdiv__(self, other):
        return elemwise(operator.div, other, self, columns=self._elemwise_cols)
    def __eq__(self, other):
        return elemwise(operator.eq, self, other, columns=self._elemwise_cols)
    def __gt__(self, other):
        return elemwise(operator.gt, self, other, columns=self._elemwise_cols)
    def __ge__(self, other):
        return elemwise(operator.ge, self, other, columns=self._elemwise_cols)
    def __invert__(self):
        return elemwise(operator.inv, self, columns=self._elemwise_cols)
    def __lt__(self, other):
        return elemwise(operator.lt, self, other, columns=self._elemwise_cols)
    def __le__(self, other):
        return elemwise(operator.le, self, other, columns=self._elemwise_cols)
    def __mod__(self, other):
        return elemwise(operator.mod, self, other, columns=self._elemwise_cols)
    def __rmod__(self, other):
        return elemwise(operator.mod, other, self, columns=self._elemwise_cols)
    def __mul__(self, other):
        return elemwise(operator.mul, self, other, columns=self._elemwise_cols)
    def __rmul__(self, other):
        return elemwise(operator.mul, other, self, columns=self._elemwise_cols)
    def __ne__(self, other):
        return elemwise(operator.ne, self, other, columns=self._elemwise_cols)
    def __neg__(self):
        return elemwise(operator.neg, self, columns=self._elemwise_cols)
    def __or__(self, other):
        return elemwise(operator.or_, self, other, columns=self._elemwise_cols)
    def __ror__(self, other):
        return elemwise(operator.or_, other, self, columns=self._elemwise_cols)
    def __pow__(self, other):
        return elemwise(operator.pow, self, other, columns=self._elemwise_cols)
    def __rpow__(self, other):
        return elemwise(operator.pow, other, self, columns=self._elemwise_cols)
    def __sub__(self, other):
        return elemwise(operator.sub, self, other, columns=self._elemwise_cols)
    def __rsub__(self, other):
        return elemwise(operator.sub, other, self, columns=self._elemwise_cols)
    def __truediv__(self, other):
        return elemwise(operator.truediv, self, other, columns=self._elemwise_cols)
    def __rtruediv__(self, other):
        return elemwise(operator.truediv, other, self, columns=self._elemwise_cols)
    def __floordiv__(self, other):
        return elemwise(operator.floordiv, self, other, columns=self._elemwise_cols)
    def __rfloordiv__(self, other):
        return elemwise(operator.floordiv, other, self, columns=self._elemwise_cols)
    def __xor__(self, other):
        return elemwise(operator.xor, self, other, columns=self._elemwise_cols)
    def __rxor__(self, other):
        return elemwise(operator.xor, other, self, columns=self._elemwise_cols)


class Series(_Frame):
    """ Out-of-core Series object

    Mimics ``pandas.Series``.

    See Also
    --------

    dask.dataframe.DataFrame
    """

    _partition_type = pd.Series

    def __init__(self, dsk, _name, name, divisions):
        self.dask = dsk
        self._name = _name
        self.name = name
        self.divisions = tuple(divisions)
        self.dt = DatetimeAccessor(self)
        self.str = StringAccessor(self)

    @property
    def _args(self):
        return (self.dask, self._name, self.name, self.divisions)

    @property
    def _constructor_sliced(self):
        return Scalar

    @property
    def _constructor(self):
        return Series

    @property
    def ndim(self):
        return 1

    @property
    def dtype(self):
        return self.head().dtype

    @property
    def column_info(self):
        return self.name

    @property
    def columns(self):
        return (self.name,)

    def __repr__(self):
        return ("dd.%s<%s, divisions=%s>" %
                (self.__class__.__name__, self._name,
                 repr_long_list(self.divisions)))

    def quantile(self, q):
        """ Approximate quantiles of column

        q : list/array of floats
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        """
        # pandas uses quantile in [0, 1]
        # numpy / everyone else uses [0, 100]
        return quantile(self, np.asarray(q) * 100)

    def quantiles(self, *args, **kwargs):
        raise NotImplementedError("This has moved to quantile to match the Pandas API\n"
                                  "Also, quantiles will now be specified on the range "
                                  "[0, 1], not [0, 100]")

    def __getitem__(self, key):
        if isinstance(key, Series) and self.divisions == key.divisions:
            name = 'series-index-%s[%s]' % (self._name, key._name)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return Series(merge(self.dask, key.dask, dsk), name,
                          self.name, self.divisions)
        raise NotImplementedError()

    @wraps(pd.Series.sum)
    def sum(self):
        return reduction(self, pd.Series.sum, np.sum, token='series-sum')

    @wraps(pd.Series.max)
    def max(self):
        return reduction(self, pd.Series.max, np.max, token='series-max')

    @wraps(pd.Series.min)
    def min(self):
        return reduction(self, pd.Series.min, np.min, token='series-min')

    @wraps(pd.Series.count)
    def count(self):
        return reduction(self, pd.Series.count, np.sum, token='series-count')

    @wraps(pd.Series.nunique)
    def nunique(self):
        return self.drop_duplicates().count()

    @wraps(pd.Series.mean)
    def mean(self):
        def chunk(ser):
            return (ser.sum(), ser.count())

        def agg(seq):
            sums, counts = list(zip(*seq))
            return 1.0 * sum(sums) / sum(counts)
        return reduction(self, chunk, agg, token='series-mean')

    @wraps(pd.Series.var)
    def var(self, ddof=1):
        def chunk(ser):
            return (ser.sum(), (ser**2).sum(), ser.count())

        def agg(seq):
            x, x2, n = list(zip(*seq))
            x = float(sum(x))
            x2 = float(sum(x2))
            n = sum(n)
            result = (x2 / n) - (x / n)**2
            if ddof:
                result = result * n / (n - ddof)
            return result
        token = 'series-var(ddof={0})'.format(ddof)
        return reduction(self, chunk, agg, token=token)

    @wraps(pd.Series.std)
    def std(self, ddof=1):
        name = 'series-std(ddof={0})-{1}'.format(ddof, tokenize(self._name))
        df = self.var(ddof=ddof)
        dsk = {(name, 0): (sqrt, (df._name, 0))}
        return Scalar(merge(df.dask, dsk), name)

    @wraps(pd.Series.value_counts)
    def value_counts(self):
        chunk = lambda s: s.value_counts()
        agg = lambda s: s.groupby(level=0).sum().sort(inplace=False, ascending=False)
        return aca(self, chunk=chunk, aggregate=agg, columns=self.name,
                   token='value-counts')

    @wraps(pd.Series.nlargest)
    def nlargest(self, n=5):
        f = lambda s: s.nlargest(n)
        token = 'series-nlargest-n={0}'.format(n)
        return aca(self, f, f, columns=self.name, token=token)

    @wraps(pd.Series.isin)
    def isin(self, other):
        return elemwise(pd.Series.isin, self, other)

    @wraps(pd.Series.map)
    def map(self, arg, na_action=None):
        return elemwise(pd.Series.map, self, arg, na_action, name=self.name)

    @wraps(pd.Series.astype)
    def astype(self, dtype):
        return map_partitions(pd.Series.astype, self.name, self, dtype)

    @wraps(pd.Series.dropna)
    def dropna(self):
        return map_partitions(pd.Series.dropna, self.name, self)

    @wraps(pd.Series.between)
    def between(self, left, right, inclusive=True):
        return map_partitions(pd.Series.between, self.name, self, left, right,
                inclusive)

    @wraps(pd.Series.clip)
    def clip(self, lower=None, upper=None):
        return map_partitions(pd.Series.clip, self.name, self, lower, upper)

    @wraps(pd.Series.notnull)
    def notnull(self):
        return map_partitions(pd.Series.notnull, self.name, self)


class Index(Series):

    @property
    def index(self):
        msg = "'{0}' object has no attribute 'index'"
        raise AttributeError(msg.format(self.__class__.__name__))

    @property
    def _constructor(self):
        return Index

    def nunique(self):
        return self.drop_duplicates().count()

    def count(self):
        f = lambda x: pd.notnull(x).sum()
        return reduction(self, f, np.sum)


class DataFrame(_Frame):
    """
    Implements out-of-core DataFrame as a sequence of pandas DataFrames

    This is a work in progress.  It is buggy and far from complete.
    Please do not use it yet.

    Parameters
    ----------

    dask: dict
        The dask graph to compute this Dataframe
    name: str
        The key prefix that specifies which keys in the dask comprise this
        particular DataFrame
    columns: list of strings
        Column names.  This metadata aids usability
    divisions: tuple of index values
        Values along which we partition our blocks on the index
    """

    _partition_type = pd.DataFrame

    def __init__(self, dask, name, columns, divisions):
        self.dask = dask
        self._name = name
        self.columns = tuple(columns)
        self.divisions = tuple(divisions)

    @property
    def _args(self):
        return (self.dask, self._name, self.columns, self.divisions)

    @property
    def _constructor_sliced(self):
        return Series

    @property
    def _constructor(self):
        return DataFrame

    def __getitem__(self, key):
        if isinstance(key, (str, unicode)):
            name = self._name + '.' + key
            if key in self.columns:
                dsk = dict(((name, i), (operator.getitem, (self._name, i), key))
                            for i in range(self.npartitions))
                return self._constructor_sliced(merge(self.dask, dsk), name,
                                                      key, self.divisions)
        if isinstance(key, list):
            name = '%s[%s]' % (self._name, str(key))
            if all(k in self.columns for k in key):
                dsk = dict(((name, i), (operator.getitem,
                                         (self._name, i),
                                         (list, key)))
                            for i in range(self.npartitions))
                return self._constructor(merge(self.dask, dsk), name,
                                               key, self.divisions)
        if isinstance(key, Series) and self.divisions == key.divisions:
            name = 'series-slice-%s[%s]' % (self._name, key._name)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return self._constructor(merge(self.dask, key.dask, dsk), name,
                                           self.columns, self.divisions)
        raise NotImplementedError()

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError as e:
            try:
                return self[key]
            except NotImplementedError:
                raise e

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(self.columns)))

    def __repr__(self):
        return ("dd.DataFrame<%s, divisions=%s>" %
                (self._name, repr_long_list(self.divisions)))

    @property
    def ndim(self):
        return 2

    @property
    def dtypes(self):
        return self._get(self.dask, self._keys()[0]).dtypes

    def set_index(self, other, **kwargs):
        return set_index(self, other, **kwargs)

    def set_partition(self, column, divisions, **kwargs):
        """ Set explicit divisions for new column index

        >>> df2 = df.set_partition('new-index-column', divisions=[10, 20, 50])  # doctest: +SKIP

        See also:
            set_index
        """
        return set_partition(self, column, divisions, **kwargs)

    @property
    def column_info(self):
        return self.columns

    def groupby(self, key, **kwargs):
        return GroupBy(self, key, **kwargs)

    def categorize(self, columns=None, **kwargs):
        return categorize(self, columns, **kwargs)

    @wraps(pd.DataFrame.assign)
    def assign(self, **kwargs):
        pairs = list(sum(kwargs.items(), ()))

        # Figure out columns of the output
        df = pd.DataFrame(columns=self.columns)
        df2 = df.assign(**dict((k, []) for k in kwargs))

        return elemwise(_assign, self, *pairs, columns=list(df2.columns))

    def query(self, expr, **kwargs):
        """ Blocked version of pd.DataFrame.query

        This is like the sequential version except that this will also happen
        in many threads.  This may conflict with ``numexpr`` which will use
        multiple threads itself.  We recommend that you set numexpr to use a
        single thread

            import numexpr
            numexpr.set_nthreads(1)

        The original docstring follows below:\n
        """ + pd.DataFrame.query.__doc__
        name = '%s.query(%s)' % (self._name, expr)
        if kwargs:
            name = name + '--' + tokenize(sorted(kwargs.items()))
            dsk = dict(((name, i), (apply, pd.DataFrame.query,
                                    ((self._name, i), (expr,), kwargs)))
                       for i in range(self.npartitions))
        else:
            dsk = dict(((name, i), (pd.DataFrame.query, (self._name, i), expr))
                       for i in range(self.npartitions))

        return self._constructor(merge(dsk, self.dask), name,
                                       self.columns, self.divisions)

    @wraps(pd.DataFrame.dropna)
    def dropna(self, how='any', subset=None):
        def f(df, how=how, subset=subset):
            return df.dropna(how=how, subset=subset)
        return map_partitions(f, self.columns, self)

    def to_castra(self, fn=None, categories=None, compute=True):
        """ Write DataFrame to Castra on-disk store

        See https://github.com/blosc/castra for details

        See Also:
            Castra.to_dask
        """
        from .io import to_castra
        return to_castra(self, fn, categories, compute=compute)

    @wraps(pd.DataFrame.to_csv)
    def to_csv(self, filename, **kwargs):
        from .io import to_csv
        return to_csv(self, filename, **kwargs)

    @wraps(pd.DataFrame._get_numeric_data)
    def _get_numeric_data(self, how='any', subset=None):
        return self.map_partitions(pd.DataFrame._get_numeric_data)

    def _validate_axis(self, axis=0):
        if axis not in (0, 1, 'index', 'columns', None):
            raise ValueError('No axis named {0}'.format(axis))
        # convert to numeric axis
        return {None: 0, 'index': 0, 'columns': 1}.get(axis, axis)

    @wraps(pd.DataFrame.sum)
    def sum(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.sum(axis=1)
            return self.map_partitions(f, None)
        else:
            chunk = lambda df: df.sum()
            agg = lambda df: df.groupby(level=0).sum()
            return aca([self], chunk=chunk, aggregate=agg, columns=None,
                       token='dataframe-sum')

    @wraps(pd.DataFrame.max)
    def max(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.max(axis=1)
            return self.map_partitions(f, None)
        else:
            chunk = lambda df: df.max()
            agg = lambda df: df.groupby(level=0).max()
            return aca([self], chunk=chunk, aggregate=agg, columns=None,
                       token='dataframe-max')

    @wraps(pd.DataFrame.min)
    def min(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.min(axis=1)
            return self.map_partitions(f, None)
        else:
            chunk = lambda df: df.min()
            agg = lambda df: df.groupby(level=0).min()
            return aca([self], chunk=chunk, aggregate=agg, columns=None,
                       token='dataframe-min')

    @wraps(pd.DataFrame.count)
    def count(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.count(axis=1)
            return self.map_partitions(f, None)
        else:
            chunk = lambda df: df.count()
            agg = lambda df: df.groupby(level=0).sum()
            return aca([self], chunk=chunk, aggregate=agg, columns=None,
                       token='dataframe-count')

    @wraps(pd.DataFrame.mean)
    def mean(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.mean(axis=1)
            return self.map_partitions(f, None)
        else:
            num = self._get_numeric_data()
            return num.sum() / num.count()

    @wraps(pd.DataFrame.var)
    def var(self, axis=None, ddof=1):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x, ddof=ddof: x.var(axis=1, ddof=ddof)
            return self.map_partitions(f, None)
        else:
            num = self._get_numeric_data()
            x = 1.0 * num.sum()
            x2 = 1.0 * (num ** 2).sum()
            n = num.count()

            result = (x2 / n) - (x / n)**2
            if ddof:
                result = result * n / (n - ddof)
            return result

    @wraps(pd.DataFrame.std)
    def std(self, axis=None, ddof=1):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x, ddof=ddof: x.std(axis=1, ddof=ddof)
            return self.map_partitions(f, None)
        else:
            v = self.var(ddof=ddof)
            def apply_sqrt(s):
                return s.apply(np.sqrt)
            return map_partitions(apply_sqrt, None, v)

    @property
    def _elemwise_cols(self):
        return self.columns

    @wraps(pd.DataFrame.drop)
    def drop(self, labels, axis=0):
        if axis != 1:
            raise NotImplementedError("Drop currently only works for axis=1")

        columns = list(pd.DataFrame(columns=self.columns)
                         .drop(labels, axis=axis)
                         .columns)
        return elemwise(pd.DataFrame.drop, self, labels, axis, columns=columns)


def _assign(df, *pairs):
    kwargs = dict(partition(2, pairs))
    return df.assign(**kwargs)


def _partition_of_index_value(divisions, val):
    """ In which partition does this value lie?

    >>> _partition_of_index_value([0, 5, 10], 3)
    0
    >>> _partition_of_index_value([0, 5, 10], 8)
    1
    >>> _partition_of_index_value([0, 5, 10], 100)
    1
    >>> _partition_of_index_value([0, 5, 10], 5)  # left-inclusive divisions
    1
    """
    if divisions[0] is None:
        raise ValueError(
            "Can not use loc on DataFrame without known divisions")
    val = _coerce_loc_index(divisions, val)
    i = bisect.bisect_right(divisions, val)
    return min(len(divisions) - 2, max(0, i - 1))


def _loc(df, start, stop, include_right_boundary=True):
    """

    >>> df = pd.DataFrame({'x': [10, 20, 30, 40, 50]}, index=[1, 2, 2, 3, 4])
    >>> _loc(df, 2, None)
        x
    2  20
    2  30
    3  40
    4  50
    >>> _loc(df, 1, 3)
        x
    1  10
    2  20
    2  30
    3  40
    >>> _loc(df, 1, 3, include_right_boundary=False)
        x
    1  10
    2  20
    2  30
    """
    result = df.loc[slice(start, stop)]
    if not include_right_boundary:
        # result = df[df.index != stop]
        result = result.iloc[:result.index.get_slice_bound(stop, 'left',
                                                   result.index.inferred_type)]
    return result


def _coerce_loc_index(divisions, o):
    """ Transform values to be comparable against divisions

    This is particularly valuable to use with pandas datetimes
    """
    if divisions and isinstance(divisions[0], datetime):
        return pd.Timestamp(o)
    if divisions and isinstance(divisions[0], np.datetime64):
        return np.datetime64(o)
    return o


def consistent_name(names):
    """ New name for series in elementwise operation

    If all truthy names are the same, choose that one, otherwise, choose None
    """
    allnames = set()
    for name in names:
        if name is None:
            continue
        if isinstance(name, (tuple, list)):
            allnames.update(name)
        else:
            allnames.add(name)

    if len(allnames) == 1:
        return first(allnames)
    else:
        return None


def elemwise(op, *args, **kwargs):
    """ Elementwise operation for dask.Dataframes """
    columns = kwargs.get('columns', None)
    name = kwargs.get('name', None)

    token = (op,
             [arg._name if isinstance(arg, _Frame) else arg for arg in args],
             sorted(kwargs.items(), key=lambda kv: str(kv[0])))

    _name = 'elemwise-' + tokenize(token)

    dfs = [arg for arg in args if isinstance(arg, _Frame)]
    other = [(i, arg) for i, arg in enumerate(args)
                      if not isinstance(arg, _Frame)]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    if not all(df.divisions == dfs[0].divisions for df in dfs):
        msg = 'All dask.Dataframe and dask.Series must have same divisions'
        raise ValueError(msg)
    if not all(df.npartitions == dfs[0].npartitions for df in dfs):
        msg = 'All dask.Dataframe and dask.Series must have same npartitions'
        raise ValueError(msg)

    dsk = dict(((_name, i), (op2,) + frs)
                for i, frs in enumerate(zip(*[df._keys() for df in dfs])))
    if columns is not None:
        return DataFrame(merge(dsk, *[df.dask for df in dfs]),
                         _name, columns, dfs[0].divisions)
    else:
        column_name = name or consistent_name(n for df in dfs
                                              for n in df.columns)
        return Series(merge(dsk, *[df.dask for df in dfs]),
                      _name, column_name, dfs[0].divisions)


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


def reduction(x, chunk, aggregate, token='reduction'):
    """ General version of reductions

    >>> reduction(my_frame, np.sum, np.sum)  # doctest: +SKIP
    """
    token_seed = (x._name, (token or (chunk, aggregate)))
    token_key = tokenize(token_seed)
    a = '{0}--chunk-{1}'.format(token, token_key)
    dsk = dict(((a, i), (empty_safe, chunk, (x._name, i)))
                for i in range(x.npartitions))

    b = '{0}--aggregation-{1}'.format(token, token_key)
    dsk2 = {(b, 0): (aggregate, (remove_empties,
                        [(a,i) for i in range(x.npartitions)]))}

    return Scalar(merge(x.dask, dsk, dsk2), b)


def concat(dfs):
    """ Concatenate dataframes along rows

    Currently only supports unknown divisions
    """
    if any(df.known_divisions for df in dfs):
        # For this to work we need to add a final division for "maximum element"
        raise NotImplementedError("Concat can't currently handle dataframes"
                " with known divisions")
    name = 'concat-' + tokenize('--'.join(df._name for df in dfs))
    dsk = dict()
    i = 0
    for df in dfs:
        for key in df._keys():
            dsk[(name, i)] = key
            i += 1

    divisions = [None] * (i + 1)

    return DataFrame(merge(dsk, *[df.dask for df in dfs]), name,
                     dfs[0].columns, divisions)


def _groupby_apply(df, ind, func):
    return df.groupby(ind).apply(func)

def _groupby_apply_level0(df, func):
    return df.groupby(level=0).apply(func)

def _groupby_getitem_apply(df, ind, key, func):
    return df.groupby(ind)[key].apply(func)

def _groupby_level0_getitem_apply(df, key, func):
    return df.groupby(level=0)[key].apply(func)


class _GroupBy(object):

    def _aca_agg(self, token, func, aggfunc=None):
        if aggfunc is None:
            aggfunc = func

        def chunk(df, index, func=func, key=self.key):
            return func(df.groupby(index)[key])

        agg = lambda df: aggfunc(df.groupby(level=0))
        token = self._token_prefix + token

        return aca([self.df, self.index], chunk=chunk, aggregate=agg,
                   columns=self.key, token=token)

    def sum(self):
        return self._aca_agg(token='sum', func=lambda x: x.sum())

    def min(self):
        return self._aca_agg(token='min', func=lambda x: x.min())

    def max(self):
        return self._aca_agg(token='max', func=lambda x: x.max())

    def count(self):
        return self._aca_agg(token='count', func=lambda x: x.count(),
                             aggfunc=lambda x: x.sum())

    def mean(self):
        return 1.0 * self.sum() / self.count()


class GroupBy(_GroupBy):

    _token_prefix = 'dataframe-groupby-'

    def __init__(self, df, index=None, **kwargs):
        self.df = df
        self.index = index
        self.kwargs = kwargs

        if isinstance(index, list):
            assert all(i in df.columns for i in index)
            self.key = [c for c in df.columns if c not in index]

        elif isinstance(index, Series):
            assert index.divisions == df.divisions
            # check whether given Series is taken from given df and unchanged.
            # If any operations are performed, _name will be changed to
            # e.g. "elemwise-xxxx"
            if (index.name is not None and
                index._name == self.df._name + '.' + index.name):
                self.key = [c for c in df.columns if c != index.name]
            else:
                self.key = list(df.columns)
        else:
            assert index in df.columns
            self.key = [c for c in df.columns if c != index]


    def apply(self, func, columns=None):
        if (isinstance(self.index, Series) and
            self.index._name == self.df.index._name):
            df = self.df
            return map_partitions(_groupby_apply_level0,
                                  columns or self.df.columns,
                                  self.df, func)
        else:
            # df = set_index(self.df, self.index, **self.kwargs)
            df = shuffle(self.df, self.index, **self.kwargs)
            return map_partitions(_groupby_apply,
                                  columns or self.df.columns,
                                  self.df, self.index, func)

    def __getitem__(self, key):
        if key in self.df.columns:
            return SeriesGroupBy(self.df, self.index, key)
        else:
            raise KeyError()

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      list(self.df.columns)))

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            try:
                return self[key]
            except KeyError:
                raise AttributeError()


class SeriesGroupBy(_GroupBy):

    _token_prefix = 'series-groupby-'

    def __init__(self, df, index, key, **kwargs):
        self.df = df
        self.index = index
        self.key = key
        self.kwargs = kwargs

    def apply(func, columns=None):
        # df = set_index(self.df, self.index, **self.kwargs)
        if self.index._name == self.df.index._name:
            df = self.df
            return map_partitions(_groupby_level0_getitem_apply,
                                  self.df, self.key, func,
                                  columns=columns)
        else:
            df = shuffle(self.df, self.index, **self.kwargs)
            return map_partitions(_groupby_apply,
                                  columns or self.df.columns,
                                  self.df, self.index, func)

    def nunique(self):
        def chunk(df, index):
            # we call set_index here to force a possibly duplicate index
            # for our reduce step
            grouped = (df.groupby(index)
                .apply(pd.DataFrame.drop_duplicates, subset=self.key))
            grouped.index = grouped.index.get_level_values(level=0)
            return grouped

        def agg(df):
            return df.groupby(level=0)[self.key].nunique()

        return aca([self.df, self.index],
                   chunk=chunk, aggregate=agg, columns=self.key,
                   token='series-groupby-nunique')


def apply_concat_apply(args, chunk=None, aggregate=None,
                       columns=no_default, token='apply-concat-apply'):
    """ Apply a function to blocks, the concat, then apply again

    Parameters
    ----------

    args: dask.DataFrames
        All Dataframes should be partitioned and indexed equivalently
    chunk: function [block-per-arg] -> block
        Function to operate on each block of data
    aggregate: function concatenated-block -> block
        Function to operate on the concatenated result of chunk

    >>> def chunk(a_block, b_block):
    ...     pass

    >>> def agg(df):
    ...     pass

    >>> apply_concat_apply([a, b], chunk=chunk, aggregate=agg)  # doctest: +SKIP
    """
    if not isinstance(args, (tuple, list)):
        args = [args]

    assert all(arg.npartitions == args[0].npartitions
                for arg in args
                if isinstance(arg, _Frame))

    token_seed = ([arg._name if isinstance(arg, _Frame) else arg
                   for arg in args], (token or (chunk, aggregate)), columns)
    token_key = tokenize(token_seed)

    a = '{0}--first-{1}'.format(token, token_key)
    dsk = dict(((a, i), (apply, chunk, (list, [(x._name, i)
                                                if isinstance(x, _Frame)
                                                else x for x in args])))
                for i in range(args[0].npartitions))

    b = '{0}--second-{1}'.format(token, token_key)
    dsk2 = {(b, 0): (aggregate,
                      (_concat,
                        (list, [(a, i) for i in range(args[0].npartitions)])))}

    if columns == no_default:
        return_type = type(args[0])
        columns = None
    else:
        if isinstance(args[0], Index):
            return_type = Index
        else:
            return_type = _get_return_type(columns)

    dasks = [a.dask for a in args if isinstance(a, _Frame)]
    return return_type(merge(dsk, dsk2, *dasks), b, columns, [None, None])


aca = apply_concat_apply


def _get_return_type(columns):
    if (isinstance(columns, (str, unicode)) or not
        isinstance(columns, Iterable)):
        return Series
    else:
        return DataFrame


def map_partitions(func, columns, *args, **kwargs):
    """ Apply Python function on each DataFrame block

    column_info: tuple or string
        Column names or name of the output
    targets: list
        List of target DataFrame / Series.
    """
    assert all(not isinstance(arg, _Frame) or
               arg.divisions == args[0].divisions for arg in args)

    token = kwargs.pop('token', None)

    if kwargs:
        raise ValueError("Keyword arguments not yet supported in map_partitions")

    return_type = _get_return_type(columns)

    token = ((token or func),
             columns,
             [arg._name if isinstance(arg, _Frame) else arg for arg in args])

    name = 'map-partitions' + tokenize(token)
    dsk = dict(((name, i), (apply, func,
                             (tuple, [(arg._name, i)
                                      if isinstance(arg, _Frame)
                                      else arg
                                      for arg in args])))
                for i in range(args[0].npartitions))

    dasks = [arg.dask for arg in args if isinstance(arg, _Frame)]
    return return_type(merge(dsk, *dasks), name, columns, args[0].divisions)


def categorize_block(df, categories):
    """ Categorize a dataframe with given categories

    df: DataFrame
    categories: dict mapping column name to iterable of categories
    """
    df = df.copy()
    for col, vals in categories.items():
        df[col] = pd.Categorical(df[col], categories=vals, ordered=False)
    return df


def categorize(df, columns=None, **kwargs):
    """
    Convert columns of dataframe to category dtype

    This aids performance, both in-memory and in spilling to disk
    """
    if columns is None:
        dtypes = df.dtypes
        columns = [name for name, dt in zip(dtypes.index, dtypes.values)
                    if dt == 'O']
    if not isinstance(columns, (list, tuple)):
        columns = [columns]

    distincts = [df[col].drop_duplicates() for col in columns]
    values = compute(*distincts, **kwargs)

    func = partial(categorize_block, categories=dict(zip(columns, values)))
    return df.map_partitions(func, columns=df.columns)


def quantile(df, q):
    """ Approximate quantiles of column

    Parameters
    ----------
    q : list/array of floats
        Iterable of numbers ranging from 0 to 100 for the desired quantiles
    """
    assert len(df.columns) == 1
    if not len(q):
        return da.zeros((0,), chunks=((0,),))
    from dask.array.percentile import _percentile, merge_percentiles
    token = tokenize((df._name, q))
    name = 'quantiles-1-' + token
    val_dsk = dict(((name, i), (_percentile, (getattr, key, 'values'), q))
                   for i, key in enumerate(df._keys()))
    name2 = 'quantiles-2-' + token
    len_dsk = dict(((name2, i), (len, key)) for i, key in enumerate(df._keys()))

    name3 = 'quantiles-3-' + token
    merge_dsk = {(name3, 0): (pd.Series, (merge_percentiles, q, [q] * df.npartitions,
                                                sorted(val_dsk),
                                                sorted(len_dsk)))}

    dsk = merge(df.dask, val_dsk, len_dsk, merge_dsk)
    return Series(dsk, name3, df.name, [df.divisions[0], df.divisions[-1]])


def pd_split(df, p, seed=0):
    """ Split DataFrame into multiple pieces pseudorandomly

    >>> df = pd.DataFrame({'a': [1, 2, 3, 4, 5, 6],
    ...                    'b': [2, 3, 4, 5, 6, 7]})

    >>> a, b = pd_split(df, [0.5, 0.5], seed=123)  # roughly 50/50 split
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
    index = pseudorandom(len(df), p, seed)
    return [df.iloc[index == i] for i in range(len(p))]


def repartition_divisions(a, b, name, out1, out2):
    """ dask graph to repartition dataframe by new divisions

    Parameters
    ----------
    a: tuple
        old divisions
    b: tuple
        new divisions
    name: str
        name of old dataframe
    out1: str
        name of temporary splits
    out2: str
        name of new dataframe

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
    assert a[0] == b[0]
    assert a[-1] == b[-1]
    c = [a[0]]
    d = dict()
    low = a[0]
    i, j = 1, 1
    k = 0
    while (i < len(a) and j < len(b)):
        if a[i] < b[j]:
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
        k = k + 1
    tup = d[(out1, k - 1)]
    d[(out1, k - 1)] = tup[:-1] + (True,)
    c.append(a[-1])

    i, j = 0, 1
    while j < len(b):
        tmp = []
        while c[i] < b[j]:
            tmp.append((out1, i))
            i += 1
        if len(tmp) == 1:
            d[(out2, j - 1)] = tmp[0]
        else:
            d[(out2, j - 1)] = (pd.concat, (list, tmp))

        j += 1

    return d


def repartition(df, divisions):
    """ Repartition dataframe along new divisions

    Dask.DataFrame objects are partitioned along their index.  Often when
    multiple dataframes interact we need to align these partitionings.  The
    ``repartition`` function constructs a new DataFrame object holding the same
    data but partitioned on different values.  It does this by performing a
    sequence of ``loc`` and ``concat`` calls to split and merge the previous
    generation of partitions.

    >>> df = df.repartition([0, 5, 10, 20])  # doctest: +SKIP

    Also works on Pandas objects

    >>> ddf = dd.repartition(df, [0, 5, 10, 20])  # doctest: +SKIP
    """
    if isinstance(df, _Frame):
        token = tokenize((df._name, divisions))
        tmp = 'repartition-split-' + token
        out = 'repartition-merge-' + token
        dsk = repartition_divisions(df.divisions, divisions, df._name, tmp, out)

        return type(df)(merge(df.dask, dsk), out, df.column_info, divisions)

    elif isinstance(df, pd.core.generic.NDFrame):
        token = tokenize((tokenize_dataframe(df), divisions))
        name = 'repartition-dataframe-' + token
        dfs = shard_df_on_index(df, divisions[1:-1])
        dsk = dict(((name, i), df) for i, df in enumerate(dfs))
        if isinstance(df, pd.DataFrame):
            return DataFrame(dsk, name, df.columns, divisions)
        if isinstance(df, pd.Series):
            return Series(dsk, name, df.name, divisions)


class DatetimeAccessor(object):
    """ Accessor object for datetimelike properties of the Series values.

    Examples
    --------

    >>> s.dt.microsecond  # doctest: +SKIP
    """
    def __init__(self, series):
        if not isinstance(series, Series):
            raise ValueError('DatetimeAccessor cannot be initialized')
        self._series = series

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      dir(pd.Series.dt)))

    def _property_map(self, key):
        return self._series.map_partitions(lambda s: getattr(s.dt, key))

    def _function_map(self, key, *args):
        func = lambda s: getattr(s.dt, key)(*args)
        return self._series.map_partitions(func, *args)

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            if key in dir(pd.Series.dt):
                if isinstance(getattr(pd.Series.dt, key), property):
                    return self._property_map(key)
                else:
                    return partial(self._function_map, key)
            else:
                raise


class StringAccessor(object):
    """ Accessor object for string properties of the Series values.

    Examples
    --------

    >>> s.str.lower()  # doctest: +SKIP
    """
    def __init__(self, series):
        if not isinstance(series, Series):
            raise ValueError('DatetimeAccessor cannot be initialized')
        self._series = series

    def __dir__(self):
        return sorted(set(dir(type(self)) + list(self.__dict__) +
                      dir(pd.Series.str)))

    def _property_map(self, key):
        return self._series.map_partitions(lambda s: getattr(s.str, key))

    def _function_map(self, key, *args):
        func = lambda s: getattr(s.str, key)(*args)
        return self._series.map_partitions(func, *args)

    def __getattr__(self, key):
        try:
            return object.__getattribute__(self, key)
        except AttributeError:
            if key in dir(pd.Series.str):
                if isinstance(getattr(pd.Series.str, key), property):
                    return self._property_map(key)
                else:
                    return partial(self._function_map, key)
            else:
                raise

from .shuffle import set_index, set_partition, shuffle
