from __future__ import division

import bisect
from collections import Iterable, Iterator
from datetime import datetime
from distutils.version import LooseVersion
from functools import wraps
import operator
from operator import getitem, setitem
from pprint import pformat
import uuid

from toolz import merge, partial, first, partition, unique
import pandas as pd
import numpy as np

try:
    from chest import Chest as Cache
except ImportError:
    Cache = dict

from .. import array as da
from .. import core
from ..array.core import partial_by_order
from .. import threaded
from ..compatibility import unicode, apply, operator_div, bind_method
from ..utils import repr_long_list, IndexCallable, pseudorandom
from .utils import shard_df_on_index
from ..base import Base, compute, tokenize, normalize_token

no_default = '__no_default__'
return_scalar = '__return_scalar__'

pd.computation.expressions.set_use_numexpr(False)

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

    def __array__(self):
        # array interface is required to support pandas instance + Scalar
        # Otherwise, above op results in pd.Series of Scalar (object dtype)
        return np.asarray(self.compute())

    @property
    def _args(self):
        return (self.dask, self._name)

    def _keys(self):
        return [(self._name, 0)]

    @classmethod
    def _get_unary_operator(cls, op):
        def f(self):
            name = tokenize(self)
            dsk = {(name, 0): (op, (self._name, 0))}
            return Scalar(merge(dsk, self.dask), name)
        return f

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        return lambda self, other: _scalar_binary(op, self, other, inv=inv)


def _scalar_binary(op, a, b, inv=False):
    name = '{0}-{1}'.format(op.__name__, tokenize(a, b))

    dsk = a.dask
    if not isinstance(b, Base):
        pass
    elif isinstance(b, Scalar):
        dsk = merge(dsk, b.dask)
        b = (b._name, 0)
    else:
        return NotImplemented

    if inv:
        dsk.update({(name, 0): (op, b, (a._name, 0))})
    else:
        dsk.update({(name, 0): (op, (a._name, 0), b)})

    if isinstance(b, pd.Series):
        return Series(dsk, name, b.name, [b.index.min(), b.index.max()])
    elif isinstance(b, pd.DataFrame):
        return DataFrame(dsk, name, b.columns, [b.index.min(), b.index.max()])
    else:
        return Scalar(dsk, name)


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
        """Return number of partitions"""
        return len(self.divisions) - 1

    def _keys(self):
        return [(self._name, i) for i in range(self.npartitions)]

    @property
    def index(self):
        """Return dask Index instance"""
        name = self._name + '-index'
        dsk = dict(((name, i), (getattr, key, 'index'))
                   for i, key in enumerate(self._keys()))
        return Index(merge(dsk, self.dask), name, None, self.divisions)

    @property
    def known_divisions(self):
        """Whether divisions are already known"""
        return len(self.divisions) > 0 and self.divisions[0] is not None

    def get_division(self, n):
        """ Get nth division of the data """
        if 0 <= n < self.npartitions:
            name = 'get-division-%s-%s' % (str(n), self._name)
            dsk = {(name, 0): (self._name, n)}
            divisions = self.divisions[n:n+2]
            return self._constructor(merge(self.dask, dsk), name,
                                     self.column_info, divisions)
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
        return self._constructor(dsk2, name, self.column_info, self.divisions)

    @wraps(pd.DataFrame.drop_duplicates)
    def drop_duplicates(self):
        chunk = lambda s: s.drop_duplicates()
        return aca(self, chunk=chunk, aggregate=chunk, columns=self.column_info,
                   token='drop-duplicates')

    def __len__(self):
        return reduction(self, len, np.sum, token='len').compute()

    def map_partitions(self, func, columns=no_default, *args, **kwargs):
        """ Apply Python function on each DataFrame block

        When using ``map_partitions`` you should provide either the column
        names (if the result is a DataFrame) or the name of the Series (if the
        result is a Series).  The output type will be determined by the type of
        ``columns``.

        >>> df.map_partitions(lambda df: df.x + 1, columns='x')  # doctest: +SKIP

        >>> df.map_partitions(lambda df: df.head(), columns=df.columns)  # doctest: +SKIP

        Parameters
        ----------

        func: function
            Function applied to each blocks
        columns: tuple or scalar
            Column names or name of the output. Defaults to names of data itself.
            When tuple is passed, DataFrame is returned. When scalar is passed,
            Series is returned.
        """
        if columns == no_default:
            columns = self.column_info
        return map_partitions(func, columns, self, *args, **kwargs)

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
        """ Purely label-location based indexer for selection by label.

        >>> df.loc["b"]  # doctest: +SKIP
        >>> df.loc["b":"d"]  # doctest: +SKIP"""
        return IndexCallable(self._loc)

    @property
    def iloc(self):
        """ Not implemented """

        # not implemented because of performance concerns.
        # see https://github.com/blaze/dask/pull/507
        raise AttributeError("Dask Dataframe does not support iloc")

    def repartition(self, divisions, force=False):
        """ Repartition dataframe along new divisions

        Parameters
        ----------

        divisions: list
            List of partitions to be used
        force: bool, default False
            Allows the expansion of the existing divisions.
            If False then the new divisions lower and upper bounds must be
            the same as the old divisions.

        >>> df = df.repartition([0, 5, 10, 20])  # doctest: +SKIP
        """
        return repartition(self, divisions, force=force)

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

        name = 'sample-' + tokenize(self, frac, random_state)
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

    @wraps(pd.DataFrame.to_csv)
    def to_csv(self, filename, **kwargs):
        from .io import to_csv
        return to_csv(self, filename, **kwargs)

    @property
    def _elemwise_cols(self):
        """passed to elemwise ops, None for Series, columns for DataFrame"""
        return None

    @classmethod
    def _get_unary_operator(cls, op):
        return lambda self: elemwise(op, self, columns=self._elemwise_cols)

    @classmethod
    def _get_binary_operator(cls, op, inv=False):
        if inv:
            return lambda self, other: elemwise(op, other, self,
                                                columns=self._elemwise_cols)
        else:
            return lambda self, other: elemwise(op, self, other,
                                                columns=self._elemwise_cols)

    def _aca_agg(self, token, func, aggfunc=None):
        """ Wrapper for aggregations """
        raise NotImplementedError

    @wraps(pd.DataFrame.sum)
    def sum(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.sum(axis=1)
            name = '{0}sum(axis=1)'.format(self._token_prefix)
            return map_partitions(f, None, self, token=name)
        else:
            return self._aca_agg(token='sum', func=lambda x: x.sum())

    @wraps(pd.DataFrame.max)
    def max(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.max(axis=1)
            name = '{0}max(axis=1)'.format(self._token_prefix)
            return map_partitions(f, None, self, token=name)
        else:
            return self._aca_agg(token='max', func=lambda x: x.max())

    @wraps(pd.DataFrame.min)
    def min(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.min(axis=1)
            name = '{0}min(axis=1)'.format(self._token_prefix)
            return map_partitions(f, None, self, token=name)
        else:
            return self._aca_agg(token='min', func=lambda x: x.min())

    @wraps(pd.DataFrame.count)
    def count(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.count(axis=1)
            name = '{0}count(axis=1)'.format(self._token_prefix)
            return map_partitions(f, None, self, token=name)
        else:
            return self._aca_agg(token='count', func=lambda x: x.count(),
                                 aggfunc=lambda x: x.sum())

    @wraps(pd.DataFrame.mean)
    def mean(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x: x.mean(axis=1)
            name = '{0}mean(axis=1)'.format(self._token_prefix)
            return map_partitions(f, None, self, token=name)
        else:
            num = self._get_numeric_data()
            s = num.sum()
            n = num.count()

            def f(s, n):
                try:
                    return s / n
                except ZeroDivisionError:
                    return np.nan
            name = '{0}mean-{1}'.format(self._token_prefix, tokenize(s))
            return map_partitions(f, no_default, s, n, token=name)

    @wraps(pd.DataFrame.var)
    def var(self, axis=None, ddof=1):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x, ddof=ddof: x.var(axis=1, ddof=ddof)
            name = '{0}var(axis=1, ddof={1})'.format(self._token_prefix, ddof)
            return map_partitions(f, None, self, token=name)
        else:
            num = self._get_numeric_data()
            x = 1.0 * num.sum()
            x2 = 1.0 * (num ** 2).sum()
            n = num.count()

            def f(x2, x, n):
                try:
                    result = (x2 / n) - (x / n)**2
                    if ddof:
                        result = result * n / (n - ddof)
                    return result
                except ZeroDivisionError:
                    return np.nan
            name = '{0}var(ddof={1})'.format(self._token_prefix, ddof)
            return map_partitions(f, no_default, x2, x, n, token=name)

    @wraps(pd.DataFrame.std)
    def std(self, axis=None, ddof=1):
        axis = self._validate_axis(axis)
        if axis == 1:
            f = lambda x, ddof=ddof: x.std(axis=1, ddof=ddof)
            name = '{0}std(axis=1, ddof={1})'.format(self._token_prefix, ddof)
            return map_partitions(f, None, self, token=name)
        else:
            v = self.var(ddof=ddof)
            name = '{0}std(ddof={1})'.format(self._token_prefix, ddof)
            return map_partitions(np.sqrt, no_default, v, token=name)

    def _cum_agg(self, token, chunk, aggregate, agginit):
        """ Wrapper for cumulative operation """
        # cumulate each partitions
        name1 = '{0}{1}-map'.format(self._token_prefix, token)
        cumpart = map_partitions(chunk, self.column_info, self, token=name1)
        # take last element of each cumulated partitions
        name2 = '{0}{1}-take-last'.format(self._token_prefix, token)
        cumlast = map_partitions(lambda x: x.iloc[-1],
                                 self.column_info, cumpart, token=name2)

        name = '{0}{1}'.format(self._token_prefix, token)
        cname = '{0}{1}-cum-last'.format(self._token_prefix, token)

        # aggregate cumulated partisions and its previous last element
        dask = {}
        if isinstance(self, DataFrame):
            agginit = pd.Series(agginit, index=self.column_info)
        dask[(cname, 0)] = agginit
        dask[(name, 0)] = (cumpart._name, 0)
        for i in range(1, self.npartitions):
            # store each cumulative step to graph to reduce computation
            dask[(cname, i)] = (aggregate, (cname, i - 1),
                                (cumlast._name, i - 1))
            dask[(name, i)] = (aggregate, (cumpart._name, i), (cname, i))
        return self._constructor(merge(dask, cumpart.dask, cumlast.dask),
                                 name, self.column_info, self.divisions)

    @wraps(pd.DataFrame.cumsum)
    def cumsum(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            name = '{0}cumsum(axis=1)'.format(self._token_prefix)
            return map_partitions(self._partition_type.cumsum,
                                  self.column_info, self, 1, token=name)
        else:
            return self._cum_agg('cumsum', self._partition_type.cumsum,
                                 operator.add, 0)

    @wraps(pd.DataFrame.cumprod)
    def cumprod(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            name = '{0}cumprod(axis=1)'.format(self._token_prefix)
            return map_partitions(self._partition_type.cumprod,
                                  self.column_info, self, 1, token=name)
        else:
           return self._cum_agg('cumprod', self._partition_type.cumprod,
                                 operator.mul, 1)

    @wraps(pd.DataFrame.cummax)
    def cummax(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            name = '{0}cummax(axis=1)'.format(self._token_prefix)
            return map_partitions(self._partition_type.cummax,
                                  self.column_info, self, 1, token=name)
        else:
            def aggregate(x, y):
                if isinstance(x, (pd.Series, pd.DataFrame)):
                    return x.where(x > y, y, axis=x.ndim - 1)
                else:       # scalsr
                    return x if x > y else y
            return self._cum_agg('cummax', self._partition_type.cummax,
                                 aggregate, np.nan)

    @wraps(pd.DataFrame.cummin)
    def cummin(self, axis=None):
        axis = self._validate_axis(axis)
        if axis == 1:
            name = '{0}cummin(axis=1)'.format(self._token_prefix)
            return map_partitions(self._partition_type.cummin,
                                  self.column_info, self, 1, token=name)
        else:
            def aggregate(x, y):
                if isinstance(x, (pd.Series, pd.DataFrame)):
                    return x.where(x < y, y, axis=x.ndim - 1)
                else:       # scalar
                    return x if x < y else y
            return self._cum_agg('cummin', self._partition_type.cummin,
                                 aggregate, np.nan)

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """
        raise NotImplementedError


normalize_token.register((Scalar, _Frame), lambda a: a._name)


class Series(_Frame):
    """ Out-of-core Series object

    Mimics ``pandas.Series``.

    See Also
    --------

    dask.dataframe.DataFrame
    """

    _partition_type = pd.Series
    _token_prefix = 'series-'

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
        """ Return dimensionality """
        return 1

    @property
    def dtype(self):
        """ Return data type """
        return self.head().dtype

    @property
    def column_info(self):
        """ Return Series.name """
        return self.name

    @property
    def columns(self):
        """ Return 1 element tuple containing the name """
        return (self.name,)

    @property
    def nbytes(self):
        return reduction(self, lambda s: s.nbytes, np.sum, token='nbytes')

    def __repr__(self):
        return ("dd.%s<%s, divisions=%s>" %
                (self.__class__.__name__, self._name,
                 repr_long_list(self.divisions)))

    def __array__(self, dtype=None, **kwargs):
        x = np.array(self.compute())
        if dtype and x.dtype != dtype:
            x = x.astype(dtype)
        return x

    def __array_wrap__(self, array, context=None):
        return pd.Series(array, name=self.name)

    def quantile(self, q):
        """ Approximate quantiles of column

        q : list/array of floats
            Iterable of numbers ranging from 0 to 1 for the desired quantiles
        """
        return quantile(self, q)

    def resample(self, rule, how='mean', axis=0, fill_method=None, closed=None,
                 label=None, convention='start', kind=None, loffset=None,
                 limit=None, base=0):
        """Group by a DatetimeIndex values in time periods of size `rule`.

        Parameters
        ----------
        rule : str or pandas.datetools.Tick
            The frequency to resample by. For example, 'H' is one hour
            intervals.
        how : str or callable
            Method to use to summarize your data. For example, 'mean' takes the
            average value of the Series in the time interval `rule`.

        Notes
        -----
        For additional argument descriptions please consult the pandas
        documentation.

        Returns
        -------
        dask.dataframe.Series

        See Also
        --------
        pandas.Series.resample
        """

        divs = pd.Series(range(len(self.divisions)), index=self.divisions)
        temp = divs.resample(rule, how='count', axis=axis, fill_method=fill_method,
                          closed=closed, label=label, convention=convention,
                          kind=kind, loffset=loffset, limit=limit, base=base)
        newdivs = temp.loc[temp > 0].index.tolist()
        if newdivs[-1] < self.divisions[-1]:
            newdivs.append(self.divisions[-1])
        if newdivs[0] > self.divisions[0]:
            newdivs.insert(0, self.divisions[0])

        day_nanos = pd.datetools.Day().nanos

        rule = pd.datetools.to_offset(rule)
        def block_func(df):
            if getattr(rule, 'nanos', None) and day_nanos % rule.nanos:
                raise NotImplementedError('Resampling frequency %s that does'
                                          ' not evenly divide a day is not '
                                          'implemented' % rule)

            return df.resample(rule=rule, how=how, axis=axis,
                               fill_method=fill_method, closed=closed,
                               label=label, convention=convention, kind=kind,
                               loffset=loffset, limit=limit, base=base)

        return self.repartition(newdivs, force=True).map_partitions(block_func)

    def __getitem__(self, key):
        if isinstance(key, Series) and self.divisions == key.divisions:
            name = 'series-index-%s[%s]' % (self._name, key._name)
            dsk = dict(((name, i), (operator.getitem, (self._name, i),
                                                       (key._name, i)))
                        for i in range(self.npartitions))
            return Series(merge(self.dask, key.dask, dsk), name,
                          self.name, self.divisions)
        raise NotImplementedError()

    @wraps(pd.DataFrame._get_numeric_data)
    def _get_numeric_data(self, how='any', subset=None):
        return self

    def _validate_axis(self, axis=0):
        if axis not in (0, 'index', None):
            raise ValueError('No axis named {0}'.format(axis))
        # convert to numeric axis
        return {None: 0, 'index': 0}.get(axis, axis)

    def _aca_agg(self, token, func, aggfunc=None):
        """ Wrapper for aggregations """
        if aggfunc is None:
            aggfunc = func

        return aca([self], chunk=func,
                   aggregate=lambda x: aggfunc(pd.Series(x)),
                   columns=return_scalar, token=self._token_prefix + token)

    @wraps(pd.Series.groupby)
    def groupby(self, index, **kwargs):
        return SeriesGroupBy(self, index, **kwargs)

    @wraps(pd.Series.sum)
    def sum(self, axis=None):
        return super(Series, self).sum(axis=axis)

    @wraps(pd.Series.max)
    def max(self, axis=None):
        return super(Series, self).max(axis=axis)

    @wraps(pd.Series.min)
    def min(self, axis=None):
        return super(Series, self).min(axis=axis)

    @wraps(pd.Series.count)
    def count(self):
        return super(Series, self).count()

    @wraps(pd.Series.mean)
    def mean(self, axis=None):
        return super(Series, self).mean(axis=axis)

    @wraps(pd.Series.var)
    def var(self, axis=None, ddof=1):
        return super(Series, self).var(axis=axis, ddof=ddof)

    @wraps(pd.Series.std)
    def std(self, axis=None, ddof=1):
        return super(Series, self).std(axis=axis, ddof=ddof)

    @wraps(pd.Series.cumsum)
    def cumsum(self, axis=None):
        return super(Series, self).cumsum(axis=axis)

    @wraps(pd.Series.cumprod)
    def cumprod(self, axis=None):
        return super(Series, self).cumprod(axis=axis)

    @wraps(pd.Series.cummax)
    def cummax(self, axis=None):
        return super(Series, self).cummax(axis=axis)

    @wraps(pd.Series.cummin)
    def cummin(self, axis=None):
        return super(Series, self).cummin(axis=axis)

    @wraps(pd.Series.nunique)
    def nunique(self):
        return self.drop_duplicates().count()

    @wraps(pd.Series.value_counts)
    def value_counts(self):
        chunk = lambda s: s.value_counts()
        if LooseVersion(pd.__version__) > '0.16.2':
            agg = lambda s: s.groupby(level=0).sum().sort_values(ascending=False)
        else:
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

    @wraps(pd.Series.to_frame)
    def to_frame(self, name=None):
        _name = name if name is not None else self.name
        return map_partitions(pd.Series.to_frame, [_name], self, name)

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """

        def meth(self, other, level=None, fill_value=None, axis=0):
            if not level is None:
                raise NotImplementedError('level must be None')
            return map_partitions(op, self.column_info, self, other,
                                  axis=axis, fill_value=fill_value)
        meth.__doc__ = op.__doc__
        bind_method(cls, name, meth)

    def apply(self, func, convert_dtype=True, name=no_default, args=(), **kwds):
        """ Parallel version of pandas.Series.apply """
        if name is no_default:
            name = self.name
        return map_partitions(pd.Series.apply, name, self, func,
                              convert_dtype, args, **kwds)


class Index(Series):

    _token_prefix = 'index-'

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
        return reduction(self, f, np.sum, token='index-count')


class DataFrame(_Frame):
    """
    Implements out-of-core DataFrame as a sequence of pandas DataFrames

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
    _token_prefix = 'dataframe-'

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
            dsk = dict(((name, i), (self._partition_type._getitem_array,
                                     (self._name, i),
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
        """ Return dimensionality """
        return 2

    @property
    def dtypes(self):
        """ Return data types """
        return self._get(self.dask, self._keys()[0]).dtypes

    @wraps(pd.DataFrame.set_index)
    def set_index(self, other, **kwargs):
        from .shuffle import set_index
        return set_index(self, other, **kwargs)

    def set_partition(self, column, divisions, **kwargs):
        """ Set explicit divisions for new column index

        >>> df2 = df.set_partition('new-index-column', divisions=[10, 20, 50])  # doctest: +SKIP

        See also:
            set_index
        """
        from .shuffle import set_partition
        return set_partition(self, column, divisions, **kwargs)

    @property
    def column_info(self):
        """ Return DataFrame.columns """
        return self.columns

    @wraps(pd.DataFrame.groupby)
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

    @wraps(pd.DataFrame.rename)
    def rename(self, index=None, columns=None):
        if index is not None:
            raise ValueError("Cannot rename index.")
        column_info = (pd.DataFrame(columns=self.column_info)
                         .rename(columns=columns).columns)
        func = pd.DataFrame.rename
        # *args here is index, columns but columns arg is already used
        return map_partitions(func, column_info, self, None, columns)

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
            name = name + '--' + tokenize(kwargs)
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

    def to_castra(self, fn=None, categories=None, sorted_index_column=None,
                  compute=True):
        """ Write DataFrame to Castra on-disk store

        See https://github.com/blosc/castra for details

        See Also:
            Castra.to_dask
        """
        from .io import to_castra
        return to_castra(self, fn, categories, sorted_index_column,
                         compute=compute)

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

    @wraps(pd.DataFrame._get_numeric_data)
    def _get_numeric_data(self, how='any', subset=None):
        return self.map_partitions(pd.DataFrame._get_numeric_data)

    def _validate_axis(self, axis=0):
        if axis not in (0, 1, 'index', 'columns', None):
            raise ValueError('No axis named {0}'.format(axis))
        # convert to numeric axis
        return {None: 0, 'index': 0, 'columns': 1}.get(axis, axis)

    def _aca_agg(self, token, func, aggfunc=None):
        """ Wrapper for aggregations """
        if aggfunc is None:
            aggfunc = func

        return aca([self], chunk=func,
                   aggregate=lambda x: aggfunc(x.groupby(level=0)),
                   columns=None, token=self._token_prefix + token)

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

    @wraps(pd.DataFrame.merge)
    def merge(self, right, how='inner', on=None, left_on=None, right_on=None,
              left_index=False, right_index=False,
              suffixes=('_x', '_y'), npartitions=None):

        if not isinstance(right, (DataFrame, pd.DataFrame)):
            raise ValueError('right must be DataFrame')

        from .multi import merge
        return merge(self, right, how=how, on=on,
                     left_on=left_on, right_on=right_on,
                     left_index=left_index, right_index=right_index,
                     suffixes=suffixes, npartitions=npartitions)

    @wraps(pd.DataFrame.join)
    def join(self, other, on=None, how='left',
             lsuffix='', rsuffix='', npartitions=None):

        if not isinstance(other, (DataFrame, pd.DataFrame)):
            raise ValueError('other must be DataFrame')

        from .multi import merge
        return merge(self, other, how=how,
                     left_index=on is None, right_index=True,
                     left_on=on, suffixes=[lsuffix, rsuffix],
                     npartitions=npartitions)

    @classmethod
    def _bind_operator_method(cls, name, op):
        """ bind operator method like DataFrame.add to this class """

        # name must be explicitly passed for div method whose name is truediv

        def meth(self, other, axis='columns', level=None, fill_value=None):
            if level is not None:
                raise NotImplementedError('level must be None')

            axis = self._validate_axis(axis)

            right = None
            if axis == 1:
                # when axis=1, series will be added to each row
                # it not supported for dd.Series.
                # dd.DataFrame is not affected as op is applied elemwise
                if isinstance(other, Series):
                    msg = 'Unable to {0} dd.Series with axis=1'.format(name)
                    raise ValueError(msg)
                elif isinstance(other, pd.Series):
                    right = other.index
            if isinstance(other, (DataFrame, pd.DataFrame)):
                right = other.columns

            if right is not None:
                left = pd.DataFrame(columns=self.columns)
                right = pd.DataFrame(columns=right)
                columns = op(left, right, axis=axis).columns.tolist()
            else:
                columns = self.columns

            return map_partitions(op, columns, self, other,
                                  axis=axis, fill_value=fill_value)
        meth.__doc__ = op.__doc__
        bind_method(cls, name, meth)

    def apply(self, func, axis=0, args=(), columns=no_default, **kwds):
        """ Parallel version of pandas.DataFrame.apply

        This mimics the pandas version except for the following:

        1.  The user must specify axis=0 explicitly
        2.  The user must provide output columns or column
        """
        if axis == 0:
            raise NotImplementedError(
                    "dd.DataFrame.apply only supports axis=1\n"
                    "  Try: df.apply(func, axis=1)")

        if columns is no_default:
            raise ValueError(
            "Please supply column names of output dataframe or series\n"
            "  Before: df.apply(func)\n"
            "  After:  df.apply(func, columns=['x', 'y']) for dataframe result\n"
            "  or:     df.apply(func, columns='x')        for series result")

        return map_partitions(pd.DataFrame.apply, columns, self, func, axis,
                              False, False, None, args, **kwds)


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
    result = df.loc[start:stop]
    if not include_right_boundary:
        right_index = result.index.get_slice_bound(stop, 'left',
                                                   result.index.inferred_type)
        result = result.iloc[:right_index]
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

    _name = 'elemwise-' + tokenize(op, kwargs, *args)

    from .io import from_pandas
    args = [from_pandas(arg, 1) if isinstance(arg, (pd.DataFrame, pd.Series))
            else arg for arg in args]

    from .multi import _maybe_align_partitions
    args = _maybe_align_partitions(args)
    dasks = [arg for arg in args if isinstance(arg, (_Frame, Scalar))]
    dfs = [df for df in dasks if isinstance(df, _Frame)]
    divisions = dfs[0].divisions
    n = len(divisions) - 1

    other = [(i, arg) for i, arg in enumerate(args)
             if not isinstance(arg, (_Frame, Scalar))]

    if other:
        op2 = partial_by_order(op, other)
    else:
        op2 = op

    # adjust the key length of Scalar
    keys = [d._keys() *  n if isinstance(d, Scalar)
            else d._keys() for d in dasks]

    dsk = dict(((_name, i), (op2,) + frs) for i, frs in enumerate(zip(*keys)))
    dsk = merge(dsk, *[d.dask for d in dasks])

    if columns is not None:
        return DataFrame(dsk, _name, columns, divisions)
    else:
        column_name = name or consistent_name(n for df in dfs
                                              for n in df.columns)
        return Series(dsk, _name, column_name, divisions)


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


def reduction(x, chunk, aggregate, token=None):
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
                        [(a,i) for i in range(x.npartitions)]))}

    return Scalar(merge(x.dask, dsk, dsk2), b)


def _concat_dfs(dfs, name):
    """ Internal function to concat dask dict and DataFrame.columns """
    dsk = dict()
    i = 0
    for df in dfs:
        for key in df._keys():
            dsk[(name, i)] = key
            i += 1

    empties = [pd.DataFrame(columns=df.columns) for df in dfs]
    columns = pd.concat(empties, axis=0, join='outer').columns

    return dsk, columns


def concat(dfs, interleave_partitions=False):
    """ Concatenate DataFrames along rows.

    - If all divisions are known and ordered, concatenate DataFrames keeping
      divisions. When divisions are not ordered, specifying
      interleave_partition=True allows concatenate divisions each by each.
    - If any of division is unknown, concatenate DataFrames resetting its
      division to unknown (None)

    Parameters
    ----------

    dfs: list
        List of dask.DataFrames to be concatenated
    interleave_partitions: bool, default False
        Whether to concatenate DataFrames ignoring its order. If True, every
        divisions are concatenated each by each.

    Examples
    --------

    # If all divisions are known and ordered, divisions are kept.
    >>> a                                               # doctest: +SKIP
    dd.DataFrame<x, divisions=(1, 3, 5)>
    >>> b                                               # doctest: +SKIP
    dd.DataFrame<y, divisions=(6, 8, 10)>
    >>> dd.concat([a, b])                               # doctest: +SKIP
    dd.DataFrame<concat-..., divisions=(1, 3, 6, 8, 10)>

    # Unable to concatenate if divisions are not ordered.
    >>> a                                               # doctest: +SKIP
    dd.DataFrame<x, divisions=(1, 3, 5)>
    >>> b                                               # doctest: +SKIP
    dd.DataFrame<y, divisions=(2, 3, 6)>
    >>> dd.concat([a, b])                               # doctest: +SKIP
    ValueError: All inputs have known divisions which cannnot be concatenated
    in order. Specify interleave_partitions=True to ignore order

    # Specify interleave_partitions=True to ignore the division order.
    >>> dd.concat([a, b], interleave_partitions=True)   # doctest: +SKIP
    dd.DataFrame<concat-..., divisions=(1, 2, 3, 5, 6)>

    # If any of division is unknown, the result division will be unknown
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

    if all(df.known_divisions for df in dfs):
        # each DataFrame's division must be greater than previous one
        if all(dfs[i].divisions[-1] < dfs[i + 1].divisions[0]
               for i in range(len(dfs) - 1)):
            name = 'concat-{0}'.format(tokenize(*dfs))
            dsk, columns = _concat_dfs(dfs, name)

            divisions = []
            for df in dfs[:-1]:
                # remove last to concatenate with next
                divisions += df.divisions[:-1]
            divisions += dfs[-1].divisions

            return DataFrame(merge(dsk, *[df.dask for df in dfs]), name,
                             columns, divisions)
        else:
            if interleave_partitions:
                from .multi import concat_indexed_dataframes
                return concat_indexed_dataframes(dfs)

            raise ValueError('All inputs have known divisions which cannnot be '
                             'concatenated in order. Specify '
                             'interleave_partitions=True to ignore order')

    else:
        name = 'concat-{0}'.format(tokenize(*dfs))
        dsk, columns = _concat_dfs(dfs, name)

        divisions = [None] * (len(dsk) + 1)

        return DataFrame(merge(dsk, *[df.dask for df in dfs]), name,
                         columns, divisions)


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

        if isinstance(self.index, Series):

            def chunk(df, index, func=func, key=self.key):
                if isinstance(df, pd.Series):
                    return func(df.groupby(index))
                else:
                    return func(df.groupby(index)[key])

            agg = lambda df: aggfunc(df.groupby(level=0))
            token = self._token_prefix + token

            return aca([self.df, self.index], chunk=chunk, aggregate=agg,
                       columns=self.key, token=token)
        else:
            def chunk(df, index=self.index, func=func, key=self.key):
                return func(df.groupby(index)[key])

            if isinstance(self.index, list):
                levels = list(range(len(self.index)))
            else:
                levels = 0
            agg = lambda df: aggfunc(df.groupby(level=levels))
            token = self._token_prefix + token

            return aca(self.df, chunk=chunk, aggregate=agg,
                       columns=self.key, token=token)

    @wraps(pd.core.groupby.GroupBy.sum)
    def sum(self):
        return self._aca_agg(token='sum', func=lambda x: x.sum())

    @wraps(pd.core.groupby.GroupBy.min)
    def min(self):
        return self._aca_agg(token='min', func=lambda x: x.min())

    @wraps(pd.core.groupby.GroupBy.max)
    def max(self):
        return self._aca_agg(token='max', func=lambda x: x.max())

    @wraps(pd.core.groupby.GroupBy.count)
    def count(self):
        return self._aca_agg(token='count', func=lambda x: x.count(),
                             aggfunc=lambda x: x.sum())

    @wraps(pd.core.groupby.GroupBy.mean)
    def mean(self):
        return 1.0 * self.sum() / self.count()


class GroupBy(_GroupBy):

    _token_prefix = 'dataframe-groupby-'

    def __init__(self, df, index=None, key=None, **kwargs):
        self.df = df
        self.index = index
        self.kwargs = kwargs

        if isinstance(index, list):
            for i in index:
                if i not in df.columns:
                    raise KeyError("Columns not found: '{0}'".format(i))
            _key = [c for c in df.columns if c not in index]

        elif isinstance(index, Series):
            assert index.divisions == df.divisions
            # check whether given Series is taken from given df and unchanged.
            # If any operations are performed, _name will be changed to
            # e.g. "elemwise-xxxx"
            if (index.name is not None and
                index._name == self.df._name + '.' + index.name):
                _key = [c for c in df.columns if c != index.name]
            else:
                _key = list(df.columns)
        else:
            if index not in df.columns:
                raise KeyError("Columns not found: '{0}'".format(index))
            _key = [c for c in df.columns if c != index]

        self.key = key or _key

    def apply(self, func, columns=None):
        if (isinstance(self.index, Series) and
            self.index._name == self.df.index._name):
            df = self.df
            return map_partitions(_groupby_apply_level0,
                                  columns or self.df.columns,
                                  self.df, func)
        else:
            from .shuffle import shuffle
            # df = set_index(self.df, self.index, **self.kwargs)
            df = shuffle(self.df, self.index, **self.kwargs)
            return map_partitions(_groupby_apply,
                                  columns or self.df.columns,
                                  self.df, self.index, func)

    def __getitem__(self, key):
        if isinstance(key, list):
            for k in key:
                if k not in self.df.columns:
                    raise KeyError("Columns not found: '{0}'".format(k))
            return GroupBy(self.df, index=self.index, key=key, **self.kwargs)
        else:
            if key not in self.df.columns:
                raise KeyError("Columns not found: '{0}'".format(key))
            return SeriesGroupBy(self.df, self.index, key)

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

    def __init__(self, df, index, key=None, **kwargs):
        self.df = df
        self.index = index
        self.key = key
        self.kwargs = kwargs

        if isinstance(df, Series):
            if not isinstance(index, Series):
                raise TypeError("A dask Series must be used as the index for a"
                                " Series groupby.")
            if not df.divisions == index.divisions:
                raise NotImplementedError("The Series and index of the groupby"
                                          " must have the same divisions.")

    def apply(self, func, columns=None):
        # df = set_index(self.df, self.index, **self.kwargs)
        if self.index._name == self.df.index._name:
            df = self.df
            return map_partitions(_groupby_level0_getitem_apply,
                                  self.df, self.key, func,
                                  columns=columns)
        else:
            from .shuffle import shuffle
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
                       columns=no_default, token=None):
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

    token_key = tokenize(token or (chunk, aggregate), columns, *args)
    token = token or 'apply-concat-apply'

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
        return_type = _get_return_type(args[0], columns)

    dasks = [a.dask for a in args if isinstance(a, _Frame)]
    return return_type(merge(dsk, dsk2, *dasks), b, columns, [None, None])


aca = apply_concat_apply


def _get_return_type(arg, columns):
    if (isinstance(columns, (str, unicode)) or not
          isinstance(columns, Iterable)):

        if columns == return_scalar:
            return Scalar

        elif isinstance(arg, Index):
            return Index
        else:
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
    token = kwargs.pop('token', 'map-partitions')
    token_key = tokenize(token or func, columns, *args)
    name = '{0}-{1}'.format(token, token_key)

    if len(kwargs) > 1:
        func = partial(func, **kwargs)

    if all(isinstance(arg, Scalar) for arg in args):
        dask = {(name, 0): (func, ) + tuple((arg._name, 0) for arg in args)}
        return Scalar(merge(dask, *[arg.dask for arg in args]), name)

    from .io import from_pandas
    args = [from_pandas(arg, 1) if isinstance(arg, (pd.DataFrame, pd.Series))
            else arg for arg in args]

    if columns is no_default:
        columns = None

    from .multi import _maybe_align_partitions
    args = _maybe_align_partitions(args)
    dfs = [df for df in args if isinstance(df, _Frame)]

    return_type = _get_return_type(dfs[0], columns)
    dsk = {}
    for i in range(dfs[0].npartitions):
        values = [(arg._name, i if isinstance(arg, _Frame) else 0)
                  if isinstance(arg, (_Frame, Scalar)) else arg for arg in args]
        dsk[(name, i)] = (_rename, columns, (apply, func, (tuple, values)))

    dasks = [arg.dask for arg in args if isinstance(arg, (_Frame, Scalar))]

    return return_type(merge(dsk, *dasks), name, columns, args[0].divisions)


def _rename(columns, df):
    """ Rename columns in dataframe or series """
    if isinstance(columns, Iterator):
        columns = list(columns)
    if columns is no_default:
        return df
    if isinstance(df, pd.DataFrame) and len(columns) == len(df.columns):
        return df.rename(columns=dict(zip(df.columns, columns)))
    elif isinstance(df, pd.Series):
        return pd.Series(df, name=columns)
    else:
        return df


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
    from dask.array.percentile import _percentile, merge_percentiles

    # currently, only Series has quantile method
    if isinstance(q, (list, tuple, np.ndarray)):
        # make Series
        merge_type = lambda v: df._partition_type(v, index=q, name=df.name)
        return_type = df._constructor
        if issubclass(return_type, Index):
            return_type = Series
    else:
        merge_type = lambda v: df._partition_type(v).item()
        return_type = df._constructor_sliced
        q = [q]

    # pandas uses quantile in [0, 1]
    # numpy / everyone else uses [0, 100]
    qs = np.asarray(q) * 100
    token = tokenize(df, qs)

    if len(qs) == 0:
        name = 'quantiles-' + token
        return Series({(name, 0): pd.Series([], name=df.name)},
                      name, df.name, [None, None])
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
    return return_type(dsk, name3, df.name, new_divisions)


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


def repartition_divisions(a, b, name, out1, out2, force=False):
    """ dask graph to repartition dataframe by new divisions

    Parameters
    ----------
    a: tuple
        old divisions
    b: tuple, list
        new divisions
    name: str
        name of old dataframe
    out1: str
        name of temporary splits
    out2: str
        name of new dataframe
    force: bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.

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
    if a[-1] < b[-1]:
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
        if last_elem and c[i] == b[-1] and i < k:
            # append if last split is not included
            tmp.append((out1, i))
            i += 1
        if len(tmp) == 0:
            # dumy slice to return empty DataFrame or Series,
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


def repartition(df, divisions, force=False):
    """ Repartition dataframe along new divisions

    Dask.DataFrame objects are partitioned along their index.  Often when
    multiple dataframes interact we need to align these partitionings.  The
    ``repartition`` function constructs a new DataFrame object holding the same
    data but partitioned on different values.  It does this by performing a
    sequence of ``loc`` and ``concat`` calls to split and merge the previous
    generation of partitions.

    Parameters
    ----------

    divisions: list
        List of partitions to be used
    force: bool, default False
        Allows the expansion of the existing divisions.
        If False then the new divisions lower and upper bounds must be
        the same as the old divisions.

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
                               df.column_info, divisions)
    elif isinstance(df, pd.core.generic.NDFrame):
        name = 'repartition-dataframe-' + token
        dfs = shard_df_on_index(df, divisions[1:-1])
        dsk = dict(((name, i), df) for i, df in enumerate(dfs))
        if isinstance(df, pd.DataFrame):
            return DataFrame(dsk, name, df.columns, divisions)
        if isinstance(df, pd.Series):
            return Series(dsk, name, df.name, divisions)
    raise ValueError('Data must be DataFrame or Series')


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
