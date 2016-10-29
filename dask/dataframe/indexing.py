from __future__ import absolute_import, division, print_function

from datetime import datetime

from toolz import merge
import bisect
import numpy as np
import pandas as pd

from .core import new_dd_object, Series
from . import methods
from ..base import tokenize


class _LocIndexer(object):
    """ Helper class for the .loc accessor """

    def __init__(self, obj):
        self.obj = obj

    @property
    def _name(self):
        return self.obj._name

    def _make_meta(self, iindexer, cindexer):
        """
        get metadata
        """
        if cindexer is None:
            return self.obj
        else:
            return self.obj._meta.loc[:, cindexer]

    def __getitem__(self, key):

        if isinstance(key, tuple):
            # multi-dimensional selection
            if len(key) > self.obj.ndim:
                raise KeyError(key)

            iindexer = key[0]
            cindexer = key[1]
        else:
            # if self.obj is Series, cindexer is always None
            iindexer = key
            cindexer = None
        return self._loc(iindexer, cindexer)

    def _loc(self, iindexer, cindexer):
        """ Helper function for the .loc accessor """
        if isinstance(iindexer, Series):
            return self._loc_series(iindexer, cindexer)

        if self.obj.known_divisions:
            iindexer = self._maybe_partial_time_string(iindexer)

            if isinstance(iindexer, slice):
                return self._loc_slice(iindexer, cindexer)
            else:
                # element should raise KeyError
                return self._loc_element(iindexer, cindexer)
        else:
            if not isinstance(iindexer, slice):
                iindexer = slice(iindexer, iindexer)

            meta = self._make_meta(iindexer, cindexer)
            return self.obj.map_partitions(methods.try_loc, iindexer, cindexer,
                                           meta=meta)

    def _maybe_partial_time_string(self, iindexer):
        """
        Convert index-indexer for partial time string slicing
        if obj.index is DatetimeIndex / PeriodIndex
        """
        iindexer = _maybe_partial_time_string(self.obj._meta_nonempty.index,
                                              iindexer, kind='loc')
        return iindexer

    def _loc_series(self, iindexer, cindexer):
        meta = self._make_meta(iindexer, cindexer)
        return self.obj.map_partitions(methods.loc, iindexer, cindexer,
                                       token='loc-series', meta=meta)

    def _loc_element(self, iindexer, cindexer):

        name = 'loc-%s' % tokenize(iindexer, self.obj)

        part = self._partition_of_index_value(iindexer)

        if iindexer < self.obj.divisions[0] or iindexer > self.obj.divisions[-1]:
            raise KeyError('the label [%s] is not in the index' % str(iindexer))

        dsk = {(name, 0): (methods.loc, (self._name, part),
                           slice(iindexer, iindexer), cindexer)}

        meta = self._make_meta(iindexer, cindexer)
        return new_dd_object(merge(self.obj.dask, dsk), name,
                             meta=meta, divisions=[iindexer, iindexer])

    def _partition_of_index_value(self, key):
        return _partition_of_index_value(self.obj.divisions, key)

    def _coerce_loc_index(self, key):
        return _coerce_loc_index(self.obj.divisions, key)

    def _loc_slice(self, iindexer, cindexer):
        name = 'loc-%s' % tokenize(iindexer, cindexer, self)

        assert isinstance(iindexer, slice)
        assert iindexer.step in (None, 1)

        if iindexer.start is not None:
            start = self._partition_of_index_value(iindexer.start)
        else:
            start = 0
        if iindexer.stop is not None:
            stop = self._partition_of_index_value(iindexer.stop)
        else:
            stop = self.obj.npartitions - 1

        istart = self._coerce_loc_index(iindexer.start)
        istop = self._coerce_loc_index(iindexer.stop)

        if stop == start:
            dsk = {(name, 0): (methods.loc, (self._name, start),
                               slice(iindexer.start, iindexer.stop), cindexer)}
            divisions = [istart, istop]
        else:
            dsk = {(name, 0): (methods.loc, (self._name, start),
                               slice(iindexer.start, None), cindexer)}
            for i in range(1, stop - start):
                if cindexer is None:
                    dsk[name, i] = (self._name, start + i)
                else:
                    dsk[name, i] = (methods.loc, (self._name, start + i),
                                    slice(None, None), cindexer)

            dsk[name, stop - start] = (methods.loc, (self._name, stop),
                                       slice(None, iindexer.stop), cindexer)

            if iindexer.start is None:
                div_start = self.obj.divisions[0]
            else:
                div_start = max(istart, self.obj.divisions[start])

            if iindexer.stop is None:
                div_stop = self.obj.divisions[-1]
            else:
                div_stop = min(istop, self.obj.divisions[stop + 1])

            divisions = ((div_start, ) +
                         self.obj.divisions[start + 1:stop + 1] +
                         (div_stop, ))

        assert len(divisions) == len(dsk) + 1

        meta = self._make_meta(iindexer, cindexer)
        return new_dd_object(merge(self.obj.dask, dsk), name,
                             meta=meta, divisions=divisions)


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
        msg = "Can not use loc on DataFrame without known divisions"
        raise ValueError(msg)
    val = _coerce_loc_index(divisions, val)
    i = bisect.bisect_right(divisions, val)
    return min(len(divisions) - 2, max(0, i - 1))


def _coerce_loc_index(divisions, o):
    """ Transform values to be comparable against divisions

    This is particularly valuable to use with pandas datetimes
    """
    if divisions and isinstance(divisions[0], datetime):
        return pd.Timestamp(o)
    if divisions and isinstance(divisions[0], np.datetime64):
        return np.datetime64(o).astype(divisions[0].dtype)
    return o


def _maybe_partial_time_string(index, indexer, kind):
    """
    Convert indexer for partial string selection
    if data has DatetimeIndex/PeriodIndex
    """
    # do not pass dd.Index
    assert isinstance(index, pd.Index)

    if not isinstance(index, (pd.DatetimeIndex, pd.PeriodIndex)):
        return indexer

    if isinstance(indexer, slice):
        if isinstance(indexer.start, pd.compat.string_types):
            start = index._maybe_cast_slice_bound(indexer.start, 'left', kind)
        else:
            start = indexer.start

        if isinstance(indexer.stop, pd.compat.string_types):
            stop = index._maybe_cast_slice_bound(indexer.stop, 'right', kind)
        else:
            stop = indexer.stop
        return slice(start, stop)

    elif isinstance(indexer, pd.compat.string_types):
        start = index._maybe_cast_slice_bound(indexer, 'left', 'loc')
        stop = index._maybe_cast_slice_bound(indexer, 'right', 'loc')
        return slice(start, stop)
    return indexer
