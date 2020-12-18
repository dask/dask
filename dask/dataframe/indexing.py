import bisect
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd

from ..array.core import Array
from ..base import tokenize
from ..highlevelgraph import HighLevelGraph
from . import concat, from_dask_array, methods
from ._compat import PANDAS_GT_130
from .core import Index, Series, new_dd_object
from .utils import is_index_like, meta_nonempty


class _IndexerBase:
    def __init__(self, obj):
        self.obj = obj

    @property
    def _name(self):
        return self.obj._name

    @property
    def _meta_indexer(self):
        raise NotImplementedError

    def _make_meta(self, iindexer, cindexer):
        """
        get metadata
        """
        if cindexer is None:
            return self.obj
        else:
            return self._meta_indexer[:, cindexer]


class _iLocIndexer(_IndexerBase):
    @property
    def _meta_indexer(self):
        return self.obj._meta.iloc

    def __getitem__(self, key):
        if isinstance(key, int):
            key = slice(key, key + 1)

        if isinstance(key, (slice, range, list, np.ndarray, Index, Series, Array)):
            key = tuple([key])

        if not isinstance(key, tuple):
            raise ValueError("Expected slice or tuple or Dask Index, got %s" % str(key))

        obj = self.obj

        if len(key) == 0:
            return obj
        elif len(key) == 1:
            iindexer = key[0]
            cindexer = slice(None)
        elif len(key) == 2:
            if isinstance(obj, Series):
                raise ValueError("Can't slice Series on 2 axes: %s" % str(key))
            iindexer, cindexer = key
        else:
            raise pd.core.indexing.IndexingError(
                "Expected tuple of length ≤2: %s" % str(key)
            )

        partition_sizes = obj.partition_sizes
        if isinstance(iindexer, Series):
            # TODO: something like this might work for applicable cases:
            # if self.divisions == key.divisions:  # and self.known_divisions:
            #     name = "index-%s" % tokenize(self.obj, iindexer)
            #     import operator
            #     dsk = partitionwise_graph(operator.getitem, name, self.obj, iindexer)
            #     graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj, iindexer])
            #     return dd.DataFrame(
            #         graph,name,
            #         self.obj._meta,
            #         self.obj.divisions,
            #         partition_sizes=self.obj.partition_sizes,
            #     )
            # else:
            if iindexer.dtype == np.dtype(int):
                raise NotImplementedError("TODO: slicing _Frame with Series of ints")

            elif iindexer.dtype == np.dtype(bool):
                return obj.loc[iindexer, cindexer]
            else:
                raise Exception(
                    f"Expected a series to be of int or bool, not {iindexer.dtype}!"
                )
        elif isinstance(iindexer, Array):
            if len(iindexer.shape) != 1:
                raise ValueError(
                    "Can only slice %s's with 1-d Arrays, not %d (shape: %s)"
                    % (type(self), len(iindexer.shape), str(iindexer.shape))
                )
            iindexer = from_dask_array(iindexer, index=obj.index)
            if obj.ndim == 1:
                assert cindexer == slice(None, None, None)
                return obj.loc[iindexer]
            else:
                return obj.loc[iindexer, cindexer]
        elif isinstance(iindexer, slice) and iindexer == slice(None):
            if cindexer == slice(None):
                return obj
            from dask.dataframe import DataFrame

            if not isinstance(obj, DataFrame):
                raise NotImplementedError(
                    "'DataFrame.iloc' with unknown partition sizes not supported. "
                    "`partition_sizes` must be computed/set ahead of time, or propagated from an "
                    "upstream DataFrame or Series, in order to call `iloc`."
                )

            if not obj.columns.is_unique:
                # if there are any duplicate column names, do an iloc
                return self._iloc(iindexer, cindexer)
            else:
                # otherwise dispatch to dask.dataframe.core.DataFrame.__getitem__
                col_names = obj.columns[cindexer]
                return obj.__getitem__(col_names)
        else:
            # slice, list, pd.Series, ndarray
            if not partition_sizes:
                cls = obj.__class__.__name__
                raise NotImplementedError(
                    "%s.iloc only supported for %s with known partition_sizes"
                    % (cls, cls)
                )

            _len = obj._len

            if isinstance(iindexer, (slice, range)):
                start, stop, step = (
                    (iindexer.start or 0),
                    (iindexer.stop if iindexer.stop is not None else _len),
                    (iindexer.step or 1),
                )
                if isinstance(iindexer, slice):
                    # normalize negative indices
                    if start < 0:
                        start += _len
                    if stop < 0:
                        stop += _len
                    typ = slice
                elif isinstance(iindexer, range):
                    if start < 0:
                        if stop > 0 and step > 0:
                            negatives = obj.iloc[range(start, 0, step)]
                            first_positive_idx = step - ((-start) % step or step)
                            positives = obj.iloc[range(first_positive_idx, stop, step)]
                            return concat([negatives, positives])
                        else:
                            start += _len
                            stop += _len
                    elif stop < 0:
                        if start < 0:
                            start += _len
                            stop += _len
                        else:
                            if stop < -1 and step < 0:
                                # these will both end up empty if step is positive, otherwise this partitions the
                                # non-negative indices from the negatives
                                positives = obj.iloc[range(start, -1, step)]
                                first_negative_idx = start % abs(step) + step
                                negatives = obj.iloc[
                                    range(first_negative_idx, stop, step)
                                ]
                                return concat([positives, negatives])
                    typ = range

                name = "iloc-%s" % tokenize(obj, key)
                dsk = {}

                if step > 0 and stop <= start or step < 0 and start <= stop:
                    # return empty DataFrame
                    pass
                else:
                    partition_idx_ranges = obj.partition_idx_ranges
                    npartitions = obj.npartitions

                    def intersect(lo, hi, start, stop, step):
                        if step > 0:
                            if stop <= lo or start >= hi:
                                return None
                            if start < lo:
                                start = lo + step - ((lo - start) % step or step)
                            stop = min(hi, stop)
                        else:
                            hi -= 1
                            if start < lo or stop >= hi:
                                return None
                            if start > hi:
                                start = (
                                    hi + step + ((start - hi) % abs(step) or abs(step))
                                )
                            stop = max(lo - 1, stop)

                        start -= lo
                        stop -= lo

                        return typ(start, stop, step), (stop - start) // step

                    if isinstance(iindexer, range):
                        max_idx = start if start > stop else stop - step
                        if max_idx >= _len:
                            raise IndexError(
                                "Out of bounds: %d >= %d (key %s)"
                                % (max_idx, _len, str(key))
                            )
                    partition_slices = [
                        intersect(lo, hi, start, stop, step)
                        for lo, hi in partition_idx_ranges
                    ]

                    start_partition_idx = 0
                    while (
                        start_partition_idx < npartitions
                        and partition_slices[start_partition_idx] is None
                    ):
                        start_partition_idx += 1

                    end_partition_idx = npartitions
                    while (
                        end_partition_idx > start_partition_idx
                        and partition_slices[end_partition_idx - 1] is None
                    ):
                        end_partition_idx -= 1

                    new_npartitions = end_partition_idx - start_partition_idx
                    partition_slices = partition_slices[
                        start_partition_idx:end_partition_idx
                    ]
                    new_divisions = obj.divisions[
                        start_partition_idx : end_partition_idx + 1
                    ]
                    assert not any(slc is None for slc in partition_slices)

                    def iloc(df, slc):
                        return df.iloc[slc]

                    new_partition_sizes = []
                    for partition_idx, slc in enumerate(partition_slices):
                        original_partition_idx = partition_idx + start_partition_idx
                        if step > 0:
                            new_partition_idx = partition_idx
                        else:
                            new_partition_idx = new_npartitions - 1 - partition_idx
                        if slc == slice(None, None, None):
                            dsk[(name, new_partition_idx)] = (
                                lambda x: x,
                                (obj._name, original_partition_idx),
                            )
                            new_partition_sizes.append(
                                partition_sizes[original_partition_idx]
                            )
                        else:
                            slc, size = slc
                            dsk[(name, new_partition_idx)] = (
                                iloc,
                                (obj._name, original_partition_idx),
                                slc,
                            )
                            new_partition_sizes.append(size)

                    if step < 0:
                        new_partition_sizes = tuple(reversed(new_partition_sizes))
                        new_divisions = tuple(reversed(new_divisions))

                if not dsk:
                    new_divisions = (None, None)
                    new_partition_sizes = (0,)
                    dsk = {(name, 0): obj._meta}

                meta = obj._meta
                graph = HighLevelGraph.from_collections(name, dsk, dependencies=[obj])
                row_sliced = new_dd_object(
                    graph,
                    name,
                    meta=meta,
                    divisions=new_divisions,
                    partition_sizes=new_partition_sizes,
                )
                if cindexer == slice(None):
                    return row_sliced
                else:
                    return row_sliced.iloc[:, cindexer]
            elif isinstance(iindexer, (list, pd.Series, np.ndarray)):
                # unify iindexer to be an ndarray
                if isinstance(iindexer, list):
                    iindexer = np.array(iindexer)
                elif isinstance(iindexer, pd.Series):
                    iindexer = iindexer.values
                else:
                    if len(iindexer.shape) != 1:
                        raise RuntimeError(
                            "Can only slice %s's with ndarrays of rank 1" % type(self)
                        )

                if iindexer.dtype == np.dtype(int):
                    iindexer = list(
                        sorted(idx + _len if idx < 0 else idx for idx in iindexer)
                    )
                    all_partition_idxs = {}
                    cur_partition_idxs = []
                    idx_pos = 0
                    num_idxs = len(iindexer)
                    partition_ends = np.cumsum(partition_sizes).tolist()
                    npartitions = len(partition_ends)
                    partition_idx = 0
                    cur_partition_end = partition_ends[0]
                    while idx_pos < num_idxs:
                        idx = iindexer[idx_pos]
                        while idx >= cur_partition_end:
                            if cur_partition_idxs:
                                all_partition_idxs[partition_idx] = cur_partition_idxs
                                cur_partition_idxs = []
                            partition_idx += 1
                            if partition_idx == npartitions:
                                break
                            cur_partition_end = partition_ends[partition_idx]
                        cur_partition_idxs.append(idx)
                        idx_pos += 1
                    if cur_partition_idxs:
                        all_partition_idxs[partition_idx] = cur_partition_idxs

                    name = "iloc-%s" % tokenize(obj, key)

                    # TODO: is this layer of indirection necessary?
                    def iloc(d, idxs):
                        return d.iloc[idxs]

                    dsk = {
                        (name, idx): (iloc, (obj._name, partition_idx), idxs)
                        for idx, (partition_idx, idxs) in enumerate(
                            all_partition_idxs.items()
                        )
                    }
                    meta = obj._meta
                    graph = HighLevelGraph.from_collections(
                        name, dsk, dependencies=[obj]
                    )
                    row_sliced = new_dd_object(
                        graph,
                        name,
                        meta=meta,
                        divisions=[None] * (len(all_partition_idxs) + 1),
                        partition_sizes=[
                            len(idxs) for idxs in all_partition_idxs.values()
                        ],
                    )
                    # TODO: fold cindexer into the transform above
                    if cindexer == slice(None):
                        return row_sliced
                    else:
                        return row_sliced.iloc[:, cindexer]
                elif iindexer.dtype == np.dtype(bool):
                    partition_bounds = obj.partition_idx_ranges
                    name = "iloc-%s" % tokenize(obj, key)

                    # TODO: is this layer of indirection necessary?
                    def iloc(d, idxs):
                        return d.iloc[idxs]

                    all_partition_idxs = {}
                    new_partition_sizes = []
                    for idx, (start, end) in enumerate(partition_bounds):
                        idxs = iindexer[start:end]
                        size = idxs.sum()
                        all_partition_idxs[idx] = idxs
                        new_partition_sizes.append(size)

                    dsk = {
                        (name, idx): (iloc, (obj._name, idx), idxs)
                        for idx, idxs in all_partition_idxs.items()
                    }
                    meta = obj._meta
                    graph = HighLevelGraph.from_collections(
                        name, dsk, dependencies=[obj]
                    )
                    row_sliced = new_dd_object(
                        graph,
                        name,
                        meta=meta,
                        divisions=[None] * (obj.npartitions + 1),
                        partition_sizes=new_partition_sizes,
                    )
                    # TODO: fold cindexer into the transform above
                    if cindexer == slice(None):
                        return row_sliced
                    else:
                        return row_sliced.iloc[:, cindexer]
                else:
                    raise ValueError(
                        "%s dtype must be int or bool, got %s: %s"
                        % (type(iindexer).__name__, iindexer.dtype, iindexer)
                    )
            else:
                raise ValueError("Unexpected iindexer: %s" % str(iindexer))

    def _iloc(self, iindexer, cindexer):
        assert iindexer == slice(None)
        meta = self._make_meta(iindexer, cindexer)

        return self.obj.map_partitions(
            methods.iloc, cindexer, meta=meta, preserve_partition_sizes=True
        )


class _LocIndexer(_IndexerBase):
    """ Helper class for the .loc accessor """

    @property
    def _meta_indexer(self):
        return self.obj._meta.loc

    def __getitem__(self, key):

        if isinstance(key, tuple):
            # multi-dimensional selection
            if len(key) > self.obj.ndim:
                # raise from pandas
                msg = "Too many indexers"
                raise pd.core.indexing.IndexingError(msg)

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
        elif isinstance(iindexer, Array):
            return self._loc_array(iindexer, cindexer)
        elif callable(iindexer):
            return self._loc(iindexer(self.obj), cindexer)

        if self.obj.known_divisions:
            iindexer = self._maybe_partial_time_string(iindexer)

            if isinstance(iindexer, slice):
                return self._loc_slice(iindexer, cindexer)
            elif isinstance(iindexer, (list, np.ndarray)):
                return self._loc_list(iindexer, cindexer)
            else:
                # element should raise KeyError
                return self._loc_element(iindexer, cindexer)
        else:
            if isinstance(iindexer, (list, np.ndarray)):
                # applying map_partitions to each partition
                # results in duplicated NaN rows
                msg = "Cannot index with list against unknown division"
                raise KeyError(msg)
            elif not isinstance(iindexer, slice):
                iindexer = slice(iindexer, iindexer)

            meta = self._make_meta(iindexer, cindexer)
            return self.obj.map_partitions(
                methods.try_loc,
                iindexer,
                cindexer,
                meta=meta,  # TODO: partition_sizes
            )

    def _maybe_partial_time_string(self, iindexer):
        """
        Convert index-indexer for partial time string slicing
        if obj.index is DatetimeIndex / PeriodIndex
        """
        idx = meta_nonempty(self.obj._meta.index)
        iindexer = _maybe_partial_time_string(idx, iindexer)
        return iindexer

    def _loc_series(self, iindexer, cindexer):
        meta = self._make_meta(iindexer, cindexer)
        return self.obj.map_partitions(
            methods.loc,
            iindexer,
            cindexer,
            token="loc-series",
            meta=meta,  # TODO: partition_sizes
        )

    def _loc_array(self, iindexer, cindexer):
        iindexer_series = iindexer.to_dask_dataframe("_", self.obj.index)
        return self._loc_series(iindexer_series, cindexer)

    def _loc_list(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, self.obj)
        parts = self._get_partitions(iindexer)
        meta = self._make_meta(iindexer, cindexer)

        if len(iindexer):
            dsk = {}
            divisions = []
            items = sorted(parts.items())
            for i, (div, indexer) in enumerate(items):
                dsk[name, i] = (methods.loc, (self._name, div), indexer, cindexer)
                # append minimum value as division
                divisions.append(sorted(indexer)[0])
            # append maximum value of the last division
            divisions.append(sorted(items[-1][1])[-1])
            graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        else:
            divisions = [None, None]
            dsk = {(name, 0): meta.head(0)}
            graph = HighLevelGraph.from_collections(name, dsk)
        return new_dd_object(graph, name, meta=meta, divisions=divisions)

    def _loc_element(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, self.obj)
        part = self._get_partitions(iindexer)

        if iindexer < self.obj.divisions[0] or iindexer > self.obj.divisions[-1]:
            raise KeyError("the label [%s] is not in the index" % str(iindexer))

        dsk = {
            (name, 0): (
                methods.loc,
                (self._name, part),
                slice(iindexer, iindexer),
                cindexer,
            )
        }

        meta = self._make_meta(iindexer, cindexer)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        # TODO: partition_sizes presumably [1], unless index values can repeat?
        return new_dd_object(
            graph, name, meta=meta, divisions=[iindexer, iindexer], partition_sizes=None
        )

    def _get_partitions(self, keys):
        if isinstance(keys, (list, np.ndarray)):
            return _partitions_of_index_values(self.obj.divisions, keys)
        else:
            # element
            return _partition_of_index_value(self.obj.divisions, keys)

    def _coerce_loc_index(self, key):
        return _coerce_loc_index(self.obj.divisions, key)

    def _loc_slice(self, iindexer, cindexer):
        name = "loc-%s" % tokenize(iindexer, cindexer, self)

        assert isinstance(iindexer, slice)
        assert iindexer.step in (None, 1)

        if iindexer.start is not None:
            start = self._get_partitions(iindexer.start)
        else:
            start = 0
        if iindexer.stop is not None:
            stop = self._get_partitions(iindexer.stop)
        else:
            stop = self.obj.npartitions - 1

        if iindexer.start is None and self.obj.known_divisions:
            istart = self.obj.divisions[0]
        else:
            istart = self._coerce_loc_index(iindexer.start)
        if iindexer.stop is None and self.obj.known_divisions:
            istop = self.obj.divisions[-1]
        else:
            istop = self._coerce_loc_index(iindexer.stop)

        if stop == start:
            dsk = {
                (name, 0): (
                    methods.loc,
                    (self._name, start),
                    slice(iindexer.start, iindexer.stop),
                    cindexer,
                )
            }
            divisions = [istart, istop]
        else:
            dsk = {
                (name, 0): (
                    methods.loc,
                    (self._name, start),
                    slice(iindexer.start, None),
                    cindexer,
                )
            }
            for i in range(1, stop - start):
                if cindexer is None:
                    dsk[name, i] = (self._name, start + i)
                else:
                    dsk[name, i] = (
                        methods.loc,
                        (self._name, start + i),
                        slice(None, None),
                        cindexer,
                    )

            dsk[name, stop - start] = (
                methods.loc,
                (self._name, stop),
                slice(None, iindexer.stop),
                cindexer,
            )

            if iindexer.start is None:
                div_start = self.obj.divisions[0]
            else:
                div_start = max(istart, self.obj.divisions[start])

            if iindexer.stop is None:
                div_stop = self.obj.divisions[-1]
            else:
                div_stop = min(istop, self.obj.divisions[stop + 1])

            divisions = (
                (div_start,) + self.obj.divisions[start + 1 : stop + 1] + (div_stop,)
            )

        assert len(divisions) == len(dsk) + 1

        meta = self._make_meta(iindexer, cindexer)
        graph = HighLevelGraph.from_collections(name, dsk, dependencies=[self.obj])
        # TODO: could at least figure out partition_sizes for ":" all-rows iindexer, and non-boundary partition_sizes in
        # general (leaving Nones in self.partition_sizes on the ends?)
        return new_dd_object(graph, name, meta=meta, divisions=divisions)


def _partition_of_index_value(divisions, val):
    """In which partition does this value lie?

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


def _partitions_of_index_values(divisions, values):
    """Return defaultdict of division and values pairs
    Each key corresponds to the division which values are index values belong
    to the division.

    >>> sorted(_partitions_of_index_values([0, 5, 10], [3]).items())
    [(0, [3])]
    >>> sorted(_partitions_of_index_values([0, 5, 10], [3, 8, 5]).items())
    [(0, [3]), (1, [8, 5])]
    """
    if divisions[0] is None:
        msg = "Can not use loc on DataFrame without known divisions"
        raise ValueError(msg)

    results = defaultdict(list)
    values = pd.Index(values, dtype=object)
    for val in values:
        i = bisect.bisect_right(divisions, val)
        div = min(len(divisions) - 2, max(0, i - 1))
        results[div].append(val)
    return results


def _coerce_loc_index(divisions, o):
    """Transform values to be comparable against divisions

    This is particularly valuable to use with pandas datetimes
    """
    if divisions and isinstance(divisions[0], datetime):
        return pd.Timestamp(o)
    if divisions and isinstance(divisions[0], np.datetime64):
        return np.datetime64(o).astype(divisions[0].dtype)
    return o


def _maybe_partial_time_string(index, indexer):
    """
    Convert indexer for partial string selection
    if data has DatetimeIndex/PeriodIndex
    """
    # do not pass dd.Index
    assert is_index_like(index)

    if not isinstance(index, (pd.DatetimeIndex, pd.PeriodIndex)):
        return indexer

    if PANDAS_GT_130:
        kind_option = {}
    else:
        kind_option = {"kind": "loc"}

    if isinstance(indexer, slice):
        if isinstance(indexer.start, str):
            start = index._maybe_cast_slice_bound(indexer.start, "left", **kind_option)
        else:
            start = indexer.start

        if isinstance(indexer.stop, str):
            stop = index._maybe_cast_slice_bound(indexer.stop, "right", **kind_option)
        else:
            stop = indexer.stop
        return slice(start, stop)

    elif isinstance(indexer, str):
        start = index._maybe_cast_slice_bound(indexer, "left", **kind_option)
        stop = index._maybe_cast_slice_bound(indexer, "right", **kind_option)
        return slice(min(start, stop), max(start, stop))

    return indexer
