from __future__ import absolute_import, division, print_function

from fnmatch import fnmatch
from functools import wraps
from glob import glob
import os
from threading import Lock
import multiprocessing
import uuid
from warnings import warn

import pandas as pd
import dask
from toolz import merge

from ...async import get_sync
from ...base import tokenize
from ...context import _globals
from ...delayed import Delayed, delayed
import dask.multiprocessing

from ..core import DataFrame, new_dd_object

from ...utils import build_name_function

from .io import _link

lock = Lock()


def _pd_to_hdf(pd_to_hdf, lock, args, kwargs=None):
    """ A wrapper function around pd_to_hdf that enables locking"""

    if lock:
        lock.acquire()
    try:
        pd_to_hdf(*args, **kwargs)
    finally:
        if lock:
            lock.release()

    return None


@wraps(pd.DataFrame.to_hdf)
def to_hdf(df, path_or_buf, key, mode='a', append=False, get=None,
           name_function=None, compute=True, lock=None, dask_kwargs={},
           **kwargs):
    name = 'to-hdf-' + uuid.uuid1().hex

    pd_to_hdf = getattr(df._partition_type, 'to_hdf')

    single_file = True
    single_node = True

    # if path_or_buf is string, format using i_name
    if isinstance(path_or_buf, str):
        if path_or_buf.count('*') + key.count('*') > 1:
            raise ValueError("A maximum of one asterisk is accepted in file path and dataset key")

        fmt_obj = lambda path_or_buf, i_name: path_or_buf.replace('*', i_name)

        if '*' in path_or_buf:
            single_file = False
    else:
        if key.count('*') > 1:
            raise ValueError("A maximum of one asterisk is accepted in dataset key")

        fmt_obj = lambda path_or_buf, _: path_or_buf

    if '*' in key:
        single_node = False

    if 'format' in kwargs and kwargs['format'] != 'table':
        raise ValueError("Dask only support 'table' format in hdf files.")

    if mode not in ('a', 'w', 'r+'):
        raise ValueError("Mode must be one of 'a', 'w' or 'r+'")

    if name_function is None:
        name_function = build_name_function(df.npartitions - 1)

    # we guarantee partition order is preserved when its saved and read
    # so we enforce name_function to maintain the order of its input.
    if not (single_file and single_node):
        formatted_names = [name_function(i) for i in range(df.npartitions)]
        if formatted_names != sorted(formatted_names):
            warn("To preserve order between partitions name_function "
                 "must preserve the order of its input")

    # If user did not specify scheduler and write is sequential default to the
    # sequential scheduler. otherwise let the _get method choose the scheduler
    if get is None and 'get' not in _globals and single_node and single_file:
        get = get_sync

    # handle lock default based on whether we're writing to a single entity
    _actual_get = get or _globals.get('get') or df._default_get
    if lock is None:
        if not single_node:
            lock = True
        elif not single_file and _actual_get is not dask.multiprocessing.get:
            # if we're writing to multiple files with the multiprocessing
            # scheduler we don't need to lock
            lock = True
        else:
            lock = False

    if lock is True:
        if _actual_get == dask.multiprocessing.get:
            lock = multiprocessing.Manager().Lock()
        else:
            lock = Lock()

    kwargs.update({'format': 'table', 'mode': mode, 'append': append})

    dsk = dict()

    i_name = name_function(0)
    dsk[(name, 0)] = (_pd_to_hdf, pd_to_hdf, lock,
                      [(df._name, 0), fmt_obj(path_or_buf, i_name),
                       key.replace('*', i_name)], kwargs)

    kwargs2 = kwargs.copy()
    if single_file:
        kwargs2['mode'] = 'a'
    if single_node:
        kwargs2['append'] = True

    for i in range(1, df.npartitions):
        i_name = name_function(i)
        task = (_pd_to_hdf, pd_to_hdf, lock,
                [(df._name, i), fmt_obj(path_or_buf, i_name),
                    key.replace('*', i_name)], kwargs2)
        if single_file:
            link_dep = i - 1 if single_node else 0
            task = (_link, (name, link_dep), task)
        dsk[(name, i)] = task

    dsk = merge(df.dask, dsk)
    if single_file and single_node:
        keys = [(name, df.npartitions - 1)]
    else:
        keys = [(name, i) for i in range(df.npartitions)]

    if compute:
        return DataFrame._get(dsk, keys, get=get, **dask_kwargs)
    else:
        return delayed([Delayed(k, [dsk]) for k in keys])


dont_use_fixed_error_message = """
This HDFStore is not partitionable and can only be use monolithically with
pandas.  In the future when creating HDFStores use the ``format='table'``
option to ensure that your dataset can be parallelized"""

read_hdf_error_msg = """
The start and stop keywords are not supported when reading from more than
one file/dataset.

The combination is ambiguous because it could be interpreted as the starting
and stopping index per file, or starting and stopping index of the global
dataset."""


def _read_single_hdf(path, key, start=0, stop=None, columns=None,
                     chunksize=int(1e6), sorted_index=False, lock=None,
                     mode='a'):
    """
    Read a single hdf file into a dask.dataframe. Used for each file in
    read_hdf.
    """
    def get_keys_stops_divisions(path, key, stop, sorted_index):
        """
        Get the "keys" or group identifiers which match the given key, which
        can contain wildcards. This uses the hdf file identified by the
        given path. Also get the index of the last row of data for each matched
        key.
        """
        with pd.HDFStore(path, mode=mode) as hdf:
            keys = [k for k in hdf.keys() if fnmatch(k, key)]
            stops = []
            divisions = []
            for k in keys:
                storer = hdf.get_storer(k)
                if storer.format_type != 'table':
                    raise TypeError(dont_use_fixed_error_message)
                if stop is None:
                    stops.append(storer.nrows)
                elif stop > storer.nrows:
                    raise ValueError("Stop keyword exceeds dataset number "
                                     "of rows ({})".format(storer.nrows))
                else:
                    stops.append(stop)
                if sorted_index:
                    division_start = storer.read_column('index', start=0, stop=1)[0]
                    division_end = storer.read_column('index', start=storer.nrows - 1,
                                                      stop=storer.nrows)[0]
                    divisions.append([division_start, division_end])
                else:
                    divisions.append(None)
        return keys, stops, divisions

    def one_path_one_key(path, key, start, stop, columns, chunksize, division, lock):
        """
        Get the data frame corresponding to one path and one key (which should
        not contain any wildcards).
        """
        empty = pd.read_hdf(path, key, mode=mode, stop=0)
        if columns is not None:
            empty = empty[columns]

        token = tokenize((path, os.path.getmtime(path), key, start,
                          stop, empty, chunksize, division))
        name = 'read-hdf-' + token
        if empty.ndim == 1:
            base = {'name': empty.name, 'mode': mode}
        else:
            base = {'columns': empty.columns, 'mode': mode}

        if start >= stop:
            raise ValueError("Start row number ({}) is above or equal to stop "
                             "row number ({})".format(start, stop))

        if division:
            dsk = {(name, 0): (_pd_read_hdf, path, key, lock,
                               base)}

            divisions = division
        else:
            def update(s):
                new = base.copy()
                new.update({'start': s, 'stop': s + chunksize})
                return new

            dsk = dict(((name, i), (_pd_read_hdf, path, key, lock,
                                    update(s)))
                       for i, s in enumerate(range(start, stop, chunksize)))

            divisions = [None] * (len(dsk) + 1)

        return new_dd_object(dsk, name, empty, divisions)

    keys, stops, divisions = get_keys_stops_divisions(path, key, stop, sorted_index)
    if (start != 0 or stop is not None) and len(keys) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    from ..multi import concat
    return concat([one_path_one_key(path, k, start, s, columns, chunksize, d, lock)
                   for k, s, d in zip(keys, stops, divisions)])


def _pd_read_hdf(path, key, lock, kwargs):
    """ Read from hdf5 file with a lock """
    if lock:
        lock.acquire()
    try:
        result = pd.read_hdf(path, key, **kwargs)
    finally:
        if lock:
            lock.release()
    return result


@wraps(pd.read_hdf)
def read_hdf(pattern, key, start=0, stop=None, columns=None,
             chunksize=1000000, sorted_index=False, lock=True, mode='a'):
    """
    Read hdf files into a dask dataframe. Like pandas.read_hdf, except it we
    can read multiple files, and read multiple keys from the same file by using
    pattern matching.

    Parameters
    ----------
    pattern : pattern (string), or buffer to read from. Can contain wildcards
    key : group identifier in the store. Can contain wildcards
    start : optional, integer (defaults to 0), row number to start at
    stop : optional, integer (defaults to None, the last row), row number to
        stop at
    columns : optional, a list of columns that if not None, will limit the
        return columns
    chunksize : optional, positive integer
        maximal number of rows per partition

    Returns
    -------
    dask.DataFrame

    Examples
    --------
    Load single file

    >>> dd.read_hdf('myfile.1.hdf5', '/x')  # doctest: +SKIP

    Load multiple files

    >>> dd.read_hdf('myfile.*.hdf5', '/x')  # doctest: +SKIP

    Load multiple datasets

    >>> dd.read_hdf('myfile.1.hdf5', '/*')  # doctest: +SKIP
    """
    if lock is True:
        lock = Lock()

    key = key if key.startswith('/') else '/' + key
    paths = sorted(glob(pattern))
    if (start != 0 or stop is not None) and len(paths) > 1:
        raise NotImplementedError(read_hdf_error_msg)
    if chunksize <= 0:
        raise ValueError("Chunksize must be a positive integer")
    if (start != 0 or stop is not None) and sorted_index:
        raise ValueError("When assuming pre-partitioned data, data must be "
                         "read in its entirety using the same chunksizes")
    from ..multi import concat
    return concat([_read_single_hdf(path, key, start=start, stop=stop,
                                    columns=columns, chunksize=chunksize,
                                    sorted_index=sorted_index,
                                    lock=lock, mode=mode)
                   for path in paths])
