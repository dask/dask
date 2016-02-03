""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

from collections import Iterable
from itertools import product
from operator import getitem

import dask
from dask.context import _globals
import dask.array as da
import dask.bag as db
import dask.dataframe as dd
from dask.base import tokenize
import numpy as np
from tornado import gen
from toolz import compose

from .executor import default_executor, Future
from .utils import sync, ignoring


def index_min(df):
    return df.index.min()

def index_max(df):
    return df.index.max()

def get_columns(df):
    return df.columns

@gen.coroutine
def _futures_to_dask_dataframe(futures, divisions=None, executor=None):
    executor = default_executor(executor)
    columns = executor.submit(get_columns, futures[0])
    if divisions is True:
        divisions = executor.map(index_min, futures)
        divisions.append(executor.submit(index_max, futures[-1]))
        divisions2 = yield executor._gather(divisions)
        if sorted(divisions2) != divisions2:
            divisions2 = [None] * (len(futures) + 1)
    elif divisions in (None, False):
        divisions2 = [None] * (len(futures) + 1)
    else:
        raise NotImplementedError()
    columns2 = yield columns._result()

    name = 'distributed-pandas-to-dask-' + tokenize(*futures)
    dsk = {(name, i): f for i, f in enumerate(futures)}

    if _globals['get'] != executor.get:
        print("Setting global dask scheduler to use distributed")
        dask.set_options(get=executor.get)

    raise gen.Return(dd.DataFrame(dsk, name, columns2, divisions2))


def futures_to_dask_dataframe(futures, divisions=None, executor=None):
    """ Convert a list of futures into a dask.dataframe

    Parameters
    ----------
    executor: Executor
        Executor through which we access the remote dataframes
    futures: iterable of Futures
        Futures that create dataframes to form the divisions
    divisions: bool
        Set to True if the data is cleanly partitioned along the index
    """
    executor = default_executor(executor)
    return sync(executor.loop, _futures_to_dask_dataframe, futures,
                divisions=divisions, executor=executor)

def get_dim(x, i):
    return x.shape[i]

def get_dtype(x):
    return x.dtype

@gen.coroutine
def _futures_to_dask_array(futures, executor=None):
    executor = default_executor(executor)
    futures = np.array(futures, dtype=object)

    slices = [((0,) * i + (slice(None, None),) + (0,) * (futures.ndim - i - 1))
              for i in range(futures.ndim)]
    chunks = [[executor.submit(get_dim, x, i) for x in futures[slc]]
              for i, slc in enumerate(slices)]
    dtype = executor.submit(get_dtype, futures.flat[0])

    chunks, dtype = yield executor._gather([chunks, dtype])
    chunks = tuple(map(tuple, chunks))

    name = 'array-from-futures-' + tokenize(*futures.flat)
    keys = list(product([name], *map(range, futures.shape)))
    values = list(futures.flat)
    dsk = dict(zip(keys, values))

    if _globals['get'] != executor.get:
        print("Setting global dask scheduler to use distributed")
        dask.set_options(get=executor.get)

    raise gen.Return(da.Array(dsk, name, chunks, dtype))


def futures_to_dask_array(futures, executor=None):
    """ Convert a nested list of futures into a dask.array

    The futures must satisfy the following:
    *  Evaluate to numpy ndarrays
    *  Be in a nested list of the same depth as the intended dimension
    *  Serve as individual chunks of the full dask array
    *  Have shapes consistent with how dask.array sets up chunks
       (all dimensions consistent across any particular direction)

    This function will block until all futures are accessible.  It queries the
    futures directly for shape and dtype information.

    Parameters
    ----------
    futures: iterable of Futures
        Futures that create arrays
    executor: Executor (optional)
        Executor through which we access the remote dataframes
    """
    executor = default_executor(executor)
    return sync(executor.loop, _futures_to_dask_array, futures,
                executor=executor)


@gen.coroutine
def _stack(futures, axis=0, executor=None):
    executor = default_executor(executor)
    assert isinstance(futures, (list, tuple))
    assert all(isinstance(f, Future) for f in futures)  # flat list

    shapes = executor.map(lambda x: x.shape, futures[:10])
    dtype = executor.submit(get_dtype, futures[0])

    shapes, dtype = yield executor._gather([shapes, dtype])

    shape = shapes[0]
    assert all(shape == s for s in shapes)

    slc = ((slice(None),) * axis
         + (None,)
         + (slice(None),) * (len(shape) - axis))

    chunks = (tuple((shape[i],) for i in range(axis))
            + ((1,) * len(futures),)
            + tuple((shape[i],) for i in range(axis, len(shape))))

    name = 'stack-futures' + tokenize(*futures)
    keys = list(product([name], *[range(len(c)) for c in chunks]))
    dsk = {k: (getitem, f, slc) for k, f in zip(keys, futures)}

    raise gen.Return(da.Array(dsk, name, chunks, dtype))


def stack(futures, axis=0, executor=None):
    executor = default_executor(executor)
    return sync(executor.loop, _stack, futures, executor=executor)


@gen.coroutine
def _futures_to_dask_bag(futures, executor=None):
    executor = default_executor(executor)

    name = 'bag-from-futures-' + tokenize(*futures)
    dsk = {(name, i): future for i, future in enumerate(futures)}

    if _globals['get'] != executor.get:
        print("Setting global dask scheduler to use distributed")
        dask.set_options(get=executor.get)

    raise gen.Return(db.Bag(dsk, name, len(futures)))


def futures_to_dask_bag(futures, executor=None):
    """ Convert a list of futures into a dask.bag

    The futures should be point to Python iterables

    Parameters
    ----------
    futures: iterable of Futures
    executor: Executor (optional)
    """
    executor = default_executor(executor)
    return sync(executor.loop, _futures_to_dask_bag, futures,
                executor=executor)

@gen.coroutine
def _futures_to_collection(futures, executor=None, **kwargs):
    executor = default_executor(executor)
    element = futures
    while not isinstance(element, Future):
        element = element[0]

    typ = yield executor.submit(type, element)._result()
    if 'pandas' in typ.__module__:
        func = _futures_to_dask_dataframe
    elif 'numpy' in typ.__module__:
        func = _futures_to_dask_array
    elif issubclass(typ, (tuple, list, set, frozenset)):
        func = _futures_to_dask_bag
    else:
        raise NotImplementedError("First future of type %s.  Expected "
                "numpy or pandas object" % typ.__name__)

    result = yield func(futures, executor=executor, **kwargs)
    raise gen.Return(result)


def futures_to_collection(futures, executor=None, **kwargs):
    """ Convert futures into a dask collection

    Convert a list of futures pointing to a pandas dataframe into one logical
    dask.dataframe or a nested list of numpy arrays into a logical dask.array.

    The type of the collection will be chosen based on the type of the futures.

    See also
    --------
    futures_to_dask_array
    futures_to_dask_dataframe
    """
    executor = default_executor(executor)
    return sync(executor.loop, _futures_to_collection, futures,
                executor=executor, **kwargs)
