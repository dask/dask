""" This file is experimental and may disappear without warning """
from __future__ import print_function, division, absolute_import

from itertools import product

import dask.array as da
import dask.bag as db
import dask.dataframe as dd
from dask.base import tokenize
import numpy as np
from tornado import gen

from .client import (default_client, Future, _first_completed,
        ensure_default_get)
from .utils import sync


def index_min(df):
    return df.index.min()

def index_max(df):
    return df.index.max()

def get_empty(df):
    return df.iloc[0:0]

@gen.coroutine
def _futures_to_dask_dataframe(futures, divisions=None, client=None):
    client = default_client(client)
    f = yield _first_completed(futures)
    empty = client.submit(get_empty, f)
    if divisions is True:
        divisions = client.map(index_min, futures)
        divisions.append(client.submit(index_max, futures[-1]))
        divisions2 = yield client._gather(divisions)
        if sorted(divisions2) != divisions2:
            divisions2 = [None] * (len(futures) + 1)
    elif divisions in (None, False):
        divisions2 = [None] * (len(futures) + 1)
    else:
        raise NotImplementedError()
    empty = yield empty._result()

    name = 'distributed-pandas-to-dask-' + tokenize(*futures)
    dsk = {(name, i): f for i, f in enumerate(futures)}

    ensure_default_get(client)

    raise gen.Return(dd.DataFrame(dsk, name, empty, divisions2))


def futures_to_dask_dataframe(futures, divisions=None, client=None):
    """ Convert a list of futures into a dask.dataframe

    Parameters
    ----------
    client: Client
        Client through which we access the remote dataframes
    futures: iterable of Futures
        Futures that create dataframes to form the divisions
    divisions: bool
        Set to True if the data is cleanly partitioned along the index
    """
    client = default_client(client)
    return sync(client.loop, _futures_to_dask_dataframe, futures,
                divisions=divisions, client=client)


def get_shape(x):
    return x.shape

def get_dim(x, i):
    return x.shape[i]

def get_dtype(x):
    return x.dtype

@gen.coroutine
def _futures_to_dask_array(futures, client=None):
    client = default_client(client)
    futures = np.array(futures, dtype=object)

    slices = [((0,) * i + (slice(None, None),) + (0,) * (futures.ndim - i - 1))
              for i in range(futures.ndim)]
    chunks = [[client.submit(get_dim, x, i) for x in futures[slc]]
              for i, slc in enumerate(slices)]
    dtype = client.submit(get_dtype, futures.flat[0])

    chunks, dtype = yield client._gather([chunks, dtype])
    chunks = tuple(map(tuple, chunks))

    name = 'array-from-futures-' + tokenize(*futures.flat)
    keys = list(product([name], *map(range, futures.shape)))
    values = list(futures.flat)
    dsk = dict(zip(keys, values))

    ensure_default_get(client)

    raise gen.Return(da.Array(dsk, name, chunks, dtype))


def futures_to_dask_array(futures, client=None):
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
    client: Client (optional)
        Client through which we access the remote dataframes
    """
    client = default_client(client)
    return sync(client.loop, _futures_to_dask_array, futures,
                client=client)


@gen.coroutine
def _futures_to_dask_bag(futures, client=None):
    client = default_client(client)

    name = 'bag-from-futures-' + tokenize(*futures)
    dsk = {(name, i): future for i, future in enumerate(futures)}

    ensure_default_get(client)

    raise gen.Return(db.Bag(dsk, name, len(futures)))


def futures_to_dask_bag(futures, client=None):
    """ Convert a list of futures into a dask.bag

    The futures should be point to Python iterables

    Parameters
    ----------
    futures: iterable of Futures
    client: Client (optional)
    """
    client = default_client(client)
    return sync(client.loop, _futures_to_dask_bag, futures,
                client=client)

@gen.coroutine
def _futures_to_collection(futures, client=None, **kwargs):
    client = default_client(client)
    element = futures
    while not isinstance(element, Future):
        element = element[0]

    typ = yield client.submit(type, element)._result()
    if 'pandas' in typ.__module__:
        func = _futures_to_dask_dataframe
    elif 'numpy' in typ.__module__:
        func = _futures_to_dask_array
    elif issubclass(typ, (tuple, list, set, frozenset)):
        func = _futures_to_dask_bag
    else:
        raise NotImplementedError("First future of type %s.  Expected "
                "numpy or pandas object" % typ.__name__)

    result = yield func(futures, client=client, **kwargs)
    raise gen.Return(result)


def futures_to_collection(futures, client=None, **kwargs):
    """ Convert futures into a dask collection

    Convert a list of futures pointing to a pandas dataframe into one logical
    dask.dataframe or a nested list of numpy arrays into a logical dask.array.

    The type of the collection will be chosen based on the type of the futures.

    See also
    --------
    futures_to_dask_array
    futures_to_dask_dataframe
    """
    client = default_client(client)
    return sync(client.loop, _futures_to_collection, futures,
                client=client, **kwargs)


@gen.coroutine
def _future_to_dask_array(future, client=None):
    e = default_client(client)

    name = 'array-' + str(future.key)

    shape, dtype = yield e._gather([e.submit(get_shape, future),
                                    e.submit(get_dtype, future)])
    dsk = {(name,) + (0,) * len(shape): future}
    chunks = tuple((d,) for d in shape)

    ensure_default_get(e)

    raise gen.Return(da.Array(dsk, name, chunks, dtype))


def future_to_dask_array(future, client=None):
    """
    Convert a single future to a single-chunked dask.array

    See Also:
        dask.array.stack
        dask.array.concatenate
        futures_to_dask_array
        futures_to_dask_arrays
    """
    client = default_client(client)
    return sync(client.loop, _future_to_dask_array, future,
                client=client)


@gen.coroutine
def _futures_to_dask_arrays(futures, client=None):
    e = default_client(client)

    names = ['array-' + str(future.key) for future in futures]

    shapes, dtypes = yield e._gather([e.map(get_shape, futures),
                                      e.map(get_dtype, futures)])

    dsks = [{(name,) + (0,) * len(shape): future}
            for name, shape, future in zip(names, shapes, futures)]
    chunkss = [tuple((d,) for d in shape) for shape in shapes]

    ensure_default_get(e)

    raise gen.Return([da.Array(dsk, name, chunks, dtype)
            for dsk, name, chunks, dtype in zip(dsks, names, chunkss, dtypes)])


def futures_to_dask_arrays(futures, client=None):
    """
    Convert a list of futures into a list of dask arrays

    Computation will be done to evaluate the shape and dtype of the futures.

    See Also:
        dask.array.stack
        dask.array.concatenate
        future_to_dask_array
        futures_to_dask_array
    """
    client = default_client(client)
    return sync(client.loop, _futures_to_dask_arrays, futures,
                client=client)
