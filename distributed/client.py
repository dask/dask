from __future__ import print_function, division, absolute_import

from collections import Iterable, defaultdict
from itertools import count, cycle
import random
import socket
import uuid

from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from dask.base import tokenize
from toolz import merge, concat, groupby, drop

from .core import rpc, coerce_to_rpc
from .utils import ignore_exceptions, ignoring, All


no_default = '__no_default__'


def gather(center, needed):
    """ Gather data from distributed workers

    This operates by first asking the center who has all of the state keys and
    then trying those workers directly.

    Keys not found on the network will not appear in the output.  You should
    check the length of the output against the input if concerned about missing
    data.

    Parameters
    ----------
    center:
        (ip, port) tuple or Stream, or rpc object designating the Center
    needed: iterable
        A list of required keys

    Returns
    -------
    result: dict
        A mapping of the given keys to data values

    Examples
    --------
    >>> remote_data = scatter('127.0.0.1:8787', [1, 2, 3])  # doctest: +SKIP
    >>> local_data = gather('127.0.0.1:8787', remote_data)  # doctest: +SKIP

    See also
    --------
    distributed.client.scatter:
    distributed.client._gather:
    distributed.client.gather_from_workers:
    """
    func = lambda: _gather(center, needed)
    result = IOLoop().run_sync(func)
    return result


@gen.coroutine
def _gather(center, needed=[]):
    center = coerce_to_rpc(center)

    needed = [n.key if isinstance(n, WrappedKey) else n for n in needed]
    who_has = yield center.who_has(keys=needed)

    if not isinstance(who_has, dict):
        raise TypeError('Bad response from who_has: %s' % who_has)

    result = yield gather_from_workers(who_has)
    raise Return([result[key] for key in needed])

_gather.__doc__ = gather.__doc__


@gen.coroutine
def gather_from_workers(who_has):
    """ Gather data directly from peers

    Parameters
    ----------
    who_has: dict
        Dict mapping keys to sets of workers that may have that key

    Returns dict mapping key to value

    See Also
    --------
    gather
    _gather
    """
    bad_addresses = set()
    who_has = who_has.copy()
    results = dict()

    while len(results) < len(who_has):
        d = defaultdict(list)
        rev = dict()
        bad_keys = set()
        for key, addresses in who_has.items():
            if key in results:
                continue
            try:
                addr = random.choice(list(addresses - bad_addresses))
                d[addr].append(key)
                rev[key] = addr
            except IndexError:
                bad_keys.add(key)
        if bad_keys:
            raise KeyError(*bad_keys)

        coroutines = [rpc(ip=ip, port=port).get_data(keys=keys, close=True)
                            for (ip, port), keys in d.items()]
        response = yield ignore_exceptions(coroutines, socket.error,
                                                       StreamClosedError)
        response = merge(response)
        bad_addresses |= {v for k, v in rev.items() if k not in response}
        results.update(merge(response))

    raise Return(results)


class WrappedKey(object):
    """ Interface for a key in a dask graph.

    Subclasses must have .key attribute that refers to a key in a dask graph.

    Sometimes we want to associate metadata to keys in a dask graph.  For
    example we might know that that key lives on a particular machine or can
    only be accessed in a certain way.  Schedulers may have particular needs
    that can only be addressed by additional metadata.
    """
    def __init__(self, key):
        self.key = key


def scatter(center, data):
    """ Scatter data to workers

    Parameters
    ----------
    center:
        (ip, port) tuple or Stream, or rpc object designating the Center
    data: dict or iterable
        either a dictionary of key: value pairs or an iterable of values
    key:
        if data is an iterable of values then we use the key to generate keys
        as key-0, key-1, key-2, ...

    Examples
    --------
    >>> remote_data = scatter('127.0.0.1:8787', [1, 2, 3])  # doctest: +SKIP
    >>> local_data = gather('127.0.0.1:8787', remote_data)  # doctest: +SKIP

    See Also
    --------
    distributed.client.gather:
    distributed.client._scatter:
    distributed.client.scatter_to_workers:
    """
    func = lambda: _scatter(center, data)
    result = IOLoop().run_sync(func)
    return result


@gen.coroutine
def _scatter(center, data):
    center = coerce_to_rpc(center)
    ncores = yield center.ncores()

    result, who_has, nbytes = yield scatter_to_workers(ncores, data)
    raise Return(result)


_scatter.__doc__ = scatter.__doc__


_round_robin_counter = [0]


@gen.coroutine
def scatter_to_workers(ncores, data, report=True):
    """ Scatter data directly to workers

    This distributes data in a round-robin fashion to a set of workers based on
    how many cores they have.  ncores should be a dictionary mapping worker
    identities to numbers of cores.

    See scatter for parameter docstring
    """
    if isinstance(ncores, Iterable) and not isinstance(ncores, dict):
        k = len(data) // len(ncores)
        ncores = {worker: k for worker in ncores}

    workers = list(concat([w] * nc for w, nc in ncores.items()))
    in_type = type(data)
    if isinstance(data, dict):
        names, data = list(zip(*data.items()))
    else:
        names = []
        for x in data:
            try:
                names.append(tokenize(x))
            except:
                names.append(str(uuid.uuid1()))

    worker_iter = drop(_round_robin_counter[0] % len(workers), cycle(workers))
    _round_robin_counter[0] += len(data)

    L = list(zip(worker_iter, names, data))
    d = groupby(0, L)
    d = {k: {b: c for a, b, c in v}
          for k, v in d.items()}

    out = yield All([rpc(ip=w_ip, port=w_port).update_data(data=v,
                                             close=True, report=report)
                 for (w_ip, w_port), v in d.items()])
    nbytes = merge([o[1]['nbytes'] for o in out])

    who_has = {k: [w for w, _, _ in v] for k, v in groupby(1, L).items()}

    raise Return((names, who_has, nbytes))


@gen.coroutine
def broadcast_to_workers(workers, data, report=False, rpc=rpc):
    """ Broadcast data directly to all workers

    This sends all data to every worker.

    Currently this works inefficiently by sending all data out directly from
    the scheduler.  In the future we should have the workers communicate
    amongst themselves.

    Parameters
    ----------
    workers: sequence of (host, port) pairs
    data: sequence of data

    See Also
    --------
    scatter_to_workers
    """
    if isinstance(data, dict):
        names, data = list(zip(*data.items()))
    else:
        names = []
        for x in data:
            try:
                names.append(tokenize(x))
            except:
                names.append(str(uuid.uuid1()))
        data = dict(zip(names, data))

    out = yield All([rpc(ip=w_ip, port=w_port).update_data(data=data,
                                                           report=report)
                     for (w_ip, w_port) in workers])
    nbytes = merge([o[1]['nbytes'] for o in out])

    raise Return((names, nbytes))


@gen.coroutine
def _delete(center, keys):
    keys = [k.key if isinstance(k, WrappedKey) else k for k in keys]
    center = coerce_to_rpc(center)
    yield center.delete_data(keys=keys)

def delete(center, keys):
    """ Delete keys from all workers """
    return IOLoop().run_sync(lambda: _delete(center, keys))


@gen.coroutine
def _clear(center):
    center = coerce_to_rpc(center)
    who_has = yield center.who_has()
    yield center.delete_data(keys=list(who_has))

def clear(center):
    """ Clear all data from all workers' memory

    See Also
    --------
    distributed.client.delete
    """
    return IOLoop().run_sync(lambda: _clear(center))


def unpack_remotedata(o):
    """ Unpack WrappedKey objects from collection

    Returns original collection and set of all found keys

    >>> rd = WrappedKey('mykey')
    >>> unpack_remotedata(1)
    (1, set())
    >>> unpack_remotedata(())
    ((), set())
    >>> unpack_remotedata(rd)
    ('mykey', {'mykey'})
    >>> unpack_remotedata([1, rd])
    ([1, 'mykey'], {'mykey'})
    >>> unpack_remotedata({1: rd})
    ({1: 'mykey'}, {'mykey'})
    >>> unpack_remotedata({1: [rd]})
    ({1: ['mykey']}, {'mykey'})
    """
    if isinstance(o, WrappedKey):
        return o.key, {o.key}
    if isinstance(o, (tuple, list, set, frozenset)):
        if not o:
            return o, set()
        out, sets = zip(*map(unpack_remotedata, o))
        return type(o)(out), set.union(*sets)
    elif isinstance(o, dict):
        if o:
            values, sets = zip(*map(unpack_remotedata, o.values()))
            return dict(zip(o.keys(), values)), set.union(*sets)
        else:
            return o, set()
    else:
        return o, set()


def pack_data(o, d):
    """ Merge known data into tuple or dict

    Parameters
    ----------
    o:
        core data structures containing literals and keys
    d: dict
        mapping of keys to data

    Examples
    --------
    >>> data = {'x': 1}
    >>> pack_data(('x', 'y'), data)
    (1, 'y')
    >>> pack_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
    {'a': 1, 'b': 'y'}
    >>> pack_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
    {'a': [1], 'b': 'y'}
    """
    try:
        if o in d:
            return d[o]
    except (TypeError, KeyError):
        pass

    if isinstance(o, (tuple, list, set, frozenset)):
        return type(o)([pack_data(x, d) for x in o])
    elif isinstance(o, dict):
        return {k: pack_data(v, d) for k, v in o.items()}
    else:
        return o
