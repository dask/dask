from __future__ import print_function, division, absolute_import

from collections import Iterable, defaultdict
from itertools import cycle
import random
import socket
import uuid

from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from dask.base import tokenize
from toolz import merge, concat, groupby, drop, valmap

from .core import rpc, coerce_to_rpc, coerce_to_address
from .utils import ignore_exceptions, All, log_errors, tokey, sync
from .protocol.pickle import dumps, loads


no_default = '__no_default__'


@gen.coroutine
def gather_from_workers(who_has, rpc=rpc, close=True, permissive=False):
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
    who_has = {k: set(v) for k, v in who_has.items()}
    results = dict()
    all_bad_keys = set()

    while len(results) + len(all_bad_keys) < len(who_has):
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
            if permissive:
                all_bad_keys |= bad_keys
            else:
                raise KeyError(*bad_keys)

        rpcs = {addr: rpc(addr) for addr in d}
        try:
            coroutines = [rpcs[address].get_data(keys=keys, close=close)
                          for address, keys in d.items()]
            response = yield ignore_exceptions(coroutines, EnvironmentError)
        finally:
            for r in rpcs.values():
                r.close_rpc()

        response = merge(response)
        bad_addresses |= {v for k, v in rev.items() if k not in response}
        results.update(merge(response))

    if permissive:
        raise Return((results, all_bad_keys))
    else:
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


_round_robin_counter = [0]


@gen.coroutine
def scatter_to_workers(ncores, data, report=True, serialize=True):
    """ Scatter data directly to workers

    This distributes data in a round-robin fashion to a set of workers based on
    how many cores they have.  ncores should be a dictionary mapping worker
    identities to numbers of cores.

    See scatter for parameter docstring
    """
    if isinstance(ncores, Iterable) and not isinstance(ncores, dict):
        k = len(data) // len(ncores)
        ncores = {coerce_to_address(worker): k for worker in ncores}

    workers = list(concat([w] * nc for w, nc in ncores.items()))
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
    d = {worker: {key: dumps(value) if serialize else value
                   for _, key, value in v}
          for worker, v in d.items()}

    rpcs = {addr: rpc(addr) for addr in d}
    try:
        out = yield All([rpcs[address].update_data(data=v,
                                                 close=True, report=report)
                     for address, v in d.items()])
    finally:
        for r in rpcs.values():
            r.close_rpc()
    nbytes = merge(o['nbytes'] for o in out)

    who_has = {k: [w for w, _, _ in v] for k, v in groupby(1, L).items()}

    raise Return((names, who_has, nbytes))


collection_types = (tuple, list, set, frozenset)


def unpack_remotedata(o, byte_keys=False, myset=None):
    """ Unpack WrappedKey objects from collection

    Returns original collection and set of all found keys

    Examples
    --------
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

    Use the ``byte_keys=True`` keyword to force string keys

    >>> rd = WrappedKey(('x', 1))
    >>> unpack_remotedata(rd, byte_keys=True)
    ("('x', 1)", {"('x', 1)"})
    """
    if myset is None:
        myset = set()
        out = unpack_remotedata(o, byte_keys, myset)
        return out, myset

    typ = type(o)

    if typ in collection_types:
        if not o:
            return o
        outs = [unpack_remotedata(item, byte_keys, myset) for item in o]
        return type(o)(outs)
    elif typ is dict:
        if o:
            values = [unpack_remotedata(v, byte_keys, myset) for v in o.values()]
            return dict(zip(o.keys(), values))
        else:
            return o
    elif issubclass(typ, WrappedKey):  # TODO use type is Future
        k = o.key
        if byte_keys:
            k = tokey(k)
        myset.add(k)
        return k
    else:
        return o


def pack_data(o, d, key_types=object):
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
    typ = type(o)
    try:
        if isinstance(o, key_types) and  o in d:
            return d[o]
    except TypeError:
        pass

    if typ in collection_types:
        return typ([pack_data(x, d, key_types=key_types) for x in o])
    elif typ is dict:
        return {k: pack_data(v, d, key_types=key_types) for k, v in o.items()}
    else:
        return o
