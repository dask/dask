from __future__ import print_function, division, absolute_import

from collections import Iterable, defaultdict
from itertools import count, cycle
import random
import socket
import uuid

from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, StreamClosedError

from toolz import merge, concat, groupby

from .core import rpc
from .utils import ignore_exceptions


no_default = '__no_default__'

def coerce_to_rpc(o):
    if isinstance(o, tuple):
        return rpc(ip=o[0], port=o[1])
    elif isinstance(o, IOStream):
        return rpc(stream=o)
    elif isinstance(o, rpc):
        return o
    else:
        raise TypeError()


@gen.coroutine
def gather_from_center(center, needed):
    """ Gather data from peers

    This accepts any nested collection of data and unpacks RemoteData objects
    within that nesting.  Keys not found are left as keys.

    See also:
        gather_strict_from_center
    """
    center = coerce_to_rpc(center)

    pre_result, needed = unpack_remotedata(needed)
    who_has = yield center.who_has(keys=needed)

    data = yield gather_from_workers(who_has)
    result = keys_to_data(pre_result, data)

    raise Return(result)


@gen.coroutine
def gather_strict_from_center(center, needed=[]):
    """ Gather data from peers

    This accepts an iterable, keys not found will not be in the output

    See also:
        gather_from_center
    """
    center = coerce_to_rpc(center)

    needed = [n.key if isinstance(n, RemoteData) else n for n in needed]
    who_has = yield center.who_has(keys=needed)

    if not isinstance(who_has, dict):
        raise TypeError('Bad response from who_has: %s' % who_has)

    result = yield gather_from_workers(who_has)
    raise Return([result[key] for key in needed])


@gen.coroutine
def gather_from_workers(who_has):
    """ gather data from peers """
    bad_addresses = set()
    who_has = who_has.copy()
    results = dict()

    while len(results) < len(who_has):
        d = defaultdict(list)
        rev = dict()
        for key, addresses in who_has.items():
            if key in results:
                continue
            try:
                addr = random.choice(list(addresses - bad_addresses))
            except IndexError:
                raise KeyError('No workers found that have key: %s' % str(key))
            d[addr].append(key)
            rev[key] = addr

        coroutines = [rpc(ip=ip, port=port).get_data(keys=keys, close=True)
                            for (ip, port), keys in d.items()]
        response = yield ignore_exceptions(coroutines, socket.error,
                                                       StreamClosedError)
        response = merge(response)
        bad_addresses |= {v for k, v in rev.items() if k not in response}
        results.update(merge(response))

    raise Return(results)


class RemoteData(object):
    """ Data living on a remote worker

    This is created by ``PendingComputation.get()`` which is in turn created by
    ``Pool.apply_async()``.  One can retrive the data from the remote worker by
    calling the ``.get()`` method on this object

    Example
    -------

    >>> pc = pool.apply_async(func, args, kwargs)  # doctest: +SKIP
    >>> rd = pc.get()  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10
    """
    trash = defaultdict(set)

    def __init__(self, key, center_ip, center_port, status=None,
                       result=no_default):
        self.key = key
        self.status = status
        self.center = rpc(ip=center_ip, port=center_port)
        self._result = result

    def __str__(self):
        if len(self.key) > 13:
            key = self.key[:10] + '...'
        else:
            key = self.key
        return "RemoteData<center=%s:%d, key=%s>" % (self.center.ip,
                self.center.port, key)

    __repr__ = __str__

    @gen.coroutine
    def _get(self, raiseit=True):
        who_has = yield self.center.who_has(
                keys=[self.key], close=True)
        ip, port = random.choice(list(who_has[self.key]))
        result = yield rpc(ip=ip, port=port).get_data(keys=[self.key], close=True)

        self._result = result[self.key]

        if raiseit and self.status == b'error':
            raise self._result
        else:
            raise Return(self._result)

    def get(self):
        if self._result is not no_default:
            return self._result
        else:
            result = IOLoop.current().run_sync(lambda: self._get(raiseit=False))
            if self.status == b'error':
                raise result
            else:
                return result

    @gen.coroutine
    def _delete(self):
        yield self.center.delete_data(keys=[self.key], close=True)

    """
    def delete(self):
        sync(self._delete(), self.loop)
    """

    def __del__(self):
        RemoteData.trash[(self.center.ip, self.center.port)].add(self.key)

    @classmethod
    @gen.coroutine
    def _garbage_collect(cls, ip=None, port=None):
        if ip and port:
            keys = cls.trash[(ip, port)]
            cors = [rpc(ip=ip, port=port).delete_data(keys=keys, close=True)]
            n = len(keys)
        else:
            cors = [rpc(ip=ip, port=port).delete_data(keys=keys, close=True)
                    for (ip, port), keys in cls.trash.items()]
            n = len(set.union(*cls.trash.values()))

        results = yield cors
        assert all(result == b'OK' for result in results)
        raise Return(n)

    @classmethod
    def garbage_collect(cls, ip=None, port=None):
        return IOLoop.current().run_sync(lambda: cls._garbage_collect(ip, port))


@gen.coroutine
def scatter_to_center(ip, port, data, key=None):
    """ Scatter data to workers

    See also:
        scatter_to_workers
    """
    ncores = yield rpc(ip=ip, port=port).ncores(close=True)

    result = yield scatter_to_workers(ip, port, ncores, data, key=key)
    raise Return(result)


@gen.coroutine
def scatter_to_workers(ip, port, ncores, data, key=None):
    """ Scatter data directly to workers

    This distributes data in a round-robin fashion to a set of workers based on
    how many cores they have.

    See also:
        scatter_to_center: check in with center first to find workers
    """
    if key is None:
        key = str(uuid.uuid1())

    if isinstance(ncores, Iterable) and not isinstance(ncores, dict):
        ncores = {worker: 1 for worker in ncores}

    workers = list(concat([w] * nc for w, nc in ncores.items()))
    names = ('%s-%d' % (key, i) for i in count(0))

    L = list(zip(cycle(workers), names, data))
    d = groupby(0, L)
    d = {k: {b: c for a, b, c in v}
          for k, v in d.items()}

    yield [rpc(ip=w_ip, port=w_port).update_data(data=v, close=True)
            for (w_ip, w_port), v in d.items()]

    result = [RemoteData(b, ip, port, result=c)
                for a, b, c in L]

    raise Return(result)


def unpack_remotedata(o):
    """ Unpack RemoteData objects from collection

    Returns original collection and set of all found keys

    >>> rd = RemoteData('mykey', '127.0.0.1', 8787)
    >>> unpack_remotedata(1)
    (1, set())
    >>> unpack_remotedata(rd)
    ('mykey', {'mykey'})
    >>> unpack_remotedata([1, rd])
    ([1, 'mykey'], {'mykey'})
    >>> unpack_remotedata({1: rd})
    ({1: 'mykey'}, {'mykey'})
    >>> unpack_remotedata({1: [rd]})
    ({1: ['mykey']}, {'mykey'})
    """
    if isinstance(o, RemoteData):
        return o.key, {o.key}
    elif isinstance(o, (tuple, list, set, frozenset)):
        out, sets = zip(*map(unpack_remotedata, o))
        return type(o)(out), set.union(*sets)
    elif isinstance(o, dict):
        if o:
            values, sets = zip(*map(unpack_remotedata, o.values()))
            return dict(zip(o.keys(), values)), set.union(*sets)
        else:
            return dict(), set()
    else:
        return o, set()


def keys_to_data(o, data):
    """ Merge known data into tuple or dict

    >>> data = {'x': 1}
    >>> keys_to_data(('x', 'y'), data)
    (1, 'y')
    >>> keys_to_data({'a': 'x', 'b': 'y'}, data)  # doctest: +SKIP
    {'a': 1, 'b': 'y'}
    >>> keys_to_data({'a': ['x'], 'b': 'y'}, data)  # doctest: +SKIP
    {'a': [1], 'b': 'y'}
    """
    try:
        if o in data:
            return data[o]
    except (TypeError, KeyError):
        pass

    if isinstance(o, (tuple, list, set, frozenset)):
        return type(o)([keys_to_data(x, data) for x in o])
    elif isinstance(o, dict):
        return {k: keys_to_data(v, data) for k, v in o.items()}
    else:
        return o
