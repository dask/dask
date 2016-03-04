from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
from functools import partial
import socket
from toolz import first, valmap

from tornado import gen
from tornado.gen import Return
from tornado.iostream import StreamClosedError

from .core import (Server, read, write, rpc, pingpong, send_recv,
        coerce_to_address)
from .utils import ignoring, ignore_exceptions, All, get_ip


logger = logging.getLogger(__name__)


class Center(Server):
    """ Central metadata storage

    A Center serves as central point of metadata storage among Workers.  It
    maintains dictionaries of which worker has which keys and which keys are
    owned by which workers.

    All worker nodes in the same network have the same center node.  They
    update and query this center node to share and learn what nodes have what
    data.

    **State**

    *   ``who_has:: {key: {workers}}``
        Set of workers that own a particular key
    *   ``has_what:: {worker: {keys}}``
        Set of keys owned by a particular worker
    *   ``ncores:: {worker: int}``
        Number of cores per worker
    *   ``worker_info``:: {worker: {str: data}}``:
        Information on each worker

    Workers and clients check in with the Center to discover available resources


    Examples
    --------
    You can start a center with the ``dcenter`` command line application::

       $ dcenter
       Start center at 127.0.0.1:8787

    Of you can create one in Python:

    >>> center = Center('192.168.0.123')  # doctest: +SKIP
    >>> center.listen(8787)  # doctest: +SKIP

    >>> center.has_what  # doctest: +SKIP
    {('alice', 8788):   {'x', 'y'}
     ('bob', 8788):     {'a', 'b', 'c'},
     ('charlie', 8788): {'w', 'x', 'b'}}

    >>> center.who_has  # doctest: +SKIP
    {'x': {('alice', 8788), ('charlie', 8788)},
     'y': {('alice', 8788)},
     'a': {('bob', 8788)},
     'b': {('bob', 8788), ('charlie', 8788)},
     'c': {('bob', 8788)},
     'w': {('charlie', 8788)}}

    >>> center.ncores  # doctest: +SKIP
    {('alice', 8788): 8,
     ('bob', 8788): 4,
     ('charlie', 8788): 4}

    >>> center.worker_info  # doctest: +SKIP
    {('alice', 8788): {'services': {'nanny', 8789}},
     ('bob', 8788): {'services': {'nanny', 8789}},
     ('charlie', 8788): {'servies': {'nanny', 8789}}}

    See Also
    --------
    distributed.worker.Worker:
    """
    def __init__(self, ip=None, **kwargs):
        self.ip = ip or get_ip()
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.ncores = dict()
        self.worker_info = defaultdict(dict)
        self.status = None

        d = {func.__name__: func
             for func in [self.add_keys, self.remove_keys, self.get_who_has,
                          self.get_has_what, self.register, self.get_ncores,
                          self.unregister, self.delete_data, self.terminate,
                          self.get_worker_services, self.broadcast]}
        d = {k[len('get_'):] if k.startswith('get_') else k: v for k, v in
                d.items()}
        d['ping'] = pingpong

        super(Center, self).__init__(d, **kwargs)

    @property
    def port(self):
        return first(self._sockets.values()).getsockname()[1]

    @gen.coroutine
    def terminate(self, stream=None):
        self.stop()
        return 'OK'

    def register(self, stream, address=None, keys=(), ncores=None,
                 **info):
        address = coerce_to_address(address)
        self.has_what[address] = set(keys)
        for key in keys:
            self.who_has[key].add(address)
        self.ncores[address] = ncores
        self.worker_info[address] = info
        logger.info("Register %s", str(address))
        return 'OK'

    def unregister(self, stream, address=None):
        address = coerce_to_address(address)
        if address not in self.has_what:
            return 'Address not found: ' + address
        keys = self.has_what.pop(address)
        with ignoring(KeyError):
            del self.ncores[address]
        with ignoring(KeyError):
            del self.worker_info[address]
        for key in keys:
            s = self.who_has[key]
            s.remove(address)
            if not s:
                del self.who_has[key]
        logger.info("Unregister %s", str(address))
        return 'OK'

    def add_keys(self, stream, address=None, keys=()):
        address = coerce_to_address(address)
        self.has_what[address].update(keys)
        for key in keys:
            self.who_has[key].add(address)
        return 'OK'

    def remove_keys(self, stream, keys=(), address=None):
        address = coerce_to_address(address)
        for key in keys:
            if key in self.has_what[address]:
                self.has_what[address].remove(key)
            with ignoring(KeyError):
                self.who_has[key].remove(address)
        return 'OK'

    def get_who_has(self, stream, keys=None):
        if keys is not None:
            return {k: list(self.who_has[k]) for k in keys}
        else:
            return valmap(list, self.who_has)

    def get_has_what(self, stream, keys=None):
        if keys:
            keys = [coerce_to_address(key) for key in keys]
        if keys is not None:
            return {k: list(self.has_what[k]) for k in keys}
        else:
            return valmap(list, self.has_what)

    def get_ncores(self, stream, addresses=None):
        if addresses:
            addresses = [coerce_to_address(a) for a in addresses]
        if addresses is not None:
            return {k: self.ncores.get(k, None) for k in addresses}
        else:
            return self.ncores

    def get_worker_services(self, stream, addresses=None):
        if addresses:
            addresses = [coerce_to_address(a) for a in address]
        if addresses is not None:
            return {k: self.worker_info.get(k, {}).get('services', None)
                    for k in addresses}
        else:
            return {k: self.worker_info[k].get('services', None)
                    for k in self.worker_info}

    @gen.coroutine
    def delete_data(self, stream, keys=None):
        keys = set(keys)
        who_has2 = {k: v for k, v in self.who_has.items() if k in keys}
        d = defaultdict(list)

        for key in keys:
            for worker in self.who_has[key]:
                self.has_what[worker].remove(key)
                d[worker].append(key)
            del self.who_has[key]

        # TODO: ignore missing workers
        coroutines = [rpc(worker).delete_data(keys=list(keys), report=False,
                                              close=True)
                      for worker, keys in d.items()]
        for worker, keys in d.items():
            logger.debug("Remove %d keys from worker %s", len(keys), worker)
        yield ignore_exceptions(coroutines, socket.error, StreamClosedError)

        raise Return('OK')

    @gen.coroutine
    def broadcast(self, stream, msg=None):
        """ Broadcast message to workers, return all results """
        workers = list(self.ncores)
        results = yield All([send_recv(arg=worker, close=True, **msg)
                             for worker in workers])
        raise Return(dict(zip(workers, results)))
