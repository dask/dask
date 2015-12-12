from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
from functools import partial
import socket

from tornado import gen
from tornado.gen import Return
from tornado.iostream import StreamClosedError

from .core import Server, read, write, rpc, pingpong, send_recv
from .utils import ignoring, ignore_exceptions, All


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
    *   ``nannies:: {worker: port}``
        The port of the nanny process for a particular worker

    Workers and clients check in with the Center to discover available resources


    Examples
    --------
    You can start a center with the ``dcenter`` command line application::

       $ dcenter
       Start center at 127.0.0.1:8787

    Of you can create one in Python:

    >>> center = Center('192.168.0.123', 8000)

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

    >>> center.nannies  # doctest: +SKIP
    {('alice', 8788): 8789,
     ('bob', 8788): 8789,
     ('charlie', 8788): 8789}

    See Also
    --------
    distributed.worker.Worker:
    """
    def __init__(self, ip, port, **kwargs):
        self.ip = ip
        self.port = port
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.ncores = dict()
        self.nannies = dict()
        self.status = None

        d = {func.__name__: func
             for func in [self.add_keys, self.remove_keys, self.get_who_has,
                          self.get_has_what, self.register, self.get_ncores,
                          self.unregister, self.delete_data, self.terminate,
                          self.get_nannies, self.broadcast]}
        d = {k[len('get_'):] if k.startswith('get_') else k: v for k, v in
                d.items()}
        d['ping'] = pingpong

        super(Center, self).__init__(d, **kwargs)

    @gen.coroutine
    def terminate(self, stream):
        self.stop()
        return b'OK'

    def register(self, stream, address=None, keys=(), ncores=None,
                 nanny_port=None):
        self.has_what[address] = set(keys)
        self.ncores[address] = ncores
        self.nannies[address] = nanny_port
        logger.info("Register %s", str(address))
        return b'OK'

    def unregister(self, stream, address=None):
        if address not in self.has_what:
            return b'Address not found: ' + str(address).encode()
        keys = self.has_what.pop(address)
        with ignoring(KeyError):
            del self.ncores[address]
        with ignoring(KeyError):
            del self.nannies[address]
        for key in keys:
            s = self.who_has[key]
            s.remove(address)
            if not s:
                del self.who_has[key]
        logger.info("Unregister %s", str(address))
        return b'OK'

    def add_keys(self, stream, address=None, keys=()):
        self.has_what[address].update(keys)
        for key in keys:
            self.who_has[key].add(address)
        return b'OK'

    def remove_keys(self, stream, keys=(), address=None):
        for key in keys:
            if key in self.has_what[address]:
                self.has_what[address].remove(key)
            with ignoring(KeyError):
                self.who_has[key].remove(address)
        return b'OK'

    def get_who_has(self, stream, keys=None):
        if keys is not None:
            return {k: self.who_has[k] for k in keys}
        else:
            return self.who_has

    def get_has_what(self, stream, keys=None):
        if keys is not None:
            return {k: self.has_what[k] for k in keys}
        else:
            return self.has_what

    def get_ncores(self, stream, addresses=None):
        if addresses is not None:
            return {k: self.ncores.get(k, None) for k in addresses}
        else:
            return self.ncores

    def get_nannies(self, stream, addresses=None):
        if addresses is not None:
            return {k: self.nannies.get(k, None) for k in addresses}
        else:
            return self.nannies

    @gen.coroutine
    def delete_data(self, stream, keys=None):
        who_has2 = {k: v for k, v in self.who_has.items() if k in keys}
        d = defaultdict(list)

        for key in keys:
            for worker in self.who_has[key]:
                self.has_what[worker].remove(key)
                d[worker].append(key)
            del self.who_has[key]

        # TODO: ignore missing workers
        coroutines = [rpc(ip=worker[0], port=worker[1]).delete_data(
                                keys=keys, report=False, close=True)
                      for worker, keys in d.items()]
        for worker, keys in d.items():
            logger.debug("Remove %d keys from worker %s", len(keys), worker)
        yield ignore_exceptions(coroutines, socket.error, StreamClosedError)

        raise Return(b'OK')

    @gen.coroutine
    def broadcast(self, stream, msg=None):
        """ Broadcast message to workers, return all results """
        workers = list(self.ncores)
        results = yield All([send_recv(ip=ip, port=port, close=True, **msg)
                             for ip, port in workers])
        raise Return(dict(zip(workers, results)))
