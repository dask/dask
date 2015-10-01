from __future__ import print_function, division, absolute_import

from collections import defaultdict
from functools import partial
from time import sleep

from tornado import gen
from tornado.gen import Return

from .core import Server, read, write, rpc
from .utils import ignoring

log = print

def log(*args):
    with open('worker.log', 'a') as f:
        f.write(', '.join(list(map(str, args))))


class Center(Server):
    """ Central metadata storage

    A Center serves as central point of metadata storage among workers.  It
    maintains dictionaries of which worker has which keys and which keys are
    owned by which workers.  Computational systems tend to check in with a
    Center to determine their available resources.

    Example
    -------

    A center can be run in an event loop

    >>> c = Center('192.168.0.123', 8000)

    Or separately in a thread

    >>> c = Center('192.168.0.123', 8000, start=True, block=False)  # doctest: +SKIP
    >>> c.close()  # doctest: +SKIP
    """
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.who_has = defaultdict(set)
        self.has_what = defaultdict(set)
        self.ncores = dict()
        self.status = None

        d = {func.__name__: func
             for func in [self.add_keys, self.remove_keys, self.get_who_has,
                          self.get_has_what, self.register, self.get_ncores,
                          self.unregister, self.delete_data, self.terminate]}
        d = {k[len('get_'):] if k.startswith('get_') else k: v for k, v in
                d.items()}

        super(Center, self).__init__(d)

    @gen.coroutine
    def terminate(self, stream):
        self.stop()

    def register(self, stream, address=None, keys=(), ncores=None):
        self.has_what[address] = set(keys)
        self.ncores[address] = ncores
        print("Register %s" % str(address))
        return b'OK'

    def unregister(self, stream, address=None):
        if address not in self.has_what:
            return b'Address not found: ' + str(address).encode()
        keys = self.has_what.pop(address)
        with ignoring(KeyError):
            del self.ncores[address]
        for key in keys:
            self.who_has[key].remove(address)
        print("Unregister %s" % str(address))
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
            return {k: self.ncores[k] for k in addresses}
        else:
            return self.ncores

    @gen.coroutine
    def delete_data(self, stream, keys=None):
        who_has2 = {k: v for k, v in self.who_has.items() if k in keys}
        d = defaultdict(list)

        for key in keys:
            for worker in self.who_has[key]:
                self.has_what[worker].remove(key)
                d[worker].append(key)
            del self.who_has[key]

        yield [rpc(ip=worker[0], port=worker[1]).delete_data(
                keys=keys, report=False, close=True)
                      for worker, keys in d.items()]

        raise Return(b'OK')
