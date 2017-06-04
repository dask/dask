from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import uuid

from tornado import gen
import tornado.locks

from .client import Future, _get_global_client, Client
from .metrics import time
from .utils import tokey, sync, log_errors

logger = logging.getLogger(__name__)


class VariableExtension(object):
    """ An extension for the scheduler to manage queues

    This adds the following routes to the scheduler

    *  variable-set
    *  variable-get
    *  variable-delete
    """
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.variables = dict()
        self.lingering = defaultdict(set)
        self.events = defaultdict(tornado.locks.Event)
        self.started = tornado.locks.Condition()

        self.scheduler.handlers.update({'variable_set': self.set,
                                        'variable_get': self.get})

        self.scheduler.client_handlers['variable-future-release'] = self.future_release
        self.scheduler.client_handlers['variable_delete'] = self.delete

        self.scheduler.extensions['queues'] = self

    def set(self, stream=None, name=None, key=None, data=None, client=None):
        if key is not None:
            record = {'type': 'Future', 'value': key}
            self.scheduler.client_desires_keys(keys=[key], client='variable-%s' % name)
        else:
            record = {'type': 'msgpack', 'value': data}
        try:
            old = self.variables[name]
        except KeyError:
            pass
        else:
            if old['type'] == 'Future':
                self.release(old['value'], name)
        if name not in self.variables:
            self.started.notify_all()
        self.variables[name] = record

    @gen.coroutine
    def release(self, key, name):
        while self.lingering[key, name]:
            yield self.events[name].wait()

        self.scheduler.client_releases_keys(keys=[key],
                                            client='variable-%s' % name)
        del self.lingering[key, name]

    def future_release(self, name=None, key=None, client=None):
        self.lingering[key, name].remove(client)
        self.events[name].set()

    @gen.coroutine
    def get(self, stream=None, name=None, client=None, timeout=None):
        start = time()
        while name not in self.variables:
            if timeout is not None:
                timeout2 = timeout - (time() - start)
            else:
                timeout2 = None
            if timeout2 < 0:
                raise gen.TimeoutError()
            yield self.started.wait(timeout=timeout2)
        record = self.variables[name]
        if record['type'] == 'Future':
            self.lingering[record['value'], name].add(client)
        raise gen.Return(record)

    @gen.coroutine
    def delete(self, stream=None, name=None, client=None):
        with log_errors():
            try:
                old = self.variables[name]
            except KeyError:
                pass
            else:
                if old['type'] == 'Future':
                    yield self.release(old['value'], name)
            del self.events[name]
            del self.variables[name]


class Variable(object):
    """ Distributed Global Variable

    This allows multiple clients to share futures and data between each other
    with a single mutable variable.  All metadata is sequentialized through the
    scheduler.  Race conditions can occur.

    Values must be either Futures or msgpack-encodable data (ints, lists,
    strings, etc..)  All data will be kept and sent through the scheduler, so
    it is wise not to send too much.  If you want to share a large amount of
    data then ``scatter`` it and share the future instead.

    Examples
    --------
    >>> from dask.distributed import Client, Variable # doctest: +SKIP
    >>> client = Client()  # doctest: +SKIP
    >>> x = Variable('x')  # doctest: +SKIP
    >>> x.set(123)  # docttest: +SKIP
    >>> x.get()  # docttest: +SKIP
    123
    >>> future = client.submit(f, x)  # doctest: +SKIP
    >>> x.set(future)  # doctest: +SKIP
    """
    def __init__(self, name=None, client=None, maxsize=0):
        self.client = client or _get_global_client()
        self.name = name or 'variable-' + uuid.uuid4().hex

    @gen.coroutine
    def _set(self, value):
        if isinstance(value, Future):
            yield self.client.scheduler.variable_set(key=tokey(value.key),
                                                     name=self.name)
        else:
            yield self.client.scheduler.variable_set(data=value,
                                                     name=self.name)

    def set(self, value):
        """ Set the value of this variable

        Parameters
        ----------
        value: Future or object
            Must be either a Future or a msgpack-encodable value
        """
        return sync(self.client.loop, self._set, value)

    @gen.coroutine
    def _get(self, timeout=None):
        d = yield self.client.scheduler.variable_get(timeout=timeout,
                                                     name=self.name,
                                                     client=self.client.id)
        if d['type'] == 'Future':
            value = Future(d['value'], self.client, inform=True)
            self.client._send_to_scheduler({'op': 'variable-future-release',
                                            'name': self.name,
                                            'key': d['value'],
                                            'client': self.client.id})
        else:
            value = d['value']
        raise gen.Return(value)

    def get(self, timeout=None):
        """ Get the value of this variable """
        return sync(self.client.loop, self._get, timeout=timeout)

    def delete(self):
        """ Delete this variable

        Caution, this affects all clients currently pointing to this variable.
        """
        if self.client.status == 'running':  # TODO: can leave zombie futures
            self.client._send_to_scheduler({'op': 'variable_delete',
                                            'name': self.name})

    def __getstate__(self):
        return (self.name, self.client.scheduler.address)

    def __setstate__(self, state):
        name, address = state
        client = _get_global_client()
        if client is None or client.scheduler.address != address:
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client)
