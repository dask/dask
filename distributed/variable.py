from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import uuid

from tornado import gen
import tornado.locks

try:
    from cytoolz import merge
except ImportError:
    from toolz import merge

from .client import Future, _get_global_client, Client
from .metrics import time
from .utils import tokey, log_errors
from .worker import get_client

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
        self.waiting = defaultdict(set)
        self.waiting_conditions = defaultdict(tornado.locks.Condition)
        self.started = tornado.locks.Condition()

        self.scheduler.handlers.update({'variable_set': self.set,
                                        'variable_get': self.get})

        self.scheduler.client_handlers['variable-future-release'] = self.future_release
        self.scheduler.client_handlers['variable_delete'] = self.delete

        self.scheduler.extensions['variables'] = self

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
            if old['type'] == 'Future' and old['value'] != key:
                self.release(old['value'], name)
        if name not in self.variables:
            self.started.notify_all()
        self.variables[name] = record

    @gen.coroutine
    def release(self, key, name):
        while self.waiting[key, name]:
            yield self.waiting_conditions[name].wait()

        self.scheduler.client_releases_keys(keys=[key],
                                            client='variable-%s' % name)
        del self.waiting[key, name]

    def future_release(self, name=None, key=None, token=None, client=None):
        self.waiting[key, name].remove(token)
        if not self.waiting[key, name]:
            self.waiting_conditions[name].notify_all()

    @gen.coroutine
    def get(self, stream=None, name=None, client=None, timeout=None):
        start = time()
        while name not in self.variables:
            if timeout is not None:
                left = timeout - (time() - start)
            else:
                left = None
            if left and left < 0:
                raise gen.TimeoutError()
            yield self.started.wait(timeout=left)
        record = self.variables[name]
        if record['type'] == 'Future':
            key = record['value']
            token = uuid.uuid4().hex
            try:
                state = self.scheduler.task_state[key]
            except KeyError:
                state = 'lost'
            msg = {'token': token, 'state': state}
            if state == 'erred':
                msg['exception'] = self.scheduler.exceptions[self.scheduler.exceptions_blame[key]]
                msg['traceback'] = self.scheduler.tracebacks[self.scheduler.exceptions_blame[key]]
            record = merge(record, msg)
            self.waiting[key, name].add(token)
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
            del self.waiting_conditions[name]
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

    .. warning::

       This object is experimental and has known issues in Python 2

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

    See Also
    --------
    Queue: shared multi-producer/multi-consumer queue between clients
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

    def set(self, value, **kwargs):
        """ Set the value of this variable

        Parameters
        ----------
        value: Future or object
            Must be either a Future or a msgpack-encodable value
        """
        return self.client.sync(self._set, value, **kwargs)

    @gen.coroutine
    def _get(self, timeout=None):
        d = yield self.client.scheduler.variable_get(timeout=timeout,
                                                     name=self.name,
                                                     client=self.client.id)
        if d['type'] == 'Future':
            value = Future(d['value'], self.client, inform=True, state=d['state'])
            if d['state'] == 'erred':
                value._state.set_error(d['exception'], d['traceback'])
            self.client._send_to_scheduler({'op': 'variable-future-release',
                                            'name': self.name,
                                            'key': d['value'],
                                            'token': d['token']})
        else:
            value = d['value']
        raise gen.Return(value)

    def get(self, timeout=None, **kwargs):
        """ Get the value of this variable """
        return self.client.sync(self._get, timeout=timeout, **kwargs)

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
        try:
            client = get_client(address)
            assert client.address == address
        except (AttributeError, AssertionError):
            client = Client(address, set_as_default=False)
        self.__init__(name=name, client=client)
