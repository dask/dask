from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from datetime import timedelta
import logging
import uuid

from tornado import gen
import tornado.locks

from .client import _get_global_client
from .utils import log_errors
from .worker import get_worker

logger = logging.getLogger(__name__)


class LockExtension(object):
    """ An extension for the scheduler to manage Locks

    This adds the following routes to the scheduler

    *  lock_acquire
    *  lock_release
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.events = defaultdict(deque)
        self.ids = dict()

        self.scheduler.handlers.update({'lock_acquire': self.acquire,
                                        'lock_release': self.release})

        self.scheduler.extensions['locks'] = self

    @gen.coroutine
    def acquire(self, stream=None, name=None, id=None, timeout=None):
        with log_errors():
            if name not in self.ids:
                result = True
            else:
                while name in self.ids:
                    event = tornado.locks.Event()
                    self.events[name].append(event)
                    future = event.wait()
                    if timeout is not None:
                        future = gen.with_timeout(timedelta(seconds=timeout), future)
                    try:
                        yield future
                    except gen.TimeoutError:
                        result = False
                        break
                    else:
                        result = True
                    finally:
                        event2 = self.events[name].popleft()
                        assert event is event2
            if result:
                assert name not in self.ids
                self.ids[name] = id
            raise gen.Return(result)

    def release(self, stream=None, name=None, id=None):
        with log_errors():
            if self.ids.get(name) != id:
                raise ValueError("This lock has not yet been acquired")
            del self.ids[name]
            if self.events[name]:
                self.scheduler.loop.add_callback(self.events[name][0].set)
            else:
                del self.events[name]


class Lock(object):
    """ Distributed Centralized Lock

    Parameters
    ----------
    name: string
        Name of the lock to acquire.  Choosing the same name allows two
        disconnected processes to coordinate a lock.

    Examples
    --------
    >>> lock = Lock('x')  # doctest: +SKIP
    >>> lock.acquire(timeout=1)  # doctest: +SKIP
    >>> # do things with protected resource
    >>> lock.release()  # doctest: +SKIP
    """
    def __init__(self, name=None, client=None):
        self.client = client or _get_global_client() or get_worker().client
        self.name = name or 'variable-' + uuid.uuid4().hex
        self.id = uuid.uuid4().hex
        self._locked = False

    def acquire(self, timeout=None):
        """ Acquire the lock

        Parameters
        ----------
        timeout: number
            Seconds to wait on the lock in the scheduler.  This does not
            include local coroutine time, network transfer time, etc..

        Examples
        --------
        >>> lock = Lock('x')  # doctest: +SKIP
        >>> lock.acquire(timeout=1)  # doctest: +SKIP

        Returns
        -------
        True or False whether or not it sucessfully acquired the lock
        """
        result = self.client.sync(self.client.scheduler.lock_acquire,
                                  name=self.name, id=self.id, timeout=timeout)
        self._locked = True
        return result

    def release(self):
        """ Release the lock if already acquired """
        if not self.locked():
            raise ValueError("Lock is not yet acquired")
        result = self.client.sync(self.client.scheduler.lock_release,
                                  name=self.name, id=self.id)
        self._locked = False
        return result

    def locked(self):
        return self._locked

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args, **kwargs):
        self.release()

    @gen.coroutine
    def __aenter__(self):
        yield self.acquire()
        raise gen.Return(self)

    @gen.coroutine
    def __aexit__(self, *args, **kwargs):
        yield self.release()
