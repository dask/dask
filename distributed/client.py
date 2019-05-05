from __future__ import print_function, division, absolute_import

import atexit
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures._base import DoneAndNotDoneFutures, CancelledError
from contextlib import contextmanager
import copy
from datetime import timedelta
import errno
from functools import partial
from glob import glob
import itertools
import json
import logging
from numbers import Number, Integral
import os
import sys
import uuid
import threading
import six
import socket
import warnings
import weakref

import dask
from dask.base import tokenize, normalize_token, collections_to_dsk
from dask.core import flatten, get_dependencies
from dask.optimization import SubgraphCallable
from dask.compatibility import apply, unicode
from dask.utils import ensure_dict

try:
    from cytoolz import first, groupby, merge, valmap, keymap
except ImportError:
    from toolz import first, groupby, merge, valmap, keymap
try:
    from dask.delayed import single_key
except ImportError:
    single_key = first
from tornado import gen
from tornado.gen import TimeoutError
from tornado.locks import Event, Condition, Semaphore
from tornado.ioloop import IOLoop
from tornado.queues import Queue

from .batched import BatchedSend
from .utils_comm import (
    WrappedKey,
    unpack_remotedata,
    pack_data,
    scatter_to_workers,
    gather_from_workers,
)
from .cfexecutor import ClientExecutor
from .compatibility import (
    Queue as pyQueue,
    Empty,
    isqueue,
    html_escape,
    StopAsyncIteration,
    Iterator,
)
from .core import connect, rpc, clean_exception, CommClosedError, PooledRPCCall
from .metrics import time
from .node import Node
from .protocol import to_serialize
from .protocol.pickle import dumps, loads
from .publish import Datasets
from .pubsub import PubSubClientExtension
from .security import Security
from .sizeof import sizeof
from .threadpoolexecutor import rejoin
from .worker import dumps_task, get_client, get_worker, secede
from .utils import (
    All,
    sync,
    funcname,
    ignoring,
    queue_to_iterator,
    tokey,
    log_errors,
    str_graph,
    key_split,
    format_bytes,
    asciitable,
    thread_state,
    no_default,
    PeriodicCallback,
    LoopRunner,
    parse_timedelta,
    shutting_down,
    Any,
)
from .versions import get_versions


logger = logging.getLogger(__name__)

_global_clients = weakref.WeakValueDictionary()
_global_client_index = [0]


DEFAULT_EXTENSIONS = [PubSubClientExtension]


def _get_global_client():
    L = sorted(list(_global_clients), reverse=True)
    for k in L:
        c = _global_clients[k]
        if c.status != "closed":
            return c
        else:
            del _global_clients[k]
    del L
    return None


def _set_global_client(c):
    if c is not None:
        _global_clients[_global_client_index[0]] = c
        _global_client_index[0] += 1


def _del_global_client(c):
    for k in list(_global_clients):
        try:
            if _global_clients[k] is c:
                del _global_clients[k]
        except KeyError:
            pass


class Future(WrappedKey):
    """ A remotely running computation

    A Future is a local proxy to a result running on a remote worker.  A user
    manages future objects in the local Python process to determine what
    happens in the larger cluster.

    Parameters
    ----------
    key: str, or tuple
        Key of remote data to which this future refers
    client: Client
        Client that should own this future.  Defaults to _get_global_client()
    inform: bool
        Do we inform the scheduler that we need an update on this future

    Examples
    --------
    Futures typically emerge from Client computations

    >>> my_future = client.submit(add, 1, 2)  # doctest: +SKIP

    We can track the progress and results of a future

    >>> my_future  # doctest: +SKIP
    <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>

    We can get the result or the exception and traceback from the future

    >>> my_future.result()  # doctest: +SKIP

    See Also
    --------
    Client:  Creates futures
    """

    _cb_executor = None
    _cb_executor_pid = None

    def __init__(self, key, client=None, inform=True, state=None):
        self.key = key
        self._cleared = False
        tkey = tokey(key)
        self.client = client or _get_global_client()
        self.client._inc_ref(tkey)
        self._generation = self.client.generation

        if tkey in self.client.futures:
            self._state = self.client.futures[tkey]
        else:
            self._state = self.client.futures[tkey] = FutureState()

        if inform:
            self.client._send_to_scheduler(
                {
                    "op": "client-desires-keys",
                    "keys": [tokey(key)],
                    "client": self.client.id,
                }
            )

        if state is not None:
            try:
                handler = self.client._state_handlers[state]
            except KeyError:
                pass
            else:
                handler(key=key)

    @property
    def executor(self):
        return self.client

    @property
    def status(self):
        return self._state.status

    def done(self):
        """ Is the computation complete? """
        return self._state.done()

    def result(self, timeout=None):
        """ Wait until computation completes, gather result to local process.

        If *timeout* seconds are elapsed before returning, a
        ``dask.distributed.TimeoutError`` is raised.
        """
        if self.client.asynchronous:
            return self.client.sync(self._result, callback_timeout=timeout)

        # shorten error traceback
        result = self.client.sync(self._result, callback_timeout=timeout, raiseit=False)
        if self.status == "error":
            six.reraise(*result)
        elif self.status == "cancelled":
            raise result
        else:
            return result

    @gen.coroutine
    def _result(self, raiseit=True):
        yield self._state.wait()
        if self.status == "error":
            exc = clean_exception(self._state.exception, self._state.traceback)
            if raiseit:
                six.reraise(*exc)
            else:
                raise gen.Return(exc)
        elif self.status == "cancelled":
            exception = CancelledError(self.key)
            if raiseit:
                raise exception
            else:
                raise gen.Return(exception)
        else:
            result = yield self.client._gather([self])
            raise gen.Return(result[0])

    @gen.coroutine
    def _exception(self):
        yield self._state.wait()
        if self.status == "error":
            raise gen.Return(self._state.exception)
        else:
            raise gen.Return(None)

    def exception(self, timeout=None, **kwargs):
        """ Return the exception of a failed task

        If *timeout* seconds are elapsed before returning, a
        ``dask.distributed.TimeoutError`` is raised.

        See Also
        --------
        Future.traceback
        """
        return self.client.sync(self._exception, callback_timeout=timeout, **kwargs)

    def add_done_callback(self, fn):
        """ Call callback on future when callback has finished

        The callback ``fn`` should take the future as its only argument.  This
        will be called regardless of if the future completes successfully,
        errs, or is cancelled

        The callback is executed in a separate thread.
        """
        cls = Future
        if cls._cb_executor is None or cls._cb_executor_pid != os.getpid():
            try:
                cls._cb_executor = ThreadPoolExecutor(
                    1, thread_name_prefix="Dask-Callback-Thread"
                )
            except TypeError:
                cls._cb_executor = ThreadPoolExecutor(1)
            cls._cb_executor_pid = os.getpid()

        def execute_callback(fut):
            try:
                fn(fut)
            except BaseException:
                logger.exception("Error in callback %s of %s:", fn, fut)

        self.client.loop.add_callback(
            done_callback, self, partial(cls._cb_executor.submit, execute_callback)
        )

    def cancel(self, **kwargs):
        """ Cancel request to run this future

        See Also
        --------
        Client.cancel
        """
        return self.client.cancel([self], **kwargs)

    def retry(self, **kwargs):
        """ Retry this future if it has failed

        See Also
        --------
        Client.retry
        """
        return self.client.retry([self], **kwargs)

    def cancelled(self):
        """ Returns True if the future has been cancelled """
        return self._state.status == "cancelled"

    @gen.coroutine
    def _traceback(self):
        yield self._state.wait()
        if self.status == "error":
            raise gen.Return(self._state.traceback)
        else:
            raise gen.Return(None)

    def traceback(self, timeout=None, **kwargs):
        """ Return the traceback of a failed task

        This returns a traceback object.  You can inspect this object using the
        ``traceback`` module.  Alternatively if you call ``future.result()``
        this traceback will accompany the raised exception.

        If *timeout* seconds are elapsed before returning, a
        ``dask.distributed.TimeoutError`` is raised.

        Examples
        --------
        >>> import traceback  # doctest: +SKIP
        >>> tb = future.traceback()  # doctest: +SKIP
        >>> traceback.format_tb(tb)  # doctest: +SKIP
        [...]

        See Also
        --------
        Future.exception
        """
        return self.client.sync(self._traceback, callback_timeout=timeout, **kwargs)

    @property
    def type(self):
        return self._state.type

    def release(self, _in_destructor=False):
        # NOTE: this method can be called from different threads
        # (see e.g. Client.get() or Future.__del__())
        if not self._cleared and self.client.generation == self._generation:
            self._cleared = True
            try:
                self.client.loop.add_callback(self.client._dec_ref, tokey(self.key))
            except TypeError:
                pass  # Shutting down, add_callback may be None

    def __getstate__(self):
        return (self.key, self.client.scheduler.address)

    def __setstate__(self, state):
        key, address = state
        c = get_client(address)
        Future.__init__(self, key, c)
        c._send_to_scheduler(
            {
                "op": "update-graph",
                "tasks": {},
                "keys": [tokey(self.key)],
                "client": c.id,
            }
        )

    def __del__(self):
        try:
            self.release()
        except RuntimeError:  # closed event loop
            pass

    def __repr__(self):
        if self.type:
            try:
                typ = self.type.__name__
            except AttributeError:
                typ = str(self.type)
            return "<Future: status: %s, type: %s, key: %s>" % (
                self.status,
                typ,
                self.key,
            )
        else:
            return "<Future: status: %s, key: %s>" % (self.status, self.key)

    def _repr_html_(self):
        text = "<b>Future: %s</b> " % html_escape(key_split(self.key))
        text += (
            '<font color="gray">status: </font>'
            '<font color="%(color)s">%(status)s</font>, '
        ) % {
            "status": self.status,
            "color": "red" if self.status == "error" else "black",
        }
        if self.type:
            try:
                typ = self.type.__name__
            except AttributeError:
                typ = str(self.type)
            text += '<font color="gray">type: </font>%s, ' % typ
        text += '<font color="gray">key: </font>%s' % html_escape(str(self.key))
        return text

    def __await__(self):
        return self.result().__await__()


class FutureState(object):
    """A Future's internal state.

    This is shared between all Futures with the same key and client.
    """

    __slots__ = ("_event", "status", "type", "exception", "traceback")

    def __init__(self):
        self._event = None
        self.status = "pending"
        self.type = None

    def _get_event(self):
        # Can't create Event eagerly in constructor as it can fetch
        # its IOLoop from the wrong thread
        # (https://github.com/tornadoweb/tornado/issues/2189)
        event = self._event
        if event is None:
            event = self._event = Event()
        return event

    def cancel(self):
        self.status = "cancelled"
        self._get_event().set()

    def finish(self, type=None):
        self.status = "finished"
        self._get_event().set()
        if type is not None:
            self.type = type

    def lose(self):
        self.status = "lost"
        self._get_event().clear()

    def retry(self):
        self.status = "pending"
        self._get_event().clear()

    def set_error(self, exception, traceback):
        _, exception, traceback = clean_exception(exception, traceback)

        self.status = "error"
        self.exception = exception
        self.traceback = traceback
        self._get_event().set()

    def done(self):
        return self._event is not None and self._event.is_set()

    def reset(self):
        self.status = "pending"
        if self._event is not None:
            self._event.clear()

    @gen.coroutine
    def wait(self, timeout=None):
        yield self._get_event().wait(timeout)

    def __repr__(self):
        return "<%s: %s>" % (self.__class__.__name__, self.status)


@gen.coroutine
def done_callback(future, callback):
    """ Coroutine that waits on future, then calls callback """
    while future.status == "pending":
        yield future._state.wait()
    callback(future)


@partial(normalize_token.register, Future)
def normalize_future(f):
    return [f.key, type(f)]


class AllExit(Exception):
    """Custom exception class to exit All(...) early.
    """


class Client(Node):
    """ Connect to and drive computation on a distributed Dask cluster

    The Client connects users to a dask.distributed compute cluster.  It
    provides an asynchronous user interface around functions and futures.  This
    class resembles executors in ``concurrent.futures`` but also allows
    ``Future`` objects within ``submit/map`` calls.

    Parameters
    ----------
    address: string, or Cluster
        This can be the address of a ``Scheduler`` server like a string
        ``'127.0.0.1:8786'`` or a cluster object like ``LocalCluster()``
    timeout: int
        Timeout duration for initial connection to the scheduler
    set_as_default: bool (True)
        Claim this scheduler as the global dask scheduler
    scheduler_file: string (optional)
        Path to a file with scheduler information if available
    security: (optional)
        Optional security information
    asynchronous: bool (False by default)
        Set to True if using this client within async/await functions or within
        Tornado gen.coroutines.  Otherwise this should remain False for normal
        use.
    name: string (optional)
        Gives the client a name that will be included in logs generated on
        the scheduler for matters relating to this client
    direct_to_workers: bool (optional)
        Whether or not to connect directly to the workers, or to ask
        the scheduler to serve as intermediary.
    heartbeat_interval: int
        Time in milliseconds between heartbeats to scheduler
    **kwargs:
        If you do not pass a scheduler address, Client will create a
        ``LocalCluster`` object, passing any extra keyword arguments.

    Examples
    --------
    Provide cluster's scheduler node address on initialization:

    >>> client = Client('127.0.0.1:8786')  # doctest: +SKIP

    Use ``submit`` method to send individual computations to the cluster

    >>> a = client.submit(add, 1, 2)  # doctest: +SKIP
    >>> b = client.submit(add, 10, 20)  # doctest: +SKIP

    Continue using submit or map on results to build up larger computations

    >>> c = client.submit(add, a, b)  # doctest: +SKIP

    Gather results with the ``gather`` method.

    >>> client.gather(c)  # doctest: +SKIP
    33

    You can also call Client with no arguments in order to create your own
    local cluster.

    >>> client = Client()  # makes your own local "cluster" # doctest: +SKIP

    Extra keywords will be passed directly to LocalCluster

    >>> client = Client(processes=False, threads_per_worker=1)  # doctest: +SKIP

    See Also
    --------
    distributed.scheduler.Scheduler: Internal scheduler
    distributed.deploy.local.LocalCluster:
    """

    def __init__(
        self,
        address=None,
        loop=None,
        timeout=no_default,
        set_as_default=True,
        scheduler_file=None,
        security=None,
        asynchronous=False,
        name=None,
        heartbeat_interval=None,
        serializers=None,
        deserializers=None,
        extensions=DEFAULT_EXTENSIONS,
        direct_to_workers=None,
        **kwargs
    ):
        if timeout == no_default:
            timeout = dask.config.get("distributed.comm.timeouts.connect")
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")
        self._timeout = timeout

        self.futures = dict()
        self.refcount = defaultdict(lambda: 0)
        self.coroutines = []
        if name is None:
            name = dask.config.get("client-name", None)
        self.id = (
            type(self).__name__
            + ("-" + name + "-" if name else "-")
            + str(uuid.uuid1(clock_seq=os.getpid()))
        )
        self.generation = 0
        self.status = "newly-created"
        self._pending_msg_buffer = []
        self.extensions = {}
        self.scheduler_file = scheduler_file
        self._startup_kwargs = kwargs
        self.cluster = None
        self.scheduler = None
        self._scheduler_identity = {}
        # A reentrant-lock on the refcounts for futures associated with this
        # client. Should be held by individual operations modifying refcounts,
        # or any bulk operation that needs to ensure the set of futures doesn't
        # change during operation.
        self._refcount_lock = threading.RLock()
        self.datasets = Datasets(self)
        self._serializers = serializers
        if deserializers is None:
            deserializers = serializers
        self._deserializers = deserializers
        self.direct_to_workers = direct_to_workers

        self._gather_semaphore = Semaphore(5)
        self._gather_keys = None
        self._gather_future = None

        # Communication
        self.security = security or Security()
        self.scheduler_comm = None
        assert isinstance(self.security, Security)

        if name == "worker":
            self.connection_args = self.security.get_connection_args("worker")
        else:
            self.connection_args = self.security.get_connection_args("client")

        if address is None:
            address = dask.config.get("scheduler-address", None)
            if address:
                logger.info("Config value `scheduler-address` found: %s", address)

        if isinstance(address, (rpc, PooledRPCCall)):
            self.scheduler = address
        elif hasattr(address, "scheduler_address"):
            # It's a LocalCluster or LocalCluster-compatible object
            self.cluster = address
            with ignoring(AttributeError):
                loop = address.loop

        self._connecting_to_scheduler = False
        self._asynchronous = asynchronous
        self._should_close_loop = not loop
        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        if heartbeat_interval is None:
            heartbeat_interval = dask.config.get("distributed.client.heartbeat")
        heartbeat_interval = parse_timedelta(heartbeat_interval, default="ms")

        self._periodic_callbacks = dict()
        self._periodic_callbacks["scheduler-info"] = PeriodicCallback(
            self._update_scheduler_info, 2000, io_loop=self.loop
        )
        self._periodic_callbacks["heartbeat"] = PeriodicCallback(
            self._heartbeat, heartbeat_interval * 1000, io_loop=self.loop
        )

        self._start_arg = address
        if set_as_default:
            self._set_config = dask.config.set(
                scheduler="dask.distributed", shuffle="tasks"
            )

        self._stream_handlers = {
            "key-in-memory": self._handle_key_in_memory,
            "lost-data": self._handle_lost_data,
            "cancelled-key": self._handle_cancelled_key,
            "task-retried": self._handle_retried_key,
            "task-erred": self._handle_task_erred,
            "restart": self._handle_restart,
            "error": self._handle_error,
        }

        self._state_handlers = {
            "memory": self._handle_key_in_memory,
            "lost": self._handle_lost_data,
            "erred": self._handle_task_erred,
        }

        super(Client, self).__init__(
            connection_args=self.connection_args,
            io_loop=self.loop,
            serializers=serializers,
            deserializers=deserializers,
            timeout=timeout,
        )

        for ext in extensions:
            ext(self)

        self.start(timeout=timeout)

        from distributed.recreate_exceptions import ReplayExceptionClient

        ReplayExceptionClient(self)

    @classmethod
    def current(cls):
        """ Return global client if one exists, otherwise raise ValueError """
        return default_client()

    @property
    def asynchronous(self):
        """ Are we running in the event loop?

        This is true if the user signaled that we might be when creating the
        client as in the following::

            client = Client(asynchronous=True)

        However, we override this expectation if we can definitively tell that
        we are running from a thread that is not the event loop.  This is
        common when calling get_client() from within a worker task.  Even
        though the client was originally created in asynchronous mode we may
        find ourselves in contexts when it is better to operate synchronously.
        """
        return self._asynchronous and self.loop is IOLoop.current()

    def sync(self, func, *args, **kwargs):
        asynchronous = kwargs.pop("asynchronous", None)
        if (
            asynchronous
            or self.asynchronous
            or getattr(thread_state, "asynchronous", False)
        ):
            callback_timeout = kwargs.pop("callback_timeout", None)
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = gen.with_timeout(timedelta(seconds=callback_timeout), future)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    def __repr__(self):
        # Note: avoid doing I/O here...
        info = self._scheduler_identity
        addr = info.get("address")
        if addr:
            workers = info.get("workers", {})
            nworkers = len(workers)
            ncores = sum(w["ncores"] for w in workers.values())
            return "<%s: scheduler=%r processes=%d cores=%d>" % (
                self.__class__.__name__,
                addr,
                nworkers,
                ncores,
            )
        elif self.scheduler is not None:
            return "<%s: scheduler=%r>" % (
                self.__class__.__name__,
                self.scheduler.address,
            )
        else:
            return "<%s: not connected>" % (self.__class__.__name__,)

    def _repr_html_(self):
        if (
            self.cluster
            and hasattr(self.cluster, "scheduler")
            and self.cluster.scheduler
        ):
            info = self.cluster.scheduler.identity()
            scheduler = self.cluster.scheduler
        elif (
            self._loop_runner.is_started()
            and self.scheduler
            and not (self.asynchronous and self.loop is IOLoop.current())
        ):
            info = sync(self.loop, self.scheduler.identity)
            scheduler = self.scheduler
        else:
            info = False
            scheduler = self.scheduler

        if scheduler is not None:
            text = (
                "<h3>Client</h3>\n" "<ul>\n" "  <li><b>Scheduler: </b>%s\n"
            ) % scheduler.address
        else:
            text = (
                "<h3>Client</h3>\n" "<ul>\n" "  <li><b>Scheduler: not connected</b>\n"
            )
        if info and "bokeh" in info["services"]:
            protocol, rest = scheduler.address.split("://")
            port = info["services"]["bokeh"]
            if protocol == "inproc":
                host = "localhost"
            else:
                host = rest.split(":")[0]
            template = dask.config.get("distributed.dashboard.link")
            address = template.format(host=host, port=port, **os.environ)
            text += (
                "  <li><b>Dashboard: </b><a href='%(web)s' target='_blank'>%(web)s</a>\n"
                % {"web": address}
            )

        text += "</ul>\n"

        if info:
            workers = len(info["workers"])
            cores = sum(w["ncores"] for w in info["workers"].values())
            memory = sum(w["memory_limit"] for w in info["workers"].values())
            memory = format_bytes(memory)
            text2 = (
                "<h3>Cluster</h3>\n"
                "<ul>\n"
                "  <li><b>Workers: </b>%d</li>\n"
                "  <li><b>Cores: </b>%d</li>\n"
                "  <li><b>Memory: </b>%s</li>\n"
                "</ul>\n"
            ) % (workers, cores, memory)

            return (
                '<table style="border: 2px solid white;">\n'
                "<tr>\n"
                '<td style="vertical-align: top; border: 0px solid white">\n%s</td>\n'
                '<td style="vertical-align: top; border: 0px solid white">\n%s</td>\n'
                "</tr>\n</table>"
            ) % (text, text2)

        else:
            return text

    def start(self, **kwargs):
        """ Start scheduler running in separate thread """
        if self.status != "newly-created":
            return

        self._loop_runner.start()

        _set_global_client(self)
        self.status = "connecting"

        if self.asynchronous:
            self._started = self._start(**kwargs)
        else:
            sync(self.loop, self._start, **kwargs)

    def __await__(self):
        if hasattr(self, "_started"):
            return self._started.__await__()
        else:

            @gen.coroutine
            def _():
                raise gen.Return(self)

            return _().__await__()

    def _send_to_scheduler_safe(self, msg):
        if self.status in ("running", "closing"):
            try:
                self.scheduler_comm.send(msg)
            except (CommClosedError, AttributeError):
                if self.status == "running":
                    raise
        elif self.status in ("connecting", "newly-created"):
            self._pending_msg_buffer.append(msg)

    def _send_to_scheduler(self, msg):
        if self.status in ("running", "closing", "connecting", "newly-created"):
            self.loop.add_callback(self._send_to_scheduler_safe, msg)
        else:
            raise Exception(
                "Tried sending message after closing.  Status: %s\n"
                "Message: %s" % (self.status, msg)
            )

    @gen.coroutine
    def _start(self, timeout=no_default, **kwargs):
        if timeout == no_default:
            timeout = self._timeout
        if timeout is not None:
            timeout = parse_timedelta(timeout, "s")

        address = self._start_arg
        if self.cluster is not None:
            # Ensure the cluster is started (no-op if already running)
            try:
                yield self.cluster._start()
            except AttributeError:  # Some clusters don't have this method
                pass
            except Exception:
                logger.info(
                    "Tried to start cluster and received an error. " "Proceeding.",
                    exc_info=True,
                )
            address = self.cluster.scheduler_address
        elif self.scheduler_file is not None:
            while not os.path.exists(self.scheduler_file):
                yield gen.sleep(0.01)
            for i in range(10):
                try:
                    with open(self.scheduler_file) as f:
                        cfg = json.load(f)
                    address = cfg["address"]
                    break
                except (ValueError, KeyError):  # JSON file not yet flushed
                    yield gen.sleep(0.01)
        elif self._start_arg is None:
            from .deploy import LocalCluster

            try:
                self.cluster = LocalCluster(
                    loop=self.loop, asynchronous=True, **self._startup_kwargs
                )
                yield self.cluster
            except (OSError, socket.error) as e:
                if e.errno != errno.EADDRINUSE:
                    raise
                # The default port was taken, use a random one
                self.cluster = LocalCluster(
                    scheduler_port=0,
                    loop=self.loop,
                    asynchronous=True,
                    **self._startup_kwargs
                )
                yield self.cluster

            # Wait for all workers to be ready
            # XXX should be a LocalCluster method instead
            while not self.cluster.workers or len(self.cluster.scheduler.workers) < len(
                self.cluster.workers
            ):
                yield gen.sleep(0.01)

            address = self.cluster.scheduler_address

        if self.scheduler is None:
            self.scheduler = self.rpc(address)
        self.scheduler_comm = None

        yield self._ensure_connected(timeout=timeout)

        for pc in self._periodic_callbacks.values():
            pc.start()

        self._handle_scheduler_coroutine = self._handle_report()
        self.coroutines.append(self._handle_scheduler_coroutine)

        raise gen.Return(self)

    @gen.coroutine
    def _reconnect(self):
        with log_errors():
            assert self.scheduler_comm.comm.closed()

            self.status = "connecting"
            self.scheduler_comm = None

            for st in self.futures.values():
                st.cancel()
            self.futures.clear()

            timeout = self._timeout
            deadline = self.loop.time() + timeout
            while timeout > 0 and self.status == "connecting":
                try:
                    yield self._ensure_connected(timeout=timeout)
                    break
                except EnvironmentError:
                    # Wait a bit before retrying
                    yield gen.sleep(0.1)
                    timeout = deadline - self.loop.time()
            else:
                logger.error(
                    "Failed to reconnect to scheduler after %.2f "
                    "seconds, closing client",
                    self._timeout,
                )
                yield self._close()

    @gen.coroutine
    def _ensure_connected(self, timeout=None):
        if (
            self.scheduler_comm
            and not self.scheduler_comm.closed()
            or self._connecting_to_scheduler
            or self.scheduler is None
        ):
            return

        self._connecting_to_scheduler = True

        try:
            comm = yield connect(
                self.scheduler.address,
                timeout=timeout,
                connection_args=self.connection_args,
            )
            comm.name = "Client->Scheduler"
            if timeout is not None:
                yield gen.with_timeout(
                    timedelta(seconds=timeout), self._update_scheduler_info()
                )
            else:
                yield self._update_scheduler_info()
            yield comm.write(
                {"op": "register-client", "client": self.id, "reply": False}
            )
        finally:
            self._connecting_to_scheduler = False
        if timeout is not None:
            msg = yield gen.with_timeout(timedelta(seconds=timeout), comm.read())
        else:
            msg = yield comm.read()
        assert len(msg) == 1
        assert msg[0]["op"] == "stream-start"

        bcomm = BatchedSend(interval="10ms", loop=self.loop)
        bcomm.start(comm)
        self.scheduler_comm = bcomm

        _set_global_client(self)
        self.status = "running"

        for msg in self._pending_msg_buffer:
            self._send_to_scheduler(msg)
        del self._pending_msg_buffer[:]

        logger.debug("Started scheduling coroutines. Synchronized")

    @gen.coroutine
    def _update_scheduler_info(self):
        if self.status not in ("running", "connecting"):
            return
        try:
            self._scheduler_identity = yield self.scheduler.identity()
        except EnvironmentError:
            logger.debug("Not able to query scheduler for identity")

    def _heartbeat(self):
        if self.scheduler_comm:
            self.scheduler_comm.send({"op": "heartbeat-client"})

    def __enter__(self):
        if not self._loop_runner.is_started():
            self.start()
        return self

    @gen.coroutine
    def __aenter__(self):
        yield self._started
        raise gen.Return(self)

    @gen.coroutine
    def __aexit__(self, typ, value, traceback):
        yield self._close()

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def _inc_ref(self, key):
        with self._refcount_lock:
            self.refcount[key] += 1

    def _dec_ref(self, key):
        with self._refcount_lock:
            self.refcount[key] -= 1
            if self.refcount[key] == 0:
                del self.refcount[key]
                self._release_key(key)

    def _release_key(self, key):
        """ Release key from distributed memory """
        logger.debug("Release key %s", key)
        st = self.futures.pop(key, None)
        if st is not None:
            st.cancel()
        if self.status != "closed":
            self._send_to_scheduler(
                {"op": "client-releases-keys", "keys": [key], "client": self.id}
            )

    @gen.coroutine
    def _handle_report(self):
        """ Listen to scheduler """
        with log_errors():
            try:
                while True:
                    if self.scheduler_comm is None:
                        break
                    try:
                        msgs = yield self.scheduler_comm.comm.read()
                    except CommClosedError:
                        if self.status == "running":
                            logger.info("Client report stream closed to scheduler")
                            logger.info("Reconnecting...")
                            self.status = "connecting"
                            yield self._reconnect()
                            continue
                        else:
                            break
                    if not isinstance(msgs, (list, tuple)):
                        msgs = (msgs,)

                    breakout = False
                    for msg in msgs:
                        logger.debug("Client receives message %s", msg)

                        if "status" in msg and "error" in msg["status"]:
                            six.reraise(*clean_exception(**msg))

                        op = msg.pop("op")

                        if op == "close" or op == "stream-closed":
                            breakout = True
                            break

                        try:
                            handler = self._stream_handlers[op]
                            handler(**msg)
                        except Exception as e:
                            logger.exception(e)
                    if breakout:
                        break
            except CancelledError:
                pass

    def _handle_key_in_memory(self, key=None, type=None, workers=None):
        state = self.futures.get(key)
        if state is not None:
            if type and not state.type:  # Type exists and not yet set
                try:
                    type = loads(type)
                except Exception:
                    type = None
                # Here, `type` may be a str if actual type failed
                # serializing in Worker
            else:
                type = None
            state.finish(type)

    def _handle_lost_data(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.lose()

    def _handle_cancelled_key(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.cancel()

    def _handle_retried_key(self, key=None):
        state = self.futures.get(key)
        if state is not None:
            state.retry()

    def _handle_task_erred(self, key=None, exception=None, traceback=None):
        state = self.futures.get(key)
        if state is not None:
            state.set_error(exception, traceback)

    def _handle_restart(self):
        logger.info("Receive restart signal from scheduler")
        for state in self.futures.values():
            state.cancel()
        self.futures.clear()
        with ignoring(AttributeError):
            self._restart_event.set()

    def _handle_error(self, exception=None):
        logger.warning("Scheduler exception:")
        logger.exception(exception)

    @gen.coroutine
    def _close(self, fast=False):
        """ Send close signal and wait until scheduler completes """
        self.status = "closing"

        with log_errors():
            _del_global_client(self)
            for pc in self._periodic_callbacks.values():
                pc.stop()
            self._scheduler_identity = {}
            with ignoring(AttributeError):
                # clear the dask.config set keys
                with self._set_config:
                    pass
            if self.get == dask.config.get("get", None):
                del dask.config.config["get"]
            if self.status == "closed":
                raise gen.Return()

            if (
                self.scheduler_comm
                and self.scheduler_comm.comm
                and not self.scheduler_comm.comm.closed()
            ):
                self._send_to_scheduler({"op": "close-client"})
                self._send_to_scheduler({"op": "close-stream"})

            # Give the scheduler 'stream-closed' message 100ms to come through
            # This makes the shutdown slightly smoother and quieter
            with ignoring(AttributeError, gen.TimeoutError):
                yield gen.with_timeout(
                    timedelta(milliseconds=100),
                    self._handle_scheduler_coroutine,
                    quiet_exceptions=(CancelledError,),
                )

            if (
                self.scheduler_comm
                and self.scheduler_comm.comm
                and not self.scheduler_comm.comm.closed()
            ):
                yield self.scheduler_comm.close()
            for key in list(self.futures):
                self._release_key(key=key)
            if self._start_arg is None:
                with ignoring(AttributeError):
                    yield self.cluster._close()
            self.rpc.close()
            self.status = "closed"
            if _get_global_client() is self:
                _set_global_client(None)
            coroutines = set(self.coroutines)
            for f in self.coroutines:
                # cancel() works on asyncio futures (Tornado 5)
                # but is a no-op on Tornado futures
                with ignoring(RuntimeError):
                    f.cancel()
                if f.cancelled():
                    coroutines.remove(f)
            del self.coroutines[:]
            if not fast:
                with ignoring(TimeoutError):
                    yield gen.with_timeout(timedelta(seconds=2), list(coroutines))
            with ignoring(AttributeError):
                self.scheduler.close_rpc()
            self.scheduler = None

        self.status = "closed"

    _shutdown = _close

    def close(self, timeout=no_default):
        """ Close this client

        Clients will also close automatically when your Python session ends

        If you started a client without arguments like ``Client()`` then this
        will also close the local cluster that was started at the same time.

        See Also
        --------
        Client.restart
        """
        if timeout == no_default:
            timeout = self._timeout * 2
        # XXX handling of self.status here is not thread-safe
        if self.status == "closed":
            return
        self.status = "closing"

        if self.asynchronous:
            future = self._close()
            if timeout:
                future = gen.with_timeout(timedelta(seconds=timeout), future)
            return future

        if self._start_arg is None:
            with ignoring(AttributeError):
                self.cluster.close()

        sync(self.loop, self._close, fast=True)

        assert self.status == "closed"

        if self._should_close_loop and not shutting_down():
            self._loop_runner.stop()

    def shutdown(self, *args, **kwargs):
        """ Deprecated, see close instead

        This was deprecated because "shutdown" was sometimes confusingly
        thought to refer to the cluster rather than the client
        """
        warnings.warn("Shutdown is deprecated.  Please use close instead")
        return self.close(*args, **kwargs)

    def get_executor(self, **kwargs):
        """
        Return a concurrent.futures Executor for submitting tasks on this Client

        Parameters
        ----------
        **kwargs:
            Any submit()- or map()- compatible arguments, such as
            `workers` or `resources`.

        Returns
        -------
        An Executor object that's fully compatible with the concurrent.futures
        API.
        """
        return ClientExecutor(self, **kwargs)

    def submit(self, func, *args, **kwargs):
        """ Submit a function application to the scheduler

        Parameters
        ----------
        func: callable
        *args:
        **kwargs:
        pure: bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
        workers: set, iterable of sets
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)
        key: str
            Unique identifier for the task.  Defaults to function-name and hash
        allow_other_workers: bool (defaults to False)
            Used with `workers`. Indicates whether or not the computations
            may be performed on workers that are not in the `workers` set(s).
        retries: int (default to 0)
            Number of allowed automatic retries if the task fails
        priority: Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout: str timedelta (default '100ms')
            Allowed amount of time between calls to consider the same priority

        Examples
        --------
        >>> c = client.submit(add, a, b)  # doctest: +SKIP

        Returns
        -------
        Future

        See Also
        --------
        Client.map: Submit on many arguments at once
        """
        if not callable(func):
            raise TypeError("First input to submit must be a callable function")

        key = kwargs.pop("key", None)
        workers = kwargs.pop("workers", None)
        resources = kwargs.pop("resources", None)
        retries = kwargs.pop("retries", None)
        priority = kwargs.pop("priority", 0)
        fifo_timeout = kwargs.pop("fifo_timeout", "100ms")
        allow_other_workers = kwargs.pop("allow_other_workers", False)
        actor = kwargs.pop("actor", kwargs.pop("actors", False))
        pure = kwargs.pop("pure", not actor)

        if allow_other_workers not in (True, False, None):
            raise TypeError("allow_other_workers= must be True or False")

        if key is None:
            if pure:
                key = funcname(func) + "-" + tokenize(func, kwargs, *args)
            else:
                key = funcname(func) + "-" + str(uuid.uuid4())

        skey = tokey(key)

        with self._refcount_lock:
            if skey in self.futures:
                return Future(key, self, inform=False)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]
        if workers is not None:
            restrictions = {skey: workers}
            loose_restrictions = [skey] if allow_other_workers else []
        else:
            restrictions = {}
            loose_restrictions = []

        if kwargs:
            dsk = {skey: (apply, func, list(args), kwargs)}
        else:
            dsk = {skey: (func,) + tuple(args)}

        futures = self._graph_to_futures(
            dsk,
            [skey],
            restrictions,
            loose_restrictions,
            priority={skey: 0},
            user_priority=priority,
            resources={skey: resources} if resources else None,
            retries=retries,
            fifo_timeout=fifo_timeout,
            actors=actor,
        )

        logger.debug("Submit %s(...), %s", funcname(func), key)

        return futures[skey]

    def _threaded_map(self, q_out, func, qs_in, **kwargs):
        """ Internal function for mapping Queue """
        if isqueue(qs_in[0]):
            get = pyQueue.get
        elif isinstance(qs_in[0], Iterator):
            get = next
        else:
            raise NotImplementedError()

        while True:
            try:
                args = [get(q) for q in qs_in]
            except StopIteration as e:
                q_out.put(e)
                break
            f = self.submit(func, *args, **kwargs)
            q_out.put(f)

    def map(self, func, *iterables, **kwargs):
        """ Map a function on a sequence of arguments

        Arguments can be normal objects or Futures

        Parameters
        ----------
        func: callable
        iterables: Iterables, Iterators, or Queues
        key: str, list
            Prefix for task names if string.  Explicit names if list.
        pure: bool (defaults to True)
            Whether or not the function is pure.  Set ``pure=False`` for
            impure functions like ``np.random.random``.
        workers: set, iterable of sets
            A set of worker hostnames on which computations may be performed.
            Leave empty to default to all workers (common case)
        retries: int (default to 0)
            Number of allowed automatic retries if a task fails
        priority: Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout: str timedelta (default '100ms')
            Allowed amount of time between calls to consider the same priority
        **kwargs: dict
            Extra keywords to send to the function.
            Large values will be included explicitly in the task graph.

        Examples
        --------
        >>> L = client.map(func, sequence)  # doctest: +SKIP

        Returns
        -------
        List, iterator, or Queue of futures, depending on the type of the
        inputs.

        See also
        --------
        Client.submit: Submit a single function
        """
        if not callable(func):
            raise TypeError("First input to map must be a callable function")

        if all(map(isqueue, iterables)) or all(
            isinstance(i, Iterator) for i in iterables
        ):
            maxsize = kwargs.pop("maxsize", 0)
            q_out = pyQueue(maxsize=maxsize)
            t = threading.Thread(
                target=self._threaded_map,
                name="Threaded map()",
                args=(q_out, func, iterables),
                kwargs=kwargs,
            )
            t.daemon = True
            t.start()
            if isqueue(iterables[0]):
                return q_out
            else:
                return queue_to_iterator(q_out)

        key = kwargs.pop("key", None)
        key = key or funcname(func)
        workers = kwargs.pop("workers", None)
        retries = kwargs.pop("retries", None)
        resources = kwargs.pop("resources", None)
        user_priority = kwargs.pop("priority", 0)
        allow_other_workers = kwargs.pop("allow_other_workers", False)
        fifo_timeout = kwargs.pop("fifo_timeout", "100ms")
        actor = kwargs.pop("actor", kwargs.pop("actors", False))
        pure = kwargs.pop("pure", not actor)

        if allow_other_workers and workers is None:
            raise ValueError("Only use allow_other_workers= if using workers=")

        iterables = list(zip(*zip(*iterables)))
        if isinstance(key, list):
            keys = key
        else:
            if pure:
                keys = [
                    key + "-" + tokenize(func, kwargs, *args)
                    for args in zip(*iterables)
                ]
            else:
                uid = str(uuid.uuid4())
                keys = (
                    [
                        key + "-" + uid + "-" + str(i)
                        for i in range(min(map(len, iterables)))
                    ]
                    if iterables
                    else []
                )

        if not kwargs:
            dsk = {key: (func,) + args for key, args in zip(keys, zip(*iterables))}
        else:
            kwargs2 = {}
            dsk = {}
            for k, v in kwargs.items():
                if sizeof(v) > 1e5:
                    vv = dask.delayed(v)
                    kwargs2[k] = vv._key
                    dsk.update(vv.dask)
                else:
                    kwargs2[k] = v
            dsk.update(
                {
                    key: (apply, func, (tuple, list(args)), kwargs2)
                    for key, args in zip(keys, zip(*iterables))
                }
            )

        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]
        if isinstance(workers, (list, set)):
            if workers and isinstance(first(workers), (list, set)):
                if len(workers) != len(keys):
                    raise ValueError(
                        "You only provided %d worker restrictions"
                        " for a sequence of length %d" % (len(workers), len(keys))
                    )
                restrictions = dict(zip(keys, workers))
            else:
                restrictions = {k: workers for k in keys}
        elif workers is None:
            restrictions = {}
        else:
            raise TypeError("Workers must be a list or set of workers or None")
        if allow_other_workers not in (True, False, None):
            raise TypeError("allow_other_workers= must be True or False")
        if allow_other_workers is True:
            loose_restrictions = set(keys)
        else:
            loose_restrictions = set()

        priority = dict(zip(keys, range(len(keys))))

        if resources:
            resources = {k: resources for k in keys}
        else:
            resources = None

        futures = self._graph_to_futures(
            dsk,
            keys,
            restrictions,
            loose_restrictions,
            priority=priority,
            resources=resources,
            retries=retries,
            user_priority=user_priority,
            fifo_timeout=fifo_timeout,
            actors=actor,
        )
        logger.debug("map(%s, ...)", funcname(func))

        return [futures[tokey(k)] for k in keys]

    @gen.coroutine
    def _gather(self, futures, errors="raise", direct=None, local_worker=None):
        unpacked, future_set = unpack_remotedata(futures, byte_keys=True)
        keys = [tokey(future.key) for future in future_set]
        bad_data = dict()
        data = {}

        if direct is None:
            direct = self.direct_to_workers
        if direct is None:
            try:
                w = get_worker()
            except Exception:
                direct = False
            else:
                if w.scheduler.address == self.scheduler.address:
                    direct = True

        @gen.coroutine
        def wait(k):
            """ Want to stop the All(...) early if we find an error """
            st = self.futures[k]
            yield st.wait()
            if st.status != "finished" and errors == "raise":
                raise AllExit()

        while True:
            logger.debug("Waiting on futures to clear before gather")

            with ignoring(AllExit):
                yield All(
                    [wait(key) for key in keys if key in self.futures],
                    quiet_exceptions=AllExit,
                )

            failed = ("error", "cancelled")

            exceptions = set()
            bad_keys = set()
            for key in keys:
                if key not in self.futures or self.futures[key].status in failed:
                    exceptions.add(key)
                    if errors == "raise":
                        try:
                            st = self.futures[key]
                            exception = st.exception
                            traceback = st.traceback
                        except (AttributeError, KeyError):
                            six.reraise(CancelledError, CancelledError(key), None)
                        else:
                            six.reraise(type(exception), exception, traceback)
                    if errors == "skip":
                        bad_keys.add(key)
                        bad_data[key] = None
                    else:
                        raise ValueError("Bad value, `errors=%s`" % errors)

            keys = [k for k in keys if k not in bad_keys and k not in data]

            if local_worker:  # look inside local worker
                data.update(
                    {k: local_worker.data[k] for k in keys if k in local_worker.data}
                )
                keys = [k for k in keys if k not in data]

            # We now do an actual remote communication with workers or scheduler
            if self._gather_future:  # attach onto another pending gather request
                self._gather_keys |= set(keys)
                response = yield self._gather_future
            else:  # no one waiting, go ahead
                self._gather_keys = set(keys)
                future = self._gather_remote(direct, local_worker)
                if self._gather_keys is None:
                    self._gather_future = None
                else:
                    self._gather_future = future
                response = yield future

            if response["status"] == "error":
                log = logger.warning if errors == "raise" else logger.debug
                log(
                    "Couldn't gather %s keys, rescheduling %s",
                    len(response["keys"]),
                    response["keys"],
                )
                for key in response["keys"]:
                    self._send_to_scheduler({"op": "report-key", "key": key})
                for key in response["keys"]:
                    try:
                        self.futures[key].reset()
                    except KeyError:  # TODO: verify that this is safe
                        pass
            else:
                break

        if bad_data and errors == "skip" and isinstance(unpacked, list):
            unpacked = [f for f in unpacked if f not in bad_data]

        data.update(response["data"])
        result = pack_data(unpacked, merge(data, bad_data))
        raise gen.Return(result)

    @gen.coroutine
    def _gather_remote(self, direct, local_worker):
        """ Perform gather with workers or scheduler

        This method exists to limit and batch many concurrent gathers into a
        few.  In controls access using a Tornado semaphore, and picks up keys
        from other requests made recently.
        """
        yield self._gather_semaphore.acquire()
        keys = list(self._gather_keys)
        self._gather_keys = None  # clear state, these keys are being sent off
        self._gather_future = None

        try:
            if direct or local_worker:  # gather directly from workers
                who_has = yield self.scheduler.who_has(keys=keys)
                data2, missing_keys, missing_workers = yield gather_from_workers(
                    who_has, rpc=self.rpc, close=False
                )
                response = {"status": "OK", "data": data2}
                if missing_keys:
                    keys2 = [key for key in keys if key not in data2]
                    response = yield self.scheduler.gather(keys=keys2)
                    if response["status"] == "OK":
                        response["data"].update(data2)

            else:  # ask scheduler to gather data for us
                response = yield self.scheduler.gather(keys=keys)
        finally:
            self._gather_semaphore.release()

        raise gen.Return(response)

    def _threaded_gather(self, qin, qout, **kwargs):
        """ Internal function for gathering Queue """
        while True:
            L = [qin.get()]
            while qin.empty():
                try:
                    L.append(qin.get_nowait())
                except Empty:
                    break
            results = self.gather(L, **kwargs)
            for item in results:
                qout.put(item)

    def gather(
        self, futures, errors="raise", maxsize=0, direct=None, asynchronous=None
    ):
        """ Gather futures from distributed memory

        Accepts a future, nested container of futures, iterator, or queue.
        The return type will match the input type.

        Parameters
        ----------
        futures: Collection of futures
            This can be a possibly nested collection of Future objects.
            Collections can be lists, sets, iterators, queues or dictionaries
        errors: string
            Either 'raise' or 'skip' if we should raise if a future has erred
            or skip its inclusion in the output collection
        direct: boolean
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.
        maxsize: int
            If the input is a queue then this produces an output queue with a
            maximum size.

        Returns
        -------
        results: a collection of the same type as the input, but now with
        gathered results rather than futures

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> x = c.submit(add, 1, 2)  # doctest: +SKIP
        >>> c.gather(x)  # doctest: +SKIP
        3
        >>> c.gather([x, [x], x])  # support lists and dicts # doctest: +SKIP
        [3, [3], 3]

        >>> seq = c.gather(iter([x, x]))  # support iterators # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        3

        See Also
        --------
        Client.scatter: Send data out to cluster
        """
        if isqueue(futures):
            qout = pyQueue(maxsize=maxsize)
            t = threading.Thread(
                target=self._threaded_gather,
                name="Threaded gather()",
                args=(futures, qout),
                kwargs={"errors": errors, "direct": direct},
            )
            t.daemon = True
            t.start()
            return qout
        elif isinstance(futures, Iterator):
            return (self.gather(f, errors=errors, direct=direct) for f in futures)
        else:
            if hasattr(thread_state, "execution_state"):  # within worker task
                local_worker = thread_state.execution_state["worker"]
            else:
                local_worker = None
            return self.sync(
                self._gather,
                futures,
                errors=errors,
                direct=direct,
                local_worker=local_worker,
                asynchronous=asynchronous,
            )

    @gen.coroutine
    def _scatter(
        self,
        data,
        workers=None,
        broadcast=False,
        direct=None,
        local_worker=None,
        timeout=no_default,
        hash=True,
    ):
        if timeout == no_default:
            timeout = self._timeout
        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]
        if isinstance(data, dict) and not all(
            isinstance(k, (bytes, unicode)) for k in data
        ):
            d = yield self._scatter(keymap(tokey, data), workers, broadcast)
            raise gen.Return({k: d[tokey(k)] for k in data})

        if isinstance(data, type(range(0))):
            data = list(data)
        input_type = type(data)
        names = False
        unpack = False
        if isinstance(data, Iterator):
            data = list(data)
        if isinstance(data, (set, frozenset)):
            data = list(data)
        if not isinstance(data, (dict, list, tuple, set, frozenset)):
            unpack = True
            data = [data]
        if isinstance(data, (list, tuple)):
            if hash:
                names = [type(x).__name__ + "-" + tokenize(x) for x in data]
            else:
                names = [type(x).__name__ + "-" + uuid.uuid4().hex for x in data]
            data = dict(zip(names, data))

        assert isinstance(data, dict)

        types = valmap(type, data)

        if direct is None:
            direct = self.direct_to_workers
        if direct is None:
            try:
                w = get_worker()
            except Exception:
                direct = False
            else:
                if w.scheduler.address == self.scheduler.address:
                    direct = True

        if local_worker:  # running within task
            local_worker.update_data(data=data, report=False)

            yield self.scheduler.update_data(
                who_has={key: [local_worker.address] for key in data},
                nbytes=valmap(sizeof, data),
                client=self.id,
            )

        else:
            data2 = valmap(to_serialize, data)
            if direct:
                ncores = None
                start = time()
                while not ncores:
                    if ncores is not None:
                        yield gen.sleep(0.1)
                    if time() > start + timeout:
                        raise gen.TimeoutError("No valid workers found")
                    ncores = yield self.scheduler.ncores(workers=workers)
                if not ncores:
                    raise ValueError("No valid workers")

                _, who_has, nbytes = yield scatter_to_workers(
                    ncores, data2, report=False, rpc=self.rpc
                )

                yield self.scheduler.update_data(
                    who_has=who_has, nbytes=nbytes, client=self.id
                )
            else:
                yield self.scheduler.scatter(
                    data=data2,
                    workers=workers,
                    client=self.id,
                    broadcast=broadcast,
                    timeout=timeout,
                )

        out = {k: Future(k, self, inform=False) for k in data}
        for key, typ in types.items():
            self.futures[key].finish(type=typ)

        if direct and broadcast:
            n = None if broadcast is True else broadcast
            yield self._replicate(list(out.values()), workers=workers, n=n)

        if issubclass(input_type, (list, tuple, set, frozenset)):
            out = input_type(out[k] for k in names)

        if unpack:
            assert len(out) == 1
            out = list(out.values())[0]
        raise gen.Return(out)

    def _threaded_scatter(self, q_or_i, qout, **kwargs):
        """ Internal function for scattering Iterable/Queue data """
        while True:
            if isqueue(q_or_i):
                L = [q_or_i.get()]
                while not q_or_i.empty():
                    try:
                        L.append(q_or_i.get_nowait())
                    except Empty:
                        break
            else:
                try:
                    L = [next(q_or_i)]
                except StopIteration as e:
                    qout.put(e)
                    break

            futures = self.scatter(L, **kwargs)
            for future in futures:
                qout.put(future)

    def scatter(
        self,
        data,
        workers=None,
        broadcast=False,
        direct=None,
        hash=True,
        maxsize=0,
        timeout=no_default,
        asynchronous=None,
    ):
        """ Scatter data into distributed memory

        This moves data from the local client process into the workers of the
        distributed scheduler.  Note that it is often better to submit jobs to
        your workers to have them load the data rather than loading data
        locally and then scattering it out to them.

        Parameters
        ----------
        data: list, iterator, dict, Queue, or object
            Data to scatter out to workers.  Output type matches input type.
        workers: list of tuples (optional)
            Optionally constrain locations of data.
            Specify workers as hostname/port pairs, e.g. ``('127.0.0.1', 8787)``.
        broadcast: bool (defaults to False)
            Whether to send each data element to all workers.
            By default we round-robin based on number of cores.
        direct: bool (defaults to automatically check)
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.
        maxsize: int (optional)
            Maximum size of queue if using queues, 0 implies infinite
        hash: bool (optional)
            Whether or not to hash data to determine key.
            If False then this uses a random key

        Returns
        -------
        List, dict, iterator, or queue of futures matching the type of input.

        Examples
        --------
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.scatter(1) # doctest: +SKIP
        <Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>

        >>> c.scatter([1, 2, 3])  # doctest: +SKIP
        [<Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,
         <Future: status: finished, key: 58e78e1b34eb49a68c65b54815d1b158>,
         <Future: status: finished, key: d3395e15f605bc35ab1bac6341a285e2>]

        >>> c.scatter({'x': 1, 'y': 2, 'z': 3})  # doctest: +SKIP
        {'x': <Future: status: finished, key: x>,
         'y': <Future: status: finished, key: y>,
         'z': <Future: status: finished, key: z>}

        Constrain location of data to subset of workers

        >>> c.scatter([1, 2, 3], workers=[('hostname', 8788)])   # doctest: +SKIP

        Handle streaming sequences of data with iterators or queues

        >>> seq = c.scatter(iter([1, 2, 3]))  # doctest: +SKIP
        >>> next(seq)  # doctest: +SKIP
        <Future: status: finished, key: c0a8a20f903a4915b94db8de3ea63195>,

        Broadcast data to all workers

        >>> [future] = c.scatter([element], broadcast=True)  # doctest: +SKIP

        Send scattered data to parallelized function using client futures
        interface

        >>> data = c.scatter(data, broadcast=True)  # doctest: +SKIP
        >>> res = [c.submit(func, data, i) for i in range(100)]

        See Also
        --------
        Client.gather: Gather data back to local process
        """
        if timeout == no_default:
            timeout = self._timeout
        if isqueue(data) or isinstance(data, Iterator):
            logger.debug("Starting thread for streaming data")
            qout = pyQueue(maxsize=maxsize)

            t = threading.Thread(
                target=self._threaded_scatter,
                name="Threaded scatter()",
                args=(data, qout),
                kwargs={"workers": workers, "broadcast": broadcast},
            )
            t.daemon = True
            t.start()

            if isqueue(data):
                return qout
            else:
                return queue_to_iterator(qout)
        else:
            if hasattr(thread_state, "execution_state"):  # within worker task
                local_worker = thread_state.execution_state["worker"]
            else:
                local_worker = None
            return self.sync(
                self._scatter,
                data,
                workers=workers,
                broadcast=broadcast,
                direct=direct,
                local_worker=local_worker,
                timeout=timeout,
                asynchronous=asynchronous,
                hash=hash,
            )

    @gen.coroutine
    def _cancel(self, futures, force=False):
        keys = list({tokey(f.key) for f in futures_of(futures)})
        yield self.scheduler.cancel(keys=keys, client=self.id, force=force)
        for k in keys:
            st = self.futures.pop(k, None)
            if st is not None:
                st.cancel()

    def cancel(self, futures, asynchronous=None, force=False):
        """
        Cancel running futures

        This stops future tasks from being scheduled if they have not yet run
        and deletes them if they have already run.  After calling, this result
        and all dependent results will no longer be accessible

        Parameters
        ----------
        futures: list of Futures
        force: boolean (False)
            Cancel this future even if other clients desire it
        """
        return self.sync(self._cancel, futures, asynchronous=asynchronous, force=force)

    @gen.coroutine
    def _retry(self, futures):
        keys = list({tokey(f.key) for f in futures_of(futures)})
        response = yield self.scheduler.retry(keys=keys, client=self.id)
        for key in response:
            st = self.futures[key]
            st.retry()

    def retry(self, futures, asynchronous=None):
        """
        Retry failed futures

        Parameters
        ----------
        futures: list of Futures
        """
        return self.sync(self._retry, futures, asynchronous=asynchronous)

    @gen.coroutine
    def _publish_dataset(self, *args, **kwargs):
        with log_errors():
            coroutines = []

            def add_coro(name, data):
                keys = [tokey(f.key) for f in futures_of(data)]
                coroutines.append(
                    self.scheduler.publish_put(
                        keys=keys, name=name, data=to_serialize(data), client=self.id
                    )
                )

            name = kwargs.pop("name", None)
            if name:
                if len(args) == 0:
                    raise ValueError(
                        "If name is provided, expecting call signature like"
                        " publish_dataset(df, name='ds')"
                    )
                # in case this is a singleton, collapse it
                elif len(args) == 1:
                    args = args[0]
                add_coro(name, args)

            for name, data in kwargs.items():
                add_coro(name, data)

            yield coroutines

    def publish_dataset(self, *args, **kwargs):
        """
        Publish named datasets to scheduler

        This stores a named reference to a dask collection or list of futures
        on the scheduler.  These references are available to other Clients
        which can download the collection or futures with ``get_dataset``.

        Datasets are not immediately computed.  You may wish to call
        ``Client.persist`` prior to publishing a dataset.

        Parameters
        ----------
        args : list of objects to publish as name
        name : optional name of the dataset to publish
        kwargs: dict
            named collections to publish on the scheduler

        Examples
        --------
        Publishing client:

        >>> df = dd.read_csv('s3://...')  # doctest: +SKIP
        >>> df = c.persist(df) # doctest: +SKIP
        >>> c.publish_dataset(my_dataset=df)  # doctest: +SKIP

        Alternative invocation
        >>> c.publish_dataset(df, name='my_dataset')

        Receiving client:

        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> df2 = c.get_dataset('my_dataset')  # doctest: +SKIP

        Returns
        -------
        None

        See Also
        --------
        Client.list_datasets
        Client.get_dataset
        Client.unpublish_dataset
        Client.persist
        """
        return self.sync(self._publish_dataset, *args, **kwargs)

    def unpublish_dataset(self, name, **kwargs):
        """
        Remove named datasets from scheduler

        Examples
        --------
        >>> c.list_datasets()  # doctest: +SKIP
        ['my_dataset']
        >>> c.unpublish_datasets('my_dataset')  # doctest: +SKIP
        >>> c.list_datasets()  # doctest: +SKIP
        []

        See Also
        --------
        Client.publish_dataset
        """
        return self.sync(self.scheduler.publish_delete, name=name, **kwargs)

    def list_datasets(self, **kwargs):
        """
        List named datasets available on the scheduler

        See Also
        --------
        Client.publish_dataset
        Client.get_dataset
        """
        return self.sync(self.scheduler.publish_list, **kwargs)

    @gen.coroutine
    def _get_dataset(self, name):
        out = yield self.scheduler.publish_get(name=name, client=self.id)
        if out is None:
            raise KeyError("Dataset '%s' not found" % name)

        with temp_default_client(self):
            data = out["data"]
        raise gen.Return(data)

    def get_dataset(self, name, **kwargs):
        """
        Get named dataset from the scheduler

        See Also
        --------
        Client.publish_dataset
        Client.list_datasets
        """
        return self.sync(self._get_dataset, name, **kwargs)

    @gen.coroutine
    def _run_on_scheduler(self, function, *args, **kwargs):
        wait = kwargs.pop("wait", True)
        response = yield self.scheduler.run_function(
            function=dumps(function), args=dumps(args), kwargs=dumps(kwargs), wait=wait
        )
        if response["status"] == "error":
            six.reraise(*clean_exception(**response))
        else:
            raise gen.Return(response["result"])

    def run_on_scheduler(self, function, *args, **kwargs):
        """ Run a function on the scheduler process

        This is typically used for live debugging.  The function should take a
        keyword argument ``dask_scheduler=``, which will be given the scheduler
        object itself.

        Examples
        --------

        >>> def get_number_of_tasks(dask_scheduler=None):
        ...     return len(dask_scheduler.tasks)

        >>> client.run_on_scheduler(get_number_of_tasks)  # doctest: +SKIP
        100

        Run asynchronous functions in the background:

        >>> async def print_state(dask_scheduler):  # doctest: +SKIP
        ...    while True:
        ...        print(dask_scheduler.status)
        ...        await gen.sleep(1)

        >>> c.run(print_state, wait=False)  # doctest: +SKIP

        See Also
        --------
        Client.run: Run a function on all workers
        Client.start_ipython_scheduler: Start an IPython session on scheduler
        """
        return self.sync(self._run_on_scheduler, function, *args, **kwargs)

    @gen.coroutine
    def _run(self, function, *args, **kwargs):
        nanny = kwargs.pop("nanny", False)
        workers = kwargs.pop("workers", None)
        wait = kwargs.pop("wait", True)
        responses = yield self.scheduler.broadcast(
            msg=dict(
                op="run",
                function=dumps(function),
                args=dumps(args),
                wait=wait,
                kwargs=dumps(kwargs),
            ),
            workers=workers,
            nanny=nanny,
        )
        results = {}
        for key, resp in responses.items():
            if resp["status"] == "OK":
                results[key] = resp["result"]
            elif resp["status"] == "error":
                six.reraise(*clean_exception(**resp))
        if wait:
            raise gen.Return(results)

    def run(self, function, *args, **kwargs):
        """
        Run a function on all workers outside of task scheduling system

        This calls a function on all currently known workers immediately,
        blocks until those results come back, and returns the results
        asynchronously as a dictionary keyed by worker address.  This method
        if generally used for side effects, such and collecting diagnostic
        information or installing libraries.

        If your function takes an input argument named ``dask_worker`` then
        that variable will be populated with the worker itself.

        Parameters
        ----------
        function: callable
        *args: arguments for remote function
        **kwargs: keyword arguments for remote function
        workers: list
            Workers on which to run the function. Defaults to all known workers.
        wait: boolean (optional)
            If the function is asynchronous whether or not to wait until that
            function finishes.

        Examples
        --------
        >>> c.run(os.getpid)  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321,
         '192.168.0.102:9000': 5555}

        Restrict computation to particular workers with the ``workers=``
        keyword argument.

        >>> c.run(os.getpid, workers=['192.168.0.100:9000',
        ...                           '192.168.0.101:9000'])  # doctest: +SKIP
        {'192.168.0.100:9000': 1234,
         '192.168.0.101:9000': 4321}

        >>> def get_status(dask_worker):
        ...     return dask_worker.status

        >>> c.run(get_hostname)  # doctest: +SKIP
        {'192.168.0.100:9000': 'running',
         '192.168.0.101:9000': 'running}

        Run asynchronous functions in the background:

        >>> async def print_state(dask_worker):  # doctest: +SKIP
        ...    while True:
        ...        print(dask_worker.status)
        ...        await gen.sleep(1)

        >>> c.run(print_state, wait=False)  # doctest: +SKIP
        """
        return self.sync(self._run, function, *args, **kwargs)

    def run_coroutine(self, function, *args, **kwargs):
        """
        Spawn a coroutine on all workers.

        This spaws a coroutine on all currently known workers and then waits
        for the coroutine on each worker.  The coroutines' results are returned
        as a dictionary keyed by worker address.

        Parameters
        ----------
        function: a coroutine function
            (typically a function wrapped in gen.coroutine or
             a Python 3.5+ async function)
        *args: arguments for remote function
        **kwargs: keyword arguments for remote function
        wait: boolean (default True)
            Whether to wait for coroutines to end.
        workers: list
            Workers on which to run the function. Defaults to all known workers.

        """
        warnings.warn(
            "This method has been deprecated. "
            "Instead use Client.run which detects async functions "
            "automatically",
            stacklevel=2,
        )
        return self.run(function, *args, **kwargs)

    def _graph_to_futures(
        self,
        dsk,
        keys,
        restrictions=None,
        loose_restrictions=None,
        priority=None,
        user_priority=0,
        resources=None,
        retries=None,
        fifo_timeout=0,
        actors=None,
    ):
        with self._refcount_lock:
            if resources:
                resources = self._expand_resources(
                    resources, all_keys=itertools.chain(dsk, keys)
                )

            if retries:
                retries = self._expand_retries(
                    retries, all_keys=itertools.chain(dsk, keys)
                )

            if actors is not None and actors is not True and actors is not False:
                actors = list(self._expand_key(actors))

            keyset = set(keys)
            flatkeys = list(map(tokey, keys))
            futures = {key: Future(key, self, inform=False) for key in keyset}

            values = {
                k for k, v in dsk.items() if isinstance(v, Future) and k not in keyset
            }
            if values:
                dsk = dask.optimization.inline(dsk, keys=values)

            d = {k: unpack_remotedata(v, byte_keys=True) for k, v in dsk.items()}
            extra_futures = set.union(*[v[1] for v in d.values()]) if d else set()
            extra_keys = {tokey(future.key) for future in extra_futures}
            dsk2 = str_graph({k: v[0] for k, v in d.items()}, extra_keys)
            dsk3 = {k: v for k, v in dsk2.items() if k is not v}
            for future in extra_futures:
                if future.client is not self:
                    msg = (
                        "Inputs contain futures that were created by " "another client."
                    )
                    raise ValueError(msg)

            if restrictions:
                restrictions = keymap(tokey, restrictions)
                restrictions = valmap(list, restrictions)

            if loose_restrictions is not None:
                loose_restrictions = list(map(tokey, loose_restrictions))

            future_dependencies = {
                tokey(k): {tokey(f.key) for f in v[1]} for k, v in d.items()
            }

            for s in future_dependencies.values():
                for v in s:
                    if v not in self.futures:
                        raise CancelledError(v)

            dependencies = {k: get_dependencies(dsk, k) for k in dsk}

            if priority is None:
                priority = dask.order.order(dsk, dependencies=dependencies)
                priority = keymap(tokey, priority)

            dependencies = {
                tokey(k): [tokey(dep) for dep in deps]
                for k, deps in dependencies.items()
            }
            for k, deps in future_dependencies.items():
                if deps:
                    dependencies[k] = list(set(dependencies.get(k, ())) | deps)

            if isinstance(retries, Number) and retries > 0:
                retries = {k: retries for k in dsk3}

            self._send_to_scheduler(
                {
                    "op": "update-graph",
                    "tasks": valmap(dumps_task, dsk3),
                    "dependencies": dependencies,
                    "keys": list(flatkeys),
                    "restrictions": restrictions or {},
                    "loose_restrictions": loose_restrictions,
                    "priority": priority,
                    "user_priority": user_priority,
                    "resources": resources,
                    "submitting_task": getattr(thread_state, "key", None),
                    "retries": retries,
                    "fifo_timeout": fifo_timeout,
                    "actors": actors,
                }
            )
            return futures

    def get(
        self,
        dsk,
        keys,
        restrictions=None,
        loose_restrictions=None,
        resources=None,
        sync=True,
        asynchronous=None,
        direct=None,
        retries=None,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        **kwargs
    ):
        """ Compute dask graph

        Parameters
        ----------
        dsk: dict
        keys: object, or nested lists of objects
        restrictions: dict (optional)
            A mapping of {key: {set of worker hostnames}} that restricts where
            jobs can take place
        retries: int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority: Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        sync: bool (optional)
            Returns Futures if False or concrete values if True (default).
        direct: bool
            Whether or not to connect directly to the workers, or to ask
            the scheduler to serve as intermediary.  This can also be set when
            creating the Client.

        Examples
        --------
        >>> from operator import add  # doctest: +SKIP
        >>> c = Client('127.0.0.1:8787')  # doctest: +SKIP
        >>> c.get({'x': (add, 1, 2)}, 'x')  # doctest: +SKIP
        3

        See Also
        --------
        Client.compute: Compute asynchronous collections
        """
        futures = self._graph_to_futures(
            dsk,
            keys=set(flatten([keys])),
            restrictions=restrictions,
            loose_restrictions=loose_restrictions,
            resources=resources,
            fifo_timeout=fifo_timeout,
            retries=retries,
            user_priority=priority,
            actors=actors,
        )
        packed = pack_data(keys, futures)
        if sync:
            if getattr(thread_state, "key", False):
                try:
                    secede()
                    should_rejoin = True
                except Exception:
                    should_rejoin = False
            try:
                results = self.gather(packed, asynchronous=asynchronous, direct=direct)
            finally:
                for f in futures.values():
                    f.release()
                if getattr(thread_state, "key", False) and should_rejoin:
                    rejoin()
            return results
        return packed

    def _optimize_insert_futures(self, dsk, keys):
        """ Replace known keys in dask graph with Futures

        When given a Dask graph that might have overlapping keys with our known
        results we replace the values of that graph with futures.  This can be
        used as an optimization to avoid recomputation.

        This returns the same graph if unchanged but a new graph if any changes
        were necessary.
        """
        with self._refcount_lock:
            changed = False
            for key in list(dsk):
                if tokey(key) in self.futures:
                    if not changed:
                        changed = True
                        dsk = ensure_dict(dsk)
                    dsk[key] = Future(key, self, inform=False)

        if changed:
            dsk, _ = dask.optimization.cull(dsk, keys)

        return dsk

    def normalize_collection(self, collection):
        """
        Replace collection's tasks by already existing futures if they exist

        This normalizes the tasks within a collections task graph against the
        known futures within the scheduler.  It returns a copy of the
        collection with a task graph that includes the overlapping futures.

        Examples
        --------
        >>> len(x.__dask_graph__())  # x is a dask collection with 100 tasks  # doctest: +SKIP
        100
        >>> set(client.futures).intersection(x.__dask_graph__())  # some overlap exists  # doctest: +SKIP
        10

        >>> x = client.normalize_collection(x)  # doctest: +SKIP
        >>> len(x.__dask_graph__())  # smaller computational graph  # doctest: +SKIP
        20

        See Also
        --------
        Client.persist: trigger computation of collection's tasks
        """
        dsk_orig = collection.__dask_graph__()
        dsk = self._optimize_insert_futures(dsk_orig, collection.__dask_keys__())

        if dsk is dsk_orig:
            return collection
        else:
            return redict_collection(collection, dsk)

    def compute(
        self,
        collections,
        sync=False,
        optimize_graph=True,
        workers=None,
        allow_other_workers=False,
        resources=None,
        retries=0,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        **kwargs
    ):
        """ Compute dask collections on cluster

        Parameters
        ----------
        collections: iterable of dask objects or single dask object
            Collections like dask.array or dataframe or dask.value objects
        sync: bool (optional)
            Returns Futures if False (default) or concrete values if True
        optimize_graph: bool
            Whether or not to optimize the underlying graphs
        workers: str, list, dict
            Which workers can run which parts of the computation
            If a string a list then the output collections will run on the listed
            workers, but other sub-computations can run anywhere
            If a dict then keys should be (tuples of) collections and values
            should be addresses or lists.
        allow_other_workers: bool, list
            If True then all restrictions in workers= are considered loose
            If a list then only the keys for the listed collections are loose
        retries: int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority: Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout: timedelta str (defaults to '60s')
            Allowed amount of time between calls to consider the same priority
        **kwargs:
            Options to pass to the graph optimize calls

        Returns
        -------
        List of Futures if input is a sequence, or a single future otherwise

        Examples
        --------
        >>> from dask import delayed
        >>> from operator import add
        >>> x = delayed(add)(1, 2)
        >>> y = delayed(add)(x, x)
        >>> xx, yy = client.compute([x, y])  # doctest: +SKIP
        >>> xx  # doctest: +SKIP
        <Future: status: finished, key: add-8f6e709446674bad78ea8aeecfee188e>
        >>> xx.result()  # doctest: +SKIP
        3
        >>> yy.result()  # doctest: +SKIP
        6

        Also support single arguments

        >>> xx = client.compute(x)  # doctest: +SKIP

        See Also
        --------
        Client.get: Normal synchronous dask.get function
        """
        if isinstance(collections, (list, tuple, set, frozenset)):
            singleton = False
        else:
            collections = [collections]
            singleton = True

        traverse = kwargs.pop("traverse", True)
        if traverse:
            collections = tuple(
                dask.delayed(a)
                if isinstance(a, (list, set, tuple, dict, Iterator))
                else a
                for a in collections
            )

        variables = [a for a in collections if dask.is_dask_collection(a)]

        dsk = self.collections_to_dsk(variables, optimize_graph, **kwargs)
        names = ["finalize-%s" % tokenize(v) for v in variables]
        dsk2 = {}
        for i, (name, v) in enumerate(zip(names, variables)):
            func, extra_args = v.__dask_postcompute__()
            keys = v.__dask_keys__()
            if func is single_key and len(keys) == 1 and not extra_args:
                names[i] = keys[0]
            else:
                dsk2[name] = (func, keys) + extra_args

        restrictions, loose_restrictions = self.get_restrictions(
            collections, workers, allow_other_workers
        )

        if not isinstance(priority, Number):
            priority = {k: p for c, p in priority.items() for k in self._expand_key(c)}

        futures_dict = self._graph_to_futures(
            merge(dsk2, dsk),
            names,
            restrictions,
            loose_restrictions,
            resources=resources,
            retries=retries,
            user_priority=priority,
            fifo_timeout=fifo_timeout,
            actors=actors,
        )

        i = 0
        futures = []
        for arg in collections:
            if dask.is_dask_collection(arg):
                futures.append(futures_dict[names[i]])
                i += 1
            else:
                futures.append(arg)

        if sync:
            result = self.gather(futures)
        else:
            result = futures

        if singleton:
            return first(result)
        else:
            return result

    def persist(
        self,
        collections,
        optimize_graph=True,
        workers=None,
        allow_other_workers=None,
        resources=None,
        retries=None,
        priority=0,
        fifo_timeout="60s",
        actors=None,
        **kwargs
    ):
        """ Persist dask collections on cluster

        Starts computation of the collection on the cluster in the background.
        Provides a new dask collection that is semantically identical to the
        previous one, but now based off of futures currently in execution.

        Parameters
        ----------
        collections: sequence or single dask object
            Collections like dask.array or dataframe or dask.value objects
        optimize_graph: bool
            Whether or not to optimize the underlying graphs
        workers: str, list, dict
            Which workers can run which parts of the computation
            If a string a list then the output collections will run on the listed
            workers, but other sub-computations can run anywhere
            If a dict then keys should be (tuples of) collections and values
            should be addresses or lists.
        allow_other_workers: bool, list
            If True then all restrictions in workers= are considered loose
            If a list then only the keys for the listed collections are loose
        retries: int (default to 0)
            Number of allowed automatic retries if computing a result fails
        priority: Number
            Optional prioritization of task.  Zero is default.
            Higher priorities take precedence
        fifo_timeout: timedelta str (defaults to '60s')
            Allowed amount of time between calls to consider the same priority
        kwargs:
            Options to pass to the graph optimize calls

        Returns
        -------
        List of collections, or single collection, depending on type of input.

        Examples
        --------
        >>> xx = client.persist(x)  # doctest: +SKIP
        >>> xx, yy = client.persist([x, y])  # doctest: +SKIP

        See Also
        --------
        Client.compute
        """
        if isinstance(collections, (tuple, list, set, frozenset)):
            singleton = False
        else:
            singleton = True
            collections = [collections]

        assert all(map(dask.is_dask_collection, collections))

        dsk = self.collections_to_dsk(collections, optimize_graph, **kwargs)

        names = {k for c in collections for k in flatten(c.__dask_keys__())}

        restrictions, loose_restrictions = self.get_restrictions(
            collections, workers, allow_other_workers
        )

        if not isinstance(priority, Number):
            priority = {k: p for c, p in priority.items() for k in self._expand_key(c)}

        futures = self._graph_to_futures(
            dsk,
            names,
            restrictions,
            loose_restrictions,
            resources=resources,
            retries=retries,
            user_priority=priority,
            fifo_timeout=fifo_timeout,
            actors=actors,
        )

        postpersists = [c.__dask_postpersist__() for c in collections]
        result = [
            func({k: futures[k] for k in flatten(c.__dask_keys__())}, *args)
            for (func, args), c in zip(postpersists, collections)
        ]

        if singleton:
            return first(result)
        else:
            return result

    @gen.coroutine
    def _upload_environment(self, zipfile):
        name = os.path.split(zipfile)[1]
        yield self._upload_large_file(zipfile, name)

        def unzip(dask_worker=None):
            from distributed.utils import log_errors
            import zipfile
            import shutil

            with log_errors():
                a = os.path.join(dask_worker.worker_dir, name)
                b = os.path.join(dask_worker.local_dir, name)
                c = os.path.dirname(b)
                shutil.move(a, b)

                with zipfile.ZipFile(b) as f:
                    f.extractall(path=c)

                for fn in glob(os.path.join(c, name[:-4], "bin", "*")):
                    st = os.stat(fn)
                    os.chmod(fn, st.st_mode | 64)  # chmod u+x fn

                assert os.path.exists(os.path.join(c, name[:-4]))
                return c

        yield self._run(unzip, nanny=True)
        raise gen.Return(name[:-4])

    def upload_environment(self, name, zipfile):
        return self.sync(self._upload_environment, name, zipfile)

    @gen.coroutine
    def _restart(self, timeout=no_default):
        if timeout == no_default:
            timeout = self._timeout * 2
        self._send_to_scheduler({"op": "restart", "timeout": timeout})
        self._restart_event = Event()
        try:
            yield self._restart_event.wait(self.loop.time() + timeout)
        except gen.TimeoutError:
            logger.error("Restart timed out after %f seconds", timeout)
            pass
        self.generation += 1
        with self._refcount_lock:
            self.refcount.clear()

        raise gen.Return(self)

    def restart(self, **kwargs):
        """ Restart the distributed network

        This kills all active work, deletes all data on the network, and
        restarts the worker processes.
        """
        return self.sync(self._restart, **kwargs)

    @gen.coroutine
    def _upload_file(self, filename, raise_on_error=True):
        with open(filename, "rb") as f:
            data = f.read()
        _, fn = os.path.split(filename)
        d = yield self.scheduler.broadcast(
            msg={"op": "upload_file", "filename": fn, "data": to_serialize(data)}
        )

        if any(v["status"] == "error" for v in d.values()):
            exceptions = [v["exception"] for v in d.values() if v["status"] == "error"]
            if raise_on_error:
                raise exceptions[0]
            else:
                raise gen.Return(exceptions[0])

        assert all(len(data) == v["nbytes"] for v in d.values())

    @gen.coroutine
    def _upload_large_file(self, local_filename, remote_filename=None):
        if remote_filename is None:
            remote_filename = os.path.split(local_filename)[1]

        with open(local_filename, "rb") as f:
            data = f.read()

        [future] = yield self._scatter([data])
        key = future.key
        yield self._replicate(future)

        def dump_to_file(dask_worker=None):
            if not os.path.isabs(remote_filename):
                fn = os.path.join(dask_worker.local_dir, remote_filename)
            else:
                fn = remote_filename
            with open(fn, "wb") as f:
                f.write(dask_worker.data[key])

            return len(dask_worker.data[key])

        response = yield self._run(dump_to_file)

        assert all(len(data) == v for v in response.values())

    def upload_file(self, filename, **kwargs):
        """ Upload local package to workers

        This sends a local file up to all worker nodes.  This file is placed
        into a temporary directory on Python's system path so any .py,  .egg
        or .zip  files will be importable.

        Parameters
        ----------
        filename: string
            Filename of .py, .egg or .zip file to send to workers

        Examples
        --------
        >>> client.upload_file('mylibrary.egg')  # doctest: +SKIP
        >>> from mylibrary import myfunc  # doctest: +SKIP
        >>> L = c.map(myfunc, seq)  # doctest: +SKIP
        """
        result = self.sync(
            self._upload_file, filename, raise_on_error=self.asynchronous, **kwargs
        )
        if isinstance(result, Exception):
            raise result
        else:
            return result

    @gen.coroutine
    def _rebalance(self, futures=None, workers=None):
        yield _wait(futures)
        keys = list({tokey(f.key) for f in self.futures_of(futures)})
        result = yield self.scheduler.rebalance(keys=keys, workers=workers)
        assert result["status"] == "OK"

    def rebalance(self, futures=None, workers=None, **kwargs):
        """ Rebalance data within network

        Move data between workers to roughly balance memory burden.  This
        either affects a subset of the keys/workers or the entire network,
        depending on keyword arguments.

        This operation is generally not well tested against normal operation of
        the scheduler.  It it not recommended to use it while waiting on
        computations.

        Parameters
        ----------
        futures: list, optional
            A list of futures to balance, defaults all data
        workers: list, optional
            A list of workers on which to balance, defaults to all workers
        """
        return self.sync(self._rebalance, futures, workers, **kwargs)

    @gen.coroutine
    def _replicate(self, futures, n=None, workers=None, branching_factor=2):
        futures = self.futures_of(futures)
        yield _wait(futures)
        keys = {tokey(f.key) for f in futures}
        yield self.scheduler.replicate(
            keys=list(keys), n=n, workers=workers, branching_factor=branching_factor
        )

    def replicate(self, futures, n=None, workers=None, branching_factor=2, **kwargs):
        """ Set replication of futures within network

        Copy data onto many workers.  This helps to broadcast frequently
        accessed data and it helps to improve resilience.

        This performs a tree copy of the data throughout the network
        individually on each piece of data.  This operation blocks until
        complete.  It does not guarantee replication of data to future workers.

        Parameters
        ----------
        futures: list of futures
            Futures we wish to replicate
        n: int, optional
            Number of processes on the cluster on which to replicate the data.
            Defaults to all.
        workers: list of worker addresses
            Workers on which we want to restrict the replication.
            Defaults to all.
        branching_factor: int, optional
            The number of workers that can copy data in each generation

        Examples
        --------
        >>> x = c.submit(func, *args)  # doctest: +SKIP
        >>> c.replicate([x])  # send to all workers  # doctest: +SKIP
        >>> c.replicate([x], n=3)  # send to three workers  # doctest: +SKIP
        >>> c.replicate([x], workers=['alice', 'bob'])  # send to specific  # doctest: +SKIP
        >>> c.replicate([x], n=1, workers=['alice', 'bob'])  # send to one of specific workers  # doctest: +SKIP
        >>> c.replicate([x], n=1)  # reduce replications # doctest: +SKIP

        See also
        --------
        Client.rebalance
        """
        return self.sync(
            self._replicate,
            futures,
            n=n,
            workers=workers,
            branching_factor=branching_factor,
            **kwargs
        )

    def ncores(self, workers=None, **kwargs):
        """ The number of threads/cores available on each worker node

        Parameters
        ----------
        workers: list (optional)
            A list of workers that we care about specifically.
            Leave empty to receive information about all workers.

        Examples
        --------
        >>> c.ncores()  # doctest: +SKIP
        {'192.168.1.141:46784': 8,
         '192.167.1.142:47548': 8,
         '192.167.1.143:47329': 8,
         '192.167.1.144:37297': 8}

        See Also
        --------
        Client.who_has
        Client.has_what
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]
        return self.sync(self.scheduler.ncores, workers=workers, **kwargs)

    def who_has(self, futures=None, **kwargs):
        """ The workers storing each future's data

        Parameters
        ----------
        futures: list (optional)
            A list of futures, defaults to all data

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.who_has()  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784'],
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': ['192.168.1.141:46784']}

        >>> c.who_has([x, y])  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': ['192.168.1.141:46784'],
         'inc-1e297fc27658d7b67b3a758f16bcf47a': ['192.168.1.141:46784']}

        See Also
        --------
        Client.has_what
        Client.ncores
        """
        if futures is not None:
            futures = self.futures_of(futures)
            keys = list(map(tokey, {f.key for f in futures}))
        else:
            keys = None
        return self.sync(self.scheduler.who_has, keys=keys, **kwargs)

    def has_what(self, workers=None, **kwargs):
        """ Which keys are held by which workers

        This returns the keys of the data that are held in each worker's
        memory.

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> wait([x, y, z])  # doctest: +SKIP
        >>> c.has_what()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.who_has
        Client.ncores
        Client.processing
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]
        return self.sync(self.scheduler.has_what, workers=workers, **kwargs)

    def processing(self, workers=None):
        """ The tasks currently running on each worker

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.processing()  # doctest: +SKIP
        {'192.168.1.141:46784': ['inc-1c8dd6be1c21646c71f76c16d09304ea',
                                 'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b',
                                 'inc-1e297fc27658d7b67b3a758f16bcf47a']}

        See Also
        --------
        Client.who_has
        Client.has_what
        Client.ncores
        """
        if isinstance(workers, tuple) and all(
            isinstance(i, (str, tuple)) for i in workers
        ):
            workers = list(workers)
        if workers is not None and not isinstance(workers, (tuple, list, set)):
            workers = [workers]
        return self.sync(self.scheduler.processing, workers=workers)

    def nbytes(self, keys=None, summary=True, **kwargs):
        """ The bytes taken up by each key on the cluster

        This is as measured by ``sys.getsizeof`` which may not accurately
        reflect the true cost.

        Parameters
        ----------
        keys: list (optional)
            A list of keys, defaults to all keys
        summary: boolean, (optional)
            Summarize keys into key types

        Examples
        --------
        >>> x, y, z = c.map(inc, [1, 2, 3])  # doctest: +SKIP
        >>> c.nbytes(summary=False)  # doctest: +SKIP
        {'inc-1c8dd6be1c21646c71f76c16d09304ea': 28,
         'inc-1e297fc27658d7b67b3a758f16bcf47a': 28,
         'inc-fd65c238a7ea60f6a01bf4c8a5fcf44b': 28}

        >>> c.nbytes(summary=True)  # doctest: +SKIP
        {'inc': 84}

        See Also
        --------
        Client.who_has
        """
        return self.sync(self.scheduler.nbytes, keys=keys, summary=summary, **kwargs)

    def call_stack(self, futures=None, keys=None):
        """ The actively running call stack of all relevant keys

        You can specify data of interest either by providing futures or
        collections in the ``futures=`` keyword or a list of explicit keys in
        the ``keys=`` keyword.  If neither are provided then all call stacks
        will be returned.

        Parameters
        ----------
        futures: list (optional)
            List of futures, defaults to all data
        keys: list (optional)
            List of key names, defaults to all data

        Examples
        --------
        >>> df = dd.read_parquet(...).persist()  # doctest: +SKIP
        >>> client.call_stack(df)  # call on collections

        >>> client.call_stack()  # Or call with no arguments for all activity  # doctest: +SKIP
        """
        keys = keys or []
        if futures is not None:
            futures = self.futures_of(futures)
            keys += list(map(tokey, {f.key for f in futures}))
        return self.sync(self.scheduler.call_stack, keys=keys or None)

    def profile(
        self,
        key=None,
        start=None,
        stop=None,
        workers=None,
        merge_workers=True,
        plot=False,
        filename=None,
    ):
        """ Collect statistical profiling information about recent work

        Parameters
        ----------
        key: str
            Key prefix to select, this is typically a function name like 'inc'
            Leave as None to collect all data
        start: time
        stop: time
        workers: list
            List of workers to restrict profile information
        plot: boolean or string
            Whether or not to return a plot object
        filename: str
            Filename to save the plot

        Examples
        --------
        >>> client.profile()  # call on collections
        >>> client.profile(filename='dask-profile.html')  # save to html file
        """
        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]

        return self.sync(
            self._profile,
            key=key,
            workers=workers,
            merge_workers=merge_workers,
            start=start,
            stop=stop,
            plot=plot,
            filename=filename,
        )

    @gen.coroutine
    def _profile(
        self,
        key=None,
        start=None,
        stop=None,
        workers=None,
        merge_workers=True,
        plot=False,
        filename=None,
    ):
        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]

        state = yield self.scheduler.profile(
            key=key,
            workers=workers,
            merge_workers=merge_workers,
            start=start,
            stop=stop,
        )

        if filename:
            plot = True

        if plot:
            from . import profile

            data = profile.plot_data(state)
            figure, source = profile.plot_figure(data, sizing_mode="stretch_both")

            if plot == "save" and not filename:
                filename = "dask-profile.html"

            from bokeh.plotting import save

            save(figure, title="Dask Profile", filename=filename)
            raise gen.Return((state, figure))

        else:
            raise gen.Return(state)

    def scheduler_info(self, **kwargs):
        """ Basic information about the workers in the cluster

        Examples
        --------
        >>> c.scheduler_info()  # doctest: +SKIP
        {'id': '2de2b6da-69ee-11e6-ab6a-e82aea155996',
         'services': {},
         'type': 'Scheduler',
         'workers': {'127.0.0.1:40575': {'active': 0,
                                         'last-seen': 1472038237.4845693,
                                         'name': '127.0.0.1:40575',
                                         'services': {},
                                         'stored': 0,
                                         'time-delay': 0.0061032772064208984}}}
        """
        self.sync(self._update_scheduler_info)
        return self._scheduler_identity

    def write_scheduler_file(self, scheduler_file):
        """ Write the scheduler information to a json file.

        This facilitates easy sharing of scheduler information using a file
        system. The scheduler file can be used to instantiate a second Client
        using the same scheduler.

        Parameters
        ----------
        scheduler_file: str
            Path to a write the scheduler file.

        Examples
        --------
        >>> client = Client()  # doctest: +SKIP
        >>> client.write_scheduler_file('scheduler.json')  # doctest: +SKIP
        # connect to previous client's scheduler
        >>> client2 = Client(scheduler_file='scheduler.json')  # doctest: +SKIP
        """
        if self.scheduler_file:
            raise ValueError("Scheduler file already set")
        else:
            self.scheduler_file = scheduler_file

        with open(self.scheduler_file, "w") as f:
            json.dump(self.scheduler_info(), f, indent=2)

    def get_metadata(self, keys, default=no_default):
        """ Get arbitrary metadata from scheduler

        See set_metadata for the full docstring with examples

        Parameters
        ----------
        keys: key or list
            Key to access.  If a list then gets within a nested collection
        default: optional
            If the key does not exist then return this value instead.
            If not provided then this raises a KeyError if the key is not
            present

        See also
        --------
        Client.set_metadata
        """
        if not isinstance(keys, (list, tuple)):
            keys = (keys,)
        return self.sync(self.scheduler.get_metadata, keys=keys, default=default)

    def get_scheduler_logs(self, n=None):
        """ Get logs from scheduler

        Parameters
        ----------
        n: int
            Number of logs to retrive.  Maxes out at 10000 by default,
            confiruable in config.yaml::log-length

        Returns
        -------
        Logs in reversed order (newest first)
        """
        return self.sync(self.scheduler.logs, n=n)

    def get_worker_logs(self, n=None, workers=None):
        """ Get logs from workers

        Parameters
        ----------
        n: int
            Number of logs to retrive.  Maxes out at 10000 by default,
            confiruable in config.yaml::log-length
        workers: iterable
            List of worker addresses to retrive.  Gets all workers by default.

        Returns
        -------
        Dictionary mapping worker address to logs.
        Logs are returned in reversed order (newest first)
        """
        return self.sync(self.scheduler.worker_logs, n=n, workers=workers)

    def retire_workers(self, workers=None, close_workers=True, **kwargs):
        """ Retire certain workers on the scheduler

        See dask.distributed.Scheduler.retire_workers for the full docstring.

        Examples
        --------
        You can get information about active workers using the following:
        >>> workers = client.scheduler_info()['workers']

        From that list you may want to select some workers to close
        >>> client.retire_workers(workers=['tcp://address:port', ...])

        See Also
        --------
        dask.distributed.Scheduler.retire_workers
        """
        return self.sync(
            self.scheduler.retire_workers,
            workers=workers,
            close_workers=close_workers,
            **kwargs
        )

    def set_metadata(self, key, value):
        """ Set arbitrary metadata in the scheduler

        This allows you to store small amounts of data on the central scheduler
        process for administrative purposes.  Data should be msgpack
        serializable (ints, strings, lists, dicts)

        If the key corresponds to a task then that key will be cleaned up when
        the task is forgotten by the scheduler.

        If the key is a list then it will be assumed that you want to index
        into a nested dictionary structure using those keys.  For example if
        you call the following::

            >>> client.set_metadata(['a', 'b', 'c'], 123)

        Then this is the same as setting

            >>> scheduler.task_metadata['a']['b']['c'] = 123

        The lower level dictionaries will be created on demand.

        Examples
        --------
        >>> client.set_metadata('x', 123)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        123

        >>> client.set_metadata(['x', 'y'], 123)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        {'y': 123}

        >>> client.set_metadata(['x', 'w', 'z'], 456)  # doctest: +SKIP
        >>> client.get_metadata('x')  # doctest: +SKIP
        {'y': 123, 'w': {'z': 456}}

        >>> client.get_metadata(['x', 'w'])  # doctest: +SKIP
        {'z': 456}

        See Also
        --------
        get_metadata
        """
        if not isinstance(key, list):
            key = (key,)
        return self.sync(self.scheduler.set_metadata, keys=key, value=value)

    def get_versions(self, check=False, packages=[]):
        """ Return version info for the scheduler, all workers and myself

        Parameters
        ----------
        check : boolean, default False
            raise ValueError if all required & optional packages
            do not match
        packages : List[str]
            Extra package names to check

        Examples
        --------
        >>> c.get_versions()  # doctest: +SKIP

        >>> c.get_versions(packages=['sklearn', 'geopandas'])  # doctest: +SKIP
        """
        client = get_versions(packages=packages)
        try:
            scheduler = sync(self.loop, self.scheduler.versions, packages=packages)
        except KeyError:
            scheduler = None
        except TypeError:  # packages keyword not supported
            scheduler = sync(self.loop, self.scheduler.versions)  # this raises

        workers = sync(
            self.loop,
            self.scheduler.broadcast,
            msg={"op": "versions", "packages": packages},
        )
        result = {"scheduler": scheduler, "workers": workers, "client": client}

        if check:
            # we care about the required & optional packages matching
            def to_packages(d):
                L = list(d["packages"].values())
                return dict(sum(L, type(L[0])()))

            client_versions = to_packages(result["client"])
            versions = [("scheduler", to_packages(result["scheduler"]))]
            versions.extend((w, to_packages(d)) for w, d in sorted(workers.items()))

            mismatched = defaultdict(list)
            for name, vers in versions:
                for pkg, cv in client_versions.items():
                    v = vers.get(pkg, "MISSING")
                    if cv != v:
                        mismatched[pkg].append((name, v))

            if mismatched:
                errs = []
                for pkg, versions in sorted(mismatched.items()):
                    rows = [("client", client_versions[pkg])]
                    rows.extend(versions)
                    errs.append("%s\n%s" % (pkg, asciitable(["", "version"], rows)))

                raise ValueError(
                    "Mismatched versions found\n" "\n" "%s" % ("\n\n".join(errs))
                )

        return result

    def futures_of(self, futures):
        return futures_of(futures, client=self)

    def start_ipython(self, *args, **kwargs):
        raise Exception("Method moved to start_ipython_workers")

    @gen.coroutine
    def _start_ipython_workers(self, workers):
        if workers is None:
            workers = yield self.scheduler.ncores()

        responses = yield self.scheduler.broadcast(
            msg=dict(op="start_ipython"), workers=workers
        )
        raise gen.Return((workers, responses))

    def start_ipython_workers(
        self, workers=None, magic_names=False, qtconsole=False, qtconsole_args=None
    ):
        """ Start IPython kernels on workers

        Parameters
        ----------
        workers: list (optional)
            A list of worker addresses, defaults to all

        magic_names: str or list(str) (optional)
            If defined, register IPython magics with these names for
            executing code on the workers.  If string has asterix then expand
            asterix into 0, 1, ..., n for n workers

        qtconsole: bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).

        qtconsole_args: list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> info = c.start_ipython_workers() # doctest: +SKIP
        >>> %remote info['192.168.1.101:5752'] worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', magic_names='w') # doctest: +SKIP
        >>> %w worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}

        >>> c.start_ipython_workers('192.168.1.101:5752', qtconsole=True) # doctest: +SKIP

        Add asterix * in magic names to add one magic per worker

        >>> c.start_ipython_workers(magic_names='w_*') # doctest: +SKIP
        >>> %w_0 worker.data  # doctest: +SKIP
        {'x': 1, 'y': 100}
        >>> %w_1 worker.data  # doctest: +SKIP
        {'z': 5}

        Returns
        -------
        iter_connection_info: list
            List of connection_info dicts containing info necessary
            to connect Jupyter clients to the workers.

        See Also
        --------
        Client.start_ipython_scheduler: start ipython on the scheduler
        """
        if isinstance(workers, six.string_types + (Number,)):
            workers = [workers]

        (workers, info_dict) = sync(self.loop, self._start_ipython_workers, workers)

        if magic_names and isinstance(magic_names, six.string_types):
            if "*" in magic_names:
                magic_names = [
                    magic_names.replace("*", str(i)) for i in range(len(workers))
                ]
            else:
                magic_names = [magic_names]

        if "IPython" in sys.modules:
            from ._ipython_utils import register_remote_magic

            register_remote_magic()
        if magic_names:
            from ._ipython_utils import register_worker_magic

            for worker, magic_name in zip(workers, magic_names):
                connection_info = info_dict[worker]
                register_worker_magic(connection_info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole

            for worker, connection_info in info_dict.items():
                name = "dask-" + worker.replace(":", "-").replace("/", "-")
                connect_qtconsole(connection_info, name=name, extra_args=qtconsole_args)
        return info_dict

    def start_ipython_scheduler(
        self, magic_name="scheduler_if_ipython", qtconsole=False, qtconsole_args=None
    ):
        """ Start IPython kernel on the scheduler

        Parameters
        ----------
        magic_name: str or None (optional)
            If defined, register IPython magic with this name for
            executing code on the scheduler.
            If not defined, register %scheduler magic if IPython is running.

        qtconsole: bool (optional)
            If True, launch a Jupyter QtConsole connected to the worker(s).

        qtconsole_args: list(str) (optional)
            Additional arguments to pass to the qtconsole on startup.

        Examples
        --------
        >>> c.start_ipython_scheduler() # doctest: +SKIP
        >>> %scheduler scheduler.processing  # doctest: +SKIP
        {'127.0.0.1:3595': {'inc-1', 'inc-2'},
         '127.0.0.1:53589': {'inc-2', 'add-5'}}

        >>> c.start_ipython_scheduler(qtconsole=True) # doctest: +SKIP

        Returns
        -------
        connection_info: dict
            connection_info dict containing info necessary
            to connect Jupyter clients to the scheduler.

        See Also
        --------
        Client.start_ipython_workers: Start IPython on the workers
        """
        info = sync(self.loop, self.scheduler.start_ipython)
        if magic_name == "scheduler_if_ipython":
            # default to %scheduler if in IPython, no magic otherwise
            in_ipython = False
            if "IPython" in sys.modules:
                from IPython import get_ipython

                in_ipython = bool(get_ipython())
            if in_ipython:
                magic_name = "scheduler"
            else:
                magic_name = None
        if magic_name:
            from ._ipython_utils import register_worker_magic

            register_worker_magic(info, magic_name)
        if qtconsole:
            from ._ipython_utils import connect_qtconsole

            connect_qtconsole(info, name="dask-scheduler", extra_args=qtconsole_args)
        return info

    @classmethod
    def _expand_key(cls, k):
        """
        Expand a user-provided task key specification, e.g. in a resources
        or retries dictionary.
        """
        if not isinstance(k, tuple):
            k = (k,)
        for kk in k:
            if dask.is_dask_collection(kk):
                for kkk in kk.__dask_keys__():
                    yield tokey(kkk)
            else:
                yield tokey(kk)

    @classmethod
    def _expand_retries(cls, retries, all_keys):
        """
        Expand the user-provided "retries" specification
        to a {task key: Integral} dictionary.
        """
        if retries and isinstance(retries, dict):
            result = {
                name: value
                for key, value in retries.items()
                for name in cls._expand_key(key)
            }
        elif isinstance(retries, Integral):
            # Each task unit may potentially fail, allow retrying all of them
            result = {name: retries for name in all_keys}
        else:
            raise TypeError(
                "`retries` should be an integer or dict, got %r" % (type(retries))
            )
        return keymap(tokey, result)

    def _expand_resources(cls, resources, all_keys):
        """
        Expand the user-provided "resources" specification
        to a {task key: {resource name: Number}} dictionary.
        """
        # Resources can either be a single dict such as {'GPU': 2},
        # indicating a requirement for all keys, or a nested dict
        # such as {'x': {'GPU': 1}, 'y': {'SSD': 4}} indicating
        # per-key requirements
        if not isinstance(resources, dict):
            raise TypeError("`resources` should be a dict, got %r" % (type(resources)))

        per_key_reqs = {}
        global_reqs = {}
        all_keys = list(all_keys)
        for k, v in resources.items():
            if isinstance(v, dict):
                # It's a per-key requirement
                per_key_reqs.update((kk, v) for kk in cls._expand_key(k))
            else:
                # It's a global requirement
                global_reqs.update((kk, {k: v}) for kk in all_keys)

        if global_reqs and per_key_reqs:
            raise ValueError(
                "cannot have both per-key and all-key requirements "
                "in resources dict %r" % (resources,)
            )
        return global_reqs or per_key_reqs

    @classmethod
    def get_restrictions(cls, collections, workers, allow_other_workers):
        """ Get restrictions from inputs to compute/persist """
        if isinstance(workers, (str, tuple, list)):
            workers = {tuple(collections): workers}
        if isinstance(workers, dict):
            restrictions = {}
            for colls, ws in workers.items():
                if isinstance(ws, str):
                    ws = [ws]
                if dask.is_dask_collection(colls):
                    keys = flatten(colls.__dask_keys__())
                else:
                    keys = list(
                        {k for c in flatten(colls) for k in flatten(c.__dask_keys__())}
                    )
                restrictions.update({k: ws for k in keys})
        else:
            restrictions = {}

        if allow_other_workers is True:
            loose_restrictions = list(restrictions)
        elif allow_other_workers:
            loose_restrictions = list(
                {k for c in flatten(allow_other_workers) for k in c.__dask_keys__()}
            )
        else:
            loose_restrictions = []

        return restrictions, loose_restrictions

    @staticmethod
    def collections_to_dsk(collections, *args, **kwargs):
        return collections_to_dsk(collections, *args, **kwargs)

    def get_task_stream(
        self, start=None, stop=None, count=None, plot=False, filename="task-stream.html"
    ):
        """ Get task stream data from scheduler

        This collects the data present in the diagnostic "Task Stream" plot on
        the dashboard.  It includes the start, stop, transfer, and
        deserialization time of every task for a particular duration.

        Note that the task stream diagnostic does not run by default.  You may
        wish to call this function once before you start work to ensure that
        things start recording, and then again after you have completed.

        Parameters
        ----------
        start: Number or string
            When you want to start recording
            If a number it should be the result of calling time()
            If a string then it should be a time difference before now,
            like '60s' or '500 ms'
        stop: Number or string
            When you want to stop recording
        count: int
            The number of desired records, ignored if both start and stop are
            specified
        plot: boolean, str
            If true then also return a Bokeh figure
            If plot == 'save' then save the figure to a file
        filename: str (optional)
            The filename to save to if you set ``plot='save'``

        Examples
        --------
        >>> client.get_task_stream()  # prime plugin if not already connected
        >>> x.compute()  # do some work
        >>> client.get_task_stream()
        [{'task': ...,
          'type': ...,
          'thread': ...,
          ...}]

        Pass the ``plot=True`` or ``plot='save'`` keywords to get back a Bokeh
        figure

        >>> data, figure = client.get_task_stream(plot='save', filename='myfile.html')

        Alternatively consider the context manager

        >>> from dask.distributed import get_task_stream
        >>> with get_task_stream() as ts:
        ...     x.compute()
        >>> ts.data
        [...]

        Returns
        -------
        L: List[Dict]

        See Also
        --------
        get_task_stream: a context manager version of this method
        """
        return self.sync(
            self._get_task_stream,
            start=start,
            stop=stop,
            count=count,
            plot=plot,
            filename=filename,
        )

    @gen.coroutine
    def _get_task_stream(
        self, start=None, stop=None, count=None, plot=False, filename="task-stream.html"
    ):
        msgs = yield self.scheduler.get_task_stream(start=start, stop=stop, count=count)
        if plot:
            from .diagnostics.task_stream import rectangles

            rects = rectangles(msgs)
            from .bokeh.components import task_stream_figure

            source, figure = task_stream_figure(sizing_mode="stretch_both")
            source.data.update(rects)
            if plot == "save":
                from bokeh.plotting import save

                save(figure, title="Dask Task Stream", filename=filename)
            raise gen.Return((msgs, figure))
        else:
            raise gen.Return(msgs)

    @gen.coroutine
    def _register_worker_callbacks(self, setup=None):
        responses = yield self.scheduler.register_worker_callbacks(setup=dumps(setup))
        results = {}
        for key, resp in responses.items():
            if resp["status"] == "OK":
                results[key] = resp["result"]
            elif resp["status"] == "error":
                six.reraise(*clean_exception(**resp))
        raise gen.Return(results)

    def register_worker_callbacks(self, setup=None):
        """
        Registers a setup callback function for all current and future workers.

        This registers a new setup function for workers in this cluster. The
        function will run immediately on all currently connected workers. It
        will also be run upon connection by any workers that are added in the
        future. Multiple setup functions can be registered - these will be
        called in the order they were added.

        If the function takes an input argument named ``dask_worker`` then
        that variable will be populated with the worker itself.

        Parameters
        ----------
        setup : callable(dask_worker: Worker) -> None
            Function to register and run on all workers
        """
        return self.sync(self._register_worker_callbacks, setup=setup)


class Executor(Client):
    """ Deprecated: see Client """

    def __init__(self, *args, **kwargs):
        warnings.warn("Executor has been renamed to Client")
        super(Executor, self).__init__(*args, **kwargs)


def CompatibleExecutor(*args, **kwargs):
    raise Exception("This has been moved to the Client.get_executor() method")


ALL_COMPLETED = "ALL_COMPLETED"
FIRST_COMPLETED = "FIRST_COMPLETED"


@gen.coroutine
def _wait(fs, timeout=None, return_when=ALL_COMPLETED):
    if timeout is not None and not isinstance(timeout, Number):
        raise TypeError(
            "timeout= keyword received a non-numeric value.\n"
            "Beware that wait expects a list of values\n"
            "  Bad:  wait(x, y, z)\n"
            "  Good: wait([x, y, z])"
        )
    fs = futures_of(fs)
    if return_when == ALL_COMPLETED:
        wait_for = All
    elif return_when == FIRST_COMPLETED:
        wait_for = Any
    else:
        raise NotImplementedError(
            "Only return_when='ALL_COMPLETED' and 'FIRST_COMPLETED' are " "supported"
        )

    future = wait_for({f._state.wait() for f in fs})
    if timeout is not None:
        future = gen.with_timeout(timedelta(seconds=timeout), future)
    yield future

    done, not_done = (
        {fu for fu in fs if fu.status != "pending"},
        {fu for fu in fs if fu.status == "pending"},
    )
    cancelled = [f.key for f in done if f.status == "cancelled"]
    if cancelled:
        raise CancelledError(cancelled)

    raise gen.Return(DoneAndNotDoneFutures(done, not_done))


def wait(fs, timeout=None, return_when=ALL_COMPLETED):
    """ Wait until all/any futures are finished

    Parameters
    ----------
    fs: list of futures
    timeout: number, optional
        Time in seconds after which to raise a ``dask.distributed.TimeoutError``
    -------
    Named tuple of completed, not completed
    """
    client = default_client()
    result = client.sync(_wait, fs, timeout=timeout, return_when=return_when)
    return result


@gen.coroutine
def _as_completed(fs, queue):
    fs = futures_of(fs)
    groups = groupby(lambda f: f.key, fs)
    firsts = [v[0] for v in groups.values()]
    wait_iterator = gen.WaitIterator(*[f._state.wait() for f in firsts])

    while not wait_iterator.done():
        yield wait_iterator.next()
        # TODO: handle case of restarted futures
        future = firsts[wait_iterator.current_index]
        for f in groups[future.key]:
            queue.put_nowait(f)


@gen.coroutine
def _first_completed(futures):
    """ Return a single completed future

    See Also:
        _as_completed
    """
    q = Queue()
    yield _as_completed(futures, q)
    result = yield q.get()
    raise gen.Return(result)


class as_completed(object):
    """
    Return futures in the order in which they complete

    This returns an iterator that yields the input future objects in the order
    in which they complete.  Calling ``next`` on the iterator will block until
    the next future completes, irrespective of order.

    Additionally, you can also add more futures to this object during
    computation with the ``.add`` method

    Parameters
    ----------
    futures: Collection of futures
        A list of Future objects to be iterated over in the order in which they
        complete
    with_results: bool (False)
        Whether to wait and include results of futures as well;
        in this case `as_completed` yields a tuple of (future, result)
    raise_errors: bool (True)
        Whether we should raise when the result of a future raises an exception;
        only affects behavior when `with_results=True`.

    Examples
    --------
    >>> x, y, z = client.map(inc, [1, 2, 3])  # doctest: +SKIP
    >>> for future in as_completed([x, y, z]):  # doctest: +SKIP
    ...     print(future.result())  # doctest: +SKIP
    3
    2
    4

    Add more futures during computation

    >>> x, y, z = client.map(inc, [1, 2, 3])  # doctest: +SKIP
    >>> ac = as_completed([x, y, z])  # doctest: +SKIP
    >>> for future in ac:  # doctest: +SKIP
    ...     print(future.result())  # doctest: +SKIP
    ...     if random.random() < 0.5:  # doctest: +SKIP
    ...         ac.add(c.submit(double, future))  # doctest: +SKIP
    4
    2
    8
    3
    6
    12
    24

    Optionally wait until the result has been gathered as well

    >>> ac = as_completed([x, y, z], with_results=True)  # doctest: +SKIP
    >>> for future, result in ac:  # doctest: +SKIP
    ...     print(result)  # doctest: +SKIP
    2
    4
    3
    """

    def __init__(self, futures=None, loop=None, with_results=False, raise_errors=True):
        if futures is None:
            futures = []
        self.futures = defaultdict(lambda: 0)
        self.queue = pyQueue()
        self.lock = threading.Lock()
        self.loop = loop or default_client().loop
        self.condition = Condition()
        self.thread_condition = threading.Condition()
        self.with_results = with_results
        self.raise_errors = raise_errors

        if futures:
            self.update(futures)

    def _notify(self):
        self.condition.notify()
        with self.thread_condition:
            self.thread_condition.notify()

    @gen.coroutine
    def _track_future(self, future):
        try:
            yield _wait(future)
        except CancelledError:
            pass
        if self.with_results:
            result = yield future._result(raiseit=False)
        with self.lock:
            self.futures[future] -= 1
            if not self.futures[future]:
                del self.futures[future]
            if self.with_results:
                self.queue.put_nowait((future, result))
            else:
                self.queue.put_nowait(future)
            self._notify()

    def update(self, futures):
        """ Add multiple futures to the collection.

        The added futures will emit from the iterator once they finish"""
        with self.lock:
            for f in futures:
                if not isinstance(f, Future):
                    raise TypeError("Input must be a future, got %s" % f)
                self.futures[f] += 1
                self.loop.add_callback(self._track_future, f)

    def add(self, future):
        """ Add a future to the collection

        This future will emit from the iterator once it finishes
        """
        self.update((future,))

    def is_empty(self):
        """Returns True if there no completed or computing futures"""
        return not self.count()

    def has_ready(self):
        """Returns True if there are completed futures available."""
        return not self.queue.empty()

    def count(self):
        """ Return the number of futures yet to be returned

        This includes both the number of futures still computing, as well as
        those that are finished, but have not yet been returned from this
        iterator.
        """
        with self.lock:
            return len(self.futures) + len(self.queue.queue)

    def __iter__(self):
        return self

    def __aiter__(self):
        return self

    def _get_and_raise(self):
        res = self.queue.get()
        if self.with_results:
            future, result = res
            if self.raise_errors and future.status == "error":
                six.reraise(*result)
        return res

    def __next__(self):
        while self.queue.empty():
            if self.is_empty():
                raise StopIteration()
            with self.thread_condition:
                self.thread_condition.wait(timeout=0.100)
        return self._get_and_raise()

    @gen.coroutine
    def __anext__(self):
        if not self.futures and self.queue.empty():
            raise StopAsyncIteration
        while self.queue.empty():
            if not self.futures:
                raise StopAsyncIteration
            yield self.condition.wait()

        raise gen.Return(self._get_and_raise())

    next = __next__

    def next_batch(self, block=True):
        """ Get the next batch of completed futures.

        Parameters
        ----------
        block: bool, optional
            If True then wait until we have some result, otherwise return
            immediately, even with an empty list.  Defaults to True.

        Examples
        --------
        >>> ac = as_completed(futures)  # doctest: +SKIP
        >>> client.gather(ac.next_batch())  # doctest: +SKIP
        [4, 1, 3]

        >>> client.gather(ac.next_batch(block=False))  # doctest: +SKIP
        []

        Returns
        -------
        List of futures or (future, result) tuples
        """
        if block:
            batch = [next(self)]
        else:
            batch = []
        while not self.queue.empty():
            batch.append(self.queue.get())
        return batch

    def batches(self):
        """
        Yield all finished futures at once rather than one-by-one

        This returns an iterator of lists of futures or lists of
        (future, result) tuples rather than individual futures or individual
        (future, result) tuples.  It will yield these as soon as possible
        without waiting.

        Examples
        --------
        >>> for batch in as_completed(futures).batches():  # doctest: +SKIP
        ...     results = client.gather(batch)
        ...     print(results)
        [4, 2]
        [1, 3, 7]
        [5]
        [6]
        """
        while True:
            try:
                yield self.next_batch(block=True)
            except StopIteration:
                return


def AsCompleted(*args, **kwargs):
    raise Exception("This has moved to as_completed")


def default_client(c=None):
    """ Return a client if one has started """
    c = c or _get_global_client()
    if c:
        return c
    else:
        raise ValueError(
            "No clients found\n"
            "Start an client and point it to the scheduler address\n"
            "  from distributed import Client\n"
            "  client = Client('ip-addr-of-scheduler:8786')\n"
        )


def ensure_default_get(client):
    dask.config.set(scheduler="dask.distributed")
    _set_global_client(client)


def redict_collection(c, dsk):
    from dask.delayed import Delayed

    if isinstance(c, Delayed):
        return Delayed(c.key, dsk)
    else:
        cc = copy.copy(c)
        cc.dask = dsk
        return cc


def futures_of(o, client=None):
    """ Future objects in a collection """
    stack = [o]
    futures = set()
    while stack:
        x = stack.pop()
        if type(x) in (tuple, set, list):
            stack.extend(x)
        elif type(x) is dict:
            stack.extend(x.values())
        elif type(x) is SubgraphCallable:
            stack.extend(x.dsk.values())
        elif isinstance(x, Future):
            futures.add(x)
        elif dask.is_dask_collection(x):
            stack.extend(x.__dask_graph__().values())

    if client is not None:
        bad = {f for f in futures if f.cancelled()}
        if bad:
            raise CancelledError(bad)

    return list(futures)


def fire_and_forget(obj):
    """ Run tasks at least once, even if we release the futures

    Under normal operation Dask will not run any tasks for which there is not
    an active future (this avoids unnecessary work in many situations).
    However sometimes you want to just fire off a task, not track its future,
    and expect it to finish eventually.  You can use this function on a future
    or collection of futures to ask Dask to complete the task even if no active
    client is tracking it.

    The results will not be kept in memory after the task completes (unless
    there is an active future) so this is only useful for tasks that depend on
    side effects.

    Parameters
    ----------
    obj: Future, list, dict, dask collection
        The futures that you want to run at least once

    Examples
    --------
    >>> fire_and_forget(client.submit(func, *args))  # doctest: +SKIP
    """
    futures = futures_of(obj)
    for future in futures:
        future.client._send_to_scheduler(
            {
                "op": "client-desires-keys",
                "keys": [tokey(future.key)],
                "client": "fire-and-forget",
            }
        )


class get_task_stream(object):
    """
    Collect task stream within a context block

    This provides diagnostic information about every task that was run during
    the time when this block was active.

    This must be used as a context manager.

    Parameters
    ----------
    plot: boolean, str
        If true then also return a Bokeh figure
        If plot == 'save' then save the figure to a file
    filename: str (optional)
        The filename to save to if you set ``plot='save'``

    Examples
    --------
    >>> with get_task_stream() as ts:
    ...     x.compute()
    >>> ts.data
    [...]

    Get back a Bokeh figure and optionally save to a file

    >>> with get_task_stream(plot='save', filename='task-stream.html') as ts:
    ...    x.compute()
    >>> ts.figure
    <Bokeh Figure>

    To share this file with others you may wish to upload and serve it online.
    A common way to do this is to upload the file as a gist, and then serve it
    on https://rawgit.com ::

       $ pip install gist
       $ gist task-stream.html
       https://gist.github.com/8a5b3c74b10b413f612bb5e250856ceb

    You can then navigate to that site, click the "Raw" button to the right of
    the ``task-stream.html`` file, and then provide that URL to
    https://rawgit.com .  This process should provide a sharable link that
    others can use to see your task stream plot.

    See Also
    --------
    Client.get_task_stream: Function version of this context manager
    """

    def __init__(self, client=None, plot=False, filename="task-stream.html"):
        self.data = []
        self._plot = plot
        self._filename = filename
        self.figure = None
        self.client = client or default_client()
        self.client.get_task_stream(start=0, stop=0)  # ensure plugin

    def __enter__(self):
        self.start = time()
        return self

    def __exit__(self, typ, value, traceback):
        L = self.client.get_task_stream(
            start=self.start, plot=self._plot, filename=self._filename
        )
        if self._plot:
            L, self.figure = L
        self.data.extend(L)

    @gen.coroutine
    def __aenter__(self):
        raise gen.Return(self)

    @gen.coroutine
    def __aexit__(self, typ, value, traceback):
        L = yield self.client.get_task_stream(
            start=self.start, plot=self._plot, filename=self._filename
        )
        if self._plot:
            L, self.figure = L
        self.data.extend(L)


@contextmanager
def temp_default_client(c):
    """ Set the default client for the duration of the context

    Parameters
    ----------
    c : Client
        This is what default_client() will return within the with-block.
    """
    old_exec = default_client()
    _set_global_client(c)
    try:
        yield
    finally:
        _set_global_client(old_exec)


def _close_global_client():
    """
    Force close of global client.  This cleans up when a client
    wasn't close explicitly, e.g. interactive sessions.
    """
    c = _get_global_client()
    if c is not None:
        c._should_close_loop = False
        c.close(timeout=2)


atexit.register(_close_global_client)
