from tornado import gen
import functools

from .client import Future, default_client
from .compatibility import get_thread_identity, Queue
from .protocol import to_serialize
from .utils import sync
from .utils_comm import WrappedKey
from .worker import get_worker


class Actor(WrappedKey):
    """ Controls an object on a remote worker

    An actor allows remote control of a stateful object living on a remote
    worker.  Method calls on this object trigger operations on the remote
    object and return ActorFutures on which we can block to get results.

    Examples
    --------
    >>> class Counter:
    ...    def __init__(self):
    ...        self.n = 0
    ...    def increment(self):
    ...        self.n += 1
    ...        return self.n

    >>> from dask.distributed import Client
    >>> client = Client()

    You can create an actor by submitting a class with the keyword
    ``actor=True``.

    >>> future = client.submit(Counter, actor=True)
    >>> counter = future.result()
    >>> counter
    <Actor: Counter, key=Counter-1234abcd>

    Calling methods on this object immediately returns deferred ``ActorFuture``
    objects.  You can call ``.result()`` on these objects to block and get the
    result of the function call.

    >>> future = counter.increment()
    >>> future.result()
    1
    >>> future = counter.increment()
    >>> future.result()
    2
    """

    def __init__(self, cls, address, key, worker=None):
        self._cls = cls
        self._address = address
        self.key = key
        self._future = None
        if worker:
            self._worker = worker
            self._client = None
        else:
            try:
                self._worker = get_worker()
            except ValueError:
                self._worker = None
            try:
                self._client = default_client()
                self._future = Future(key)
            except ValueError:
                self._client = None

    def __repr__(self):
        return "<Actor: %s, key=%s>" % (self._cls.__name__, self.key)

    def __reduce__(self):
        return (Actor, (self._cls, self._address, self.key))

    @property
    def _io_loop(self):
        if self._worker:
            return self._worker.io_loop
        else:
            return self._client.io_loop

    @property
    def _scheduler_rpc(self):
        if self._worker:
            return self._worker.scheduler
        else:
            return self._client.scheduler

    @property
    def _worker_rpc(self):
        if self._worker:
            return self._worker.rpc(self._address)
        else:
            if self._client.direct_to_workers:
                return self._client.rpc(self._address)
            else:
                return ProxyRPC(self._client.scheduler, self._address)

    @property
    def _asynchronous(self):
        if self._client:
            return self._client.asynchronous
        else:
            return get_thread_identity() == self._worker.thread_id

    def _sync(self, func, *args, **kwargs):
        if self._client:
            return self._client.sync(func, *args, **kwargs)
        else:
            # TODO support sync operation by checking against thread ident of loop
            return sync(self._worker.loop, func, *args, **kwargs)

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(attr for attr in dir(self._cls) if not attr.startswith("_"))
        return sorted(o)

    def __getattr__(self, key):
        attr = getattr(self._cls, key)

        if self._future and self._future.status not in ("finished", "pending"):
            raise ValueError(
                "Worker holding Actor was lost.  Status: " + self._future.status
            )

        if callable(attr):

            @functools.wraps(attr)
            def func(*args, **kwargs):
                @gen.coroutine
                def run_actor_function_on_worker():
                    try:
                        result = yield self._worker_rpc.actor_execute(
                            function=key,
                            actor=self.key,
                            args=[to_serialize(arg) for arg in args],
                            kwargs={k: to_serialize(v) for k, v in kwargs.items()},
                        )
                    except OSError:
                        if self._future:
                            yield self._future
                        else:
                            raise OSError("Unable to contact Actor's worker")
                    raise gen.Return(result["result"])

                if self._asynchronous:
                    return run_actor_function_on_worker()
                else:
                    # TODO: this mechanism is error prone
                    # we should endeavor to make dask's standard code work here
                    q = Queue()

                    @gen.coroutine
                    def wait_then_add_to_queue():
                        x = yield run_actor_function_on_worker()
                        q.put(x)

                    self._io_loop.add_callback(wait_then_add_to_queue)

                    return ActorFuture(q, self._io_loop)

            return func

        else:

            @gen.coroutine
            def get_actor_attribute_from_worker():
                x = yield self._worker_rpc.actor_attribute(
                    attribute=key, actor=self.key
                )
                raise gen.Return(x["result"])

            return self._sync(get_actor_attribute_from_worker)

    @property
    def client(self):
        return self._future.client


class ProxyRPC(object):
    """
    An rpc-like object that uses the scheduler's rpc to connect to a worker
    """

    def __init__(self, rpc, address):
        self.rpc = rpc
        self._address = address

    def __getattr__(self, key):
        @gen.coroutine
        def func(**msg):
            msg["op"] = key
            result = yield self.rpc.proxy(worker=self._address, msg=msg)
            raise gen.Return(result)

        return func


class ActorFuture(object):
    """ Future to an actor's method call

    Whenever you call a method on an Actor you get an ActorFuture immediately
    while the computation happens in the background.  You can call ``.result``
    to block and collect the full result

    See Also
    --------
    Actor
    """

    def __init__(self, q, io_loop):
        self.q = q
        self.io_loop = io_loop

    def result(self, timeout=None):
        try:
            return self._cached_result
        except AttributeError:
            self._cached_result = self.q.get(timeout=timeout)
            return self._cached_result

    def __repr__(self):
        return "<ActorFuture>"
