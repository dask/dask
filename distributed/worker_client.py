from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from datetime import timedelta
from toolz import keymap, valmap, merge

from dask.base import tokenize
from tornado import gen

from .client import AllExit, Client, Future, pack_data, unpack_remotedata
from dask.compatibility import apply
from .sizeof import sizeof
from .threadpoolexecutor import secede
from .utils import All, log_errors, sync, tokey, ignoring
from .worker import thread_state, get_worker


@contextmanager
def worker_client(timeout=3, separate_thread=True):
    """ Get client for this thread

    This context manager is intended to be called within functions that we run
    on workers.  When run as a context manager it delivers a client
    ``Client`` object that can submit other tasks directly from that worker.

    Parameters
    ----------
    timeout: Number
        Timeout after which to err
    separate_thread: bool, optional
        Whether to run this function outside of the normal thread pool
        defaults to True

    Examples
    --------
    >>> def func(x):
    ...     with worker_client() as c:  # connect from worker back to scheduler
    ...         a = c.submit(inc, x)     # this task can submit more tasks
    ...         b = c.submit(dec, x)
    ...         result = c.gather([a, b])  # and gather results
    ...     return result

    >>> future = client.submit(func, 1)  # submit func(1) on cluster

    See Also
    --------
    get_worker
    """
    address = thread_state.execution_state['scheduler']
    worker = thread_state.execution_state['worker']
    if separate_thread:
        secede()  # have this thread secede from the thread pool
        worker.loop.add_callback(worker.transition, thread_state.key, 'long-running')

    with WorkerClient(address, loop=worker.loop, security=worker.security,
                      asynchronous=True) as wc:
        # Make sure connection errors are bubbled to the caller
        sync(wc.loop, gen.with_timeout, timedelta(seconds=timeout), wc._started)
        wc.asynchronous = False
        assert wc.status == 'running'
        yield wc


local_client = worker_client


class WorkerClient(Client):
    """ An Client designed to operate from a Worker process

    This client has had a few methods altered to make it more efficient for
    working directly from the worker nodes.  In particular scatter/gather first
    look to the local data dictionary rather than sending data over the network
    """
    def __init__(self, *args, **kwargs):
        loop = kwargs.get('loop')
        self.worker = get_worker()
        kwargs['set_as_default'] = False
        sync(loop, apply, Client.__init__, (self,) + args, kwargs)

    @gen.coroutine
    def _scatter(self, data, workers=None, broadcast=False, direct=None):
        """ Scatter data to local data dictionary

        Rather than send data out to the cluster we keep data local.  However
        we do report to the scheduler that the local worker has the scattered
        data.  This allows other workers to come by and steal this data if
        desired.

        Keywords like ``broadcast=`` do not work, however operations like
        ``.replicate`` work fine after calling scatter, which can fill in for
        this functionality.
        """
        with log_errors():
            if not (workers is None and broadcast is False):
                raise NotImplementedError("Scatter from worker doesn't support workers or broadcast keywords")

            if isinstance(data, dict) and not all(isinstance(k, (bytes, str))
                                                   for k in data):
                d = yield self._scatter(keymap(tokey, data), workers, broadcast)
                raise gen.Return({k: d[tokey(k)] for k in data})

            if isinstance(data, type(range(0))):
                data = list(data)
            input_type = type(data)
            names = False
            unpack = False
            if isinstance(data, (set, frozenset)):
                data = list(data)
            if not isinstance(data, (dict, list, tuple, set, frozenset)):
                unpack = True
                data = [data]
            if isinstance(data, (list, tuple)):
                names = list(map(tokenize, data))
                data = dict(zip(names, data))

            types = valmap(type, data)
            assert isinstance(data, dict)

            self.worker.update_data(data=data, report=False)

            yield self.scheduler.update_data(
                    who_has={key: [self.worker.address] for key in data},
                    nbytes=valmap(sizeof, data),
                    client=self.id)

            out = {k: self._Future(k, self) for k in data}
            for key, typ in types.items():
                self.futures[key].finish(type=typ)

            if issubclass(input_type, (list, tuple, set, frozenset)):
                out = input_type(out[k] for k in names)

            if unpack:
                assert len(out) == 1
                out = list(out.values())[0]
            raise gen.Return(out)

    @gen.coroutine
    def _gather(self, futures, errors='raise', direct=False):
        """

        Exactly like Client._gather, but get data directly from the local
        worker data dictionary directly rather than through the scheduler.

        TODO: avoid scheduler for other communications, and assume that we can
        communicate directly with the other workers.
        """
        futures2, keys = unpack_remotedata(futures, byte_keys=True)
        keys = [tokey(k) for k in keys]

        @gen.coroutine
        def wait(k):
            """ Want to stop the All(...) early if we find an error """
            yield self.futures[k].event.wait()
            if self.futures[k].status != 'finished':
                raise AllExit()

        with ignoring(AllExit):
            yield All([wait(key) for key in keys if key in self.futures])

        local = {k: self.worker.data[k] for k in keys
                 if k in self.worker.data}

        futures3 = {k: Future(k, self) for k in keys if k not in local}

        futures4 = pack_data(futures2, merge(local, futures3))
        if not futures3:
            raise gen.Return(futures4)

        result = yield Client._gather(self, futures4, errors=errors,
                                      direct=True)
        raise gen.Return(result)
