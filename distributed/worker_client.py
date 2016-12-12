from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
from tornado import gen
from toolz import keymap, valmap, merge
import uuid
import uuid

from dask.base import tokenize
from tornado import gen

from .client import AllExit, Client, Future, pack_data, unpack_remotedata
from .sizeof import sizeof
from .threadpoolexecutor import secede
from .utils import All, log_errors, sync, tokey, ignoring
from .worker import thread_state


@contextmanager
def local_client():
    """ Get client for this thread

    Note: This interface is new and experimental.  It may change without
    notice.

    This context manager is intended to be called within functions that we run
    on workers.  When run as a context manager it delivers a client
    ``Client`` object that can submit other tasks directly from that worker.

    Examples
    --------

    >>> def func(x):
    ...     with local_client() as e:  # connect from worker back to scheduler
    ...         a = e.submit(inc, x)     # this task can submit more tasks
    ...         b = e.submit(dec, x)
    ...         result = e.gather([a, b])  # and gather results
    ...     return result

    >>> future = e.submit(func, 1)  # submit func(1) on cluster
    """
    address = thread_state.execution_state['scheduler']
    worker = thread_state.execution_state['worker']
    secede()  # have this thread secede from the thread pool
              # so that it doesn't take up a fixed resource while waiting
    worker.loop.add_callback(worker.transition, thread_state.key, 'long-running')
    with WorkerClient(address) as e:
        yield e


def get_worker():
    return thread_state.execution_state['worker']


class WorkerClient(Client):
    """ An Client designed to operate from a Worker process

    This client has had a few methods altered to make it more efficient for
    working directly from the worker nodes.  In particular scatter/gather first
    look to the local data dictionary rather than sending data over the network
    """
    def __init__(self, *args, **kwargs):
        self.worker = get_worker()
        Client.__init__(self, *args, **kwargs)

    @gen.coroutine
    def _scatter(self, data, workers=None, broadcast=False):
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

            if isinstance(data, (list, tuple, set, frozenset)):
                keys = []
                for x in data:
                    try:
                        keys.append(tokenize(x))
                    except:
                        keys.append(str(uuid.uuid1()))
                data2 = dict(zip(keys, data))
            elif isinstance(data, dict):
                keys = set(data)
                data2 = data
            else:
                raise TypeError("Don't know how to scatter %s" % type(data))

            nbytes = valmap(sizeof, data2)

            # self.worker.data.update(data2)  # thread safety matters
            self.worker.loop.add_callback(self.worker.data.update, data2)

            yield self.scheduler.update_data(
                    who_has={key: [self.worker.address] for key in data2},
                    nbytes=valmap(sizeof, data2),
                    client=self.id)

            if isinstance(data, dict):
                out = {k: Future(k, self) for k in data}
            elif isinstance(data, (tuple, list, set, frozenset)):
                out = type(data)([Future(k, self) for k in keys])
            else:
                raise TypeError(
                        "Input to scatter must be a list or dict")

            for key in keys:
                self.futures[key].finish(type=None)

            raise gen.Return(out)

    @gen.coroutine
    def _gather(self, futures, errors='raise'):
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

        while True:  # not threadsafe, so try until success
            try:
                local = {k: self.worker.data[k] for k in keys
                         if k in self.worker.data}
                break
            except KeyError as e:
                pass

        futures3 = {k: Future(k, self) for k in keys if k not in local}

        futures4 = pack_data(futures2, merge(local, futures3))
        if not futures3:
            raise gen.Return(futures4)

        result = yield Client._gather(self, futures4, errors=errors)
        raise gen.Return(result)
