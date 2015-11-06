from __future__ import print_function, division, absolute_import

import logging

from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop

from dask.async import nested_get
from dask.core import flatten

from .core import rpc
from .client import RemoteData, _gather
from .utils import All
from .scheduler import scheduler, delete, worker


logger = logging.getLogger(__name__)


@gen.coroutine
def _get(ip, port, dsk, result, gather=False):
    center = rpc(ip=ip, port=port)
    who_has, has_what, ncores = yield [center.who_has(),
                                       center.has_what(),
                                       center.ncores()]
    workers = sorted(ncores)

    worker_queues = {worker: Queue() for worker in workers}
    scheduler_queue = Queue()
    delete_queue = Queue()
    report_queue = Queue()

    coroutines = ([report(report_queue, scheduler_queue, who_has, dsk, result),
                   scheduler(scheduler_queue, report_queue, worker_queues, delete_queue,
                             who_has, has_what, ncores),
                   delete(scheduler_queue, delete_queue, ip, port)]
                + [worker(scheduler_queue, worker_queues[w], w, ncores[w])
                   for w in workers])

    results = yield All(coroutines)
    out_keys = results[0]

    if gather:
        out_data = yield _gather(center, out_keys)
        d = dict(zip(out_keys, out_data))
    else:
        d = {key: RemoteData(key, ip, port) for key in out_keys}

    raise Return(nested_get(result, d))


def get(ip, port, dsk, keys, gather=True, _get=_get):
    """ Distributed dask scheduler

    This uses a distributed network of Center and Worker nodes.

    Parameters
    ----------
    ip/port:
        address of center
    dsk/result:
        normal graph/keys inputs to dask.get
    gather: bool
        Collect distributed results from cluster.  If False then return
        RemoteData objects.

    Examples
    --------
    >>> inc = lambda x: x + 1
    >>> dsk = {'x': 1, 'y': (inc, 'x')}
    >>> get('127.0.0.1', 8787, dsk, 'y')  # doctest: +SKIP

    Use with dask collections by partialing in the center ip/port

    >>> from functools import partial
    >>> myget = partial(get, '127.0.0.1', 8787)
    >>> import dask.array as da  # doctest: +SKIP
    >>> x = da.ones((1000, 1000), chunks=(100, 100))  # doctest: +SKIP
    >>> x.sum().compute(get=myget)  # doctest: +SKIP
    1000000
    """
    return IOLoop().run_sync(lambda: _get(ip, port, dsk, keys, gather))


@gen.coroutine
def report(report_queue, scheduler_queue, who_has, dsk, result):
    """ Report to outside world

    For a normal get function this coroutine is almost non-essential.
    It just starts and stops the scheduler coroutine.
    """
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    out_keys = set(result_flat)

    scheduler_queue.put_nowait({'op': 'update-graph',
                                'dsk': dsk,
                                'keys': out_keys})

    finished_results = {k for k in out_keys if k in who_has}

    while finished_results != out_keys:
        msg = yield report_queue.get()
        if msg['op'] == 'key-in-memory':
            if msg['key'] in out_keys:
                finished_results.add(msg['key'])
        if msg['op'] == 'lost-data':
            if msg['key'] in finished_results:
                finished_results.remove(msg['key'])
        if msg['op'] == 'task-erred':
            scheduler_queue.put_nowait({'op': 'close'})
            raise msg['exception']
    scheduler_queue.put_nowait({'op': 'close'})

    raise Return(out_keys)
