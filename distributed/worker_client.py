from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
import warnings

from .threadpoolexecutor import secede, rejoin
from .worker import thread_state, get_client, get_worker


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
    get_client
    secede
    """
    worker = get_worker()
    client = get_client(timeout=timeout)
    if separate_thread:
        secede()  # have this thread secede from the thread pool
        worker.loop.add_callback(worker.transition, thread_state.key, "long-running")

    yield client

    if separate_thread:
        rejoin()


def local_client(*args, **kwargs):
    warnings.warn("local_client has moved to worker_client")
    return worker_client(*args, **kwargs)
