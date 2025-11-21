from __future__ import annotations

import contextlib
import warnings

import dask

from distributed.metrics import time
from distributed.threadpoolexecutor import rejoin, secede
from distributed.worker import get_client, get_worker, thread_state
from distributed.worker_state_machine import SecedeEvent


@contextlib.contextmanager
def worker_client(timeout=None, separate_thread=True):
    """Get client for this thread

    This context manager is intended to be called within functions that we run
    on workers.  When run as a context manager it delivers a client
    ``Client`` object that can submit other tasks directly from that worker.

    Parameters
    ----------
    timeout : Number or String
        Timeout after which to error out. Defaults to the
        ``distributed.comm.timeouts.connect`` configuration value.
    separate_thread : bool, optional
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

    if timeout is None:
        timeout = dask.config.get("distributed.comm.timeouts.connect")

    timeout = dask.utils.parse_timedelta(timeout, "s")

    worker = get_worker()
    client = get_client(timeout=timeout)
    worker.log_event(
        "worker-client",
        {
            "client": client.id,
            "timeout": timeout,
        },
    )
    with contextlib.ExitStack() as stack:
        if separate_thread:
            try:
                thread_state.start_time
            except AttributeError:  # not in a synchronous task, can't secede
                pass
            else:
                duration = time() - thread_state.start_time
                try:
                    secede()  # have this thread secede from the thread pool
                except KeyError:  # already seceded
                    pass
                else:
                    stack.callback(rejoin)
                    worker.loop.add_callback(
                        worker.handle_stimulus,
                        SecedeEvent(
                            key=thread_state.key,
                            compute_duration=duration,
                            stimulus_id=f"worker-client-secede-{time()}",
                        ),
                    )

        yield client


def local_client(*args, **kwargs):
    warnings.warn("local_client has moved to worker_client")
    return worker_client(*args, **kwargs)
