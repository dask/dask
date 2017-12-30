from __future__ import print_function, division, absolute_import

import concurrent.futures as cf
import weakref

import six

from toolz import merge

from tornado import gen

from .metrics import time
from .utils import sync


@gen.coroutine
def _cascade_future(future, cf_future):
    """
    Coroutine that waits on Dask future, then transmits its outcome to
    cf_future.
    """
    result = yield future._result(raiseit=False)
    status = future.status
    if status == 'finished':
        cf_future.set_result(result)
    elif status == 'cancelled':
        cf_future.cancel()
        # Necessary for wait() and as_completed() to wake up
        cf_future.set_running_or_notify_cancel()
    else:
        try:
            six.reraise(*result)
        except BaseException as exc:
            cf_future.set_exception(exc)


@gen.coroutine
def _wait_on_futures(futures):
    for fut in futures:
        try:
            yield fut
        except Exception:
            pass


class ClientExecutor(cf.Executor):
    """
    A concurrent.futures Executor that executes tasks on a dask.distributed Client.
    """

    _allowed_kwargs = frozenset(['pure', 'workers', 'resources', 'allow_other_workers', 'retries'])

    def __init__(self, client, **kwargs):
        sk = set(kwargs)
        if not sk <= self._allowed_kwargs:
            raise TypeError("unsupported arguments to ClientExecutor: %s"
                            % sorted(sk - self._allowed_kwargs))
        self._client = client
        self._futures = weakref.WeakSet()
        self._shutdown = False
        self._kwargs = kwargs

    def _wrap_future(self, future):
        """
        Wrap a distributed Future in a concurrent.futures Future.
        """
        cf_future = cf.Future()

        # Support cancelling task through .cancel() on c.f.Future
        def cf_callback(cf_future):
            if cf_future.cancelled() and future.status != 'cancelled':
                future.cancel()

        cf_future.add_done_callback(cf_callback)

        self._client.loop.add_callback(_cascade_future, future, cf_future)
        return cf_future

    def submit(self, fn, *args, **kwargs):
        """Submits a callable to be executed with the given arguments.

        Schedules the callable to be executed as ``fn(*args, **kwargs)``
        and returns a Future instance representing the execution of the callable.

        Returns
        -------
        A Future representing the given call.
        """
        if self._shutdown:
            raise RuntimeError('cannot schedule new futures after shutdown')
        future = self._client.submit(fn, *args, **merge(self._kwargs, kwargs))
        self._futures.add(future)
        return self._wrap_future(future)

    def map(self, fn, *iterables, **kwargs):
        """Returns an iterator equivalent to ``map(fn, *iterables)``.

        Parameters
        ----------
        fn: A callable that will take as many arguments as there are
            passed iterables.
        iterables: One iterable for each parameter to *fn*.
        timeout: The maximum number of seconds to wait. If None, then there
            is no limit on the wait time.
        chunksize: ignored.

        Returns
        -------
        An iterator equivalent to: ``map(fn, *iterables)`` but the calls may
        be evaluated out-of-order.

        Raises
        ------
        TimeoutError: If the entire result iterator could not be generated
            before the given timeout.
        Exception: If ``fn(*args)`` raises for any values.
        """
        timeout = kwargs.pop('timeout', None)
        if timeout is not None:
            end_time = timeout + time()
        if 'chunksize' in kwargs:
            del kwargs['chunksize']
        if kwargs:
            raise TypeError("unexpected arguments to map(): %s"
                            % sorted(kwargs))

        fs = self._client.map(fn, *iterables, **self._kwargs)

        # Yield must be hidden in closure so that the tasks are submitted
        # before the first iterator value is required.
        def result_iterator():
            try:
                for future in fs:
                    self._futures.add(future)
                    if timeout is not None:
                        try:
                            yield future.result(end_time - time())
                        except gen.TimeoutError:
                            raise cf.TimeoutError
                    else:
                        yield future.result()
            finally:
                remaining = list(fs)
                for future in remaining:
                    self._futures.add(future)
                self._client.cancel(remaining)

        return result_iterator()

    def shutdown(self, wait=True):
        """Clean-up the resources associated with the Executor.

        It is safe to call this method several times. Otherwise, no other
        methods can be called after this one.

        Parameters
        ----------
        wait: If True then shutdown will not return until all running
            futures have finished executing.  If False then all running
            futures are cancelled immediately.
        """
        if not self._shutdown:
            self._shutdown = True
            fs = list(self._futures)
            if wait:
                sync(self._client.loop, _wait_on_futures, fs)
            else:
                self._client.cancel(fs)
