from __future__ import print_function, division, absolute_import

import contextlib

from distutils.version import LooseVersion
from uuid import uuid4

from tornado import gen

from .client import Client, _wait
from .utils import ignoring, funcname, itemgetter
from . import get_client, secede, rejoin

# A user could have installed joblib, sklearn, both, or neither. Further, only
# joblib >= 0.10.0 supports backends, so we also need to check for that. This
# bit of logic is to ensure that we create and register the backend for all
# viable installations of joblib.
joblib = sk_joblib = None
with ignoring(ImportError):
    import joblib
    if LooseVersion(joblib.__version__) < '0.10.2':
        joblib = None
with ignoring(ImportError):
    import sklearn.externals.joblib as sk_joblib
    if LooseVersion(sk_joblib.__version__) < '0.10.2':
        sk_joblib = None

_bases = []
if joblib:
    from joblib.parallel import AutoBatchingMixin, ParallelBackendBase
    _bases.append(ParallelBackendBase)
if sk_joblib:
    from sklearn.externals.joblib.parallel import (AutoBatchingMixin,  # noqa
            ParallelBackendBase)
    _bases.append(ParallelBackendBase)
if not _bases:
    raise RuntimeError("Joblib backend requires either `joblib` >= '0.10.2' "
                       " or `sklearn` > '0.17.1'. Please install or upgrade")


def joblib_funcname(x):
    try:
        # Can't do isinstance, since joblib is often bundled in packages, and
        # separate installs will have non-equivalent types.
        if type(x).__name__ == 'BatchedCalls':
            x = x.items[0][0]
    except Exception:
        pass
    return funcname(x)


class Batch(object):
    def __init__(self, tasks):
        self.tasks = tasks

    def __call__(self, *data):
        results = []
        for func, args, kwargs in self.tasks:
            args = [a(data) if isinstance(a, itemgetter) else a
                    for a in args]
            kwargs = {k: v(data) if isinstance(v, itemgetter) else v
                      for (k, v) in kwargs.items()}
            results.append(func(*args, **kwargs))
        return results

    def __reduce__(self):
        return (Batch, (self.tasks,))


class DaskDistributedBackend(ParallelBackendBase, AutoBatchingMixin):
    MIN_IDEAL_BATCH_DURATION = 0.2
    MAX_IDEAL_BATCH_DURATION = 1.0

    def __init__(self, scheduler_host='127.0.0.1:8786', scatter=None,
                 loop=None):
        # We may have two types of callers:
        #   1. The user, from a parallel_backend("dask.distributed", ...)
        #   2. Joblib, from within joblib.Parallel
        # We rely on the convention that 'scheduler_host=None' implies that
        # joblib is calling.
        self.scheduler_host = scheduler_host

        if scatter is not None and not isinstance(scatter, (list, tuple)):
            raise TypeError("scatter must be a list/tuple, got "
                            "`%s`" % type(scatter).__name__)

        if scatter is not None:
            # Keep a reference to the scattered data to keep the ids the same
            self._scatter = list(scatter)
            scattered = self.get_client(loop=loop).scatter(scatter,
                                                           broadcast=True)
            self.data_to_future = {id(x): f for (x, f) in zip(scatter, scattered)}
        else:
            self._scatter = []
            self.data_to_future = {}
        self.futures = set()

    def get_client(self, loop=None):
        """Get a client for our scheduler.

        If this is being called by a worker, 'distributed.worker.get_client'
        is used. Otherwise, a new client is created.
        """
        try:
            return get_client()
        except ValueError:
            return Client(self.scheduler_host, loop=loop, set_as_default=False)

    def get_nested_backend(self):
        return DaskDistributedBackend(scheduler_host=None)

    def configure(self, n_jobs=1, parallel=None, **backend_args):
        return self.effective_n_jobs(n_jobs)

    def effective_n_jobs(self, n_jobs):
        return sum(self.get_client().ncores().values())

    def _to_func_args(self, func):
        if not self.data_to_future:
            return func, ()
        args2 = []
        lookup = dict(self.data_to_future)

        def maybe_to_futures(args):
            for x in args:
                id_x = id(x)
                if id_x in lookup:
                    x = lookup[id_x]
                    if type(x) is not itemgetter:
                        x = itemgetter(len(args2))
                        args2.append(lookup[id_x])
                        lookup[id_x] = x
                yield x

        tasks = []
        for f, args, kwargs in func.items:
            args = list(maybe_to_futures(args))
            kwargs = dict(zip(kwargs.keys(), maybe_to_futures(kwargs.values())))
            tasks.append((f, args, kwargs))

        if not args2:
            return func, ()
        return Batch(tasks), args2

    def apply_async(self, func, callback=None):
        key = '%s-batch-%s' % (joblib_funcname(func), uuid4().hex)
        func, args = self._to_func_args(func)
        client = self.get_client()

        future = client.submit(func, *args, key=key)
        self.futures.add(future)

        @gen.coroutine
        def callback_wrapper():
            result = yield _wait([future])
            self.futures.remove(future)
            if callback is not None:
                callback(result)  # gets called in separate thread

        client.loop.add_callback(callback_wrapper)

        future.get = future.result  # monkey patch to achieve AsyncResult API
        return future

    def abort_everything(self, ensure_ready=True):
        # Tell the client to cancel any task submitted via this instance
        # as joblib.Parallel will never access those results.
        self.get_client().cancel(self.futures)
        self.futures.clear()

    @contextlib.contextmanager
    def retrieval_context(self):
        """Override ParallelBackendBase.retrieval_context to avoid deadlocks.

        This removes thread from the worker's thread pool (using 'secede'), and
        rejoins when finished (using 'rejoin'). This allows nested parallelism
        using joblib without deadlock.
        """
        # See 'joblib.Parallel.__call__' and 'joblib.Parallel.retrieve' for how
        # this is used.

        try:
            secede()
        except ValueError:
            pass

        yield

        try:
            rejoin()
        except AttributeError:
            pass


for base in _bases:
    base.register(DaskDistributedBackend)


DistributedBackend = DaskDistributedBackend


# Register the backend with any available versions of joblib
if joblib:
    joblib.register_parallel_backend('distributed', DaskDistributedBackend)
    joblib.register_parallel_backend('dask.distributed', DaskDistributedBackend)
if sk_joblib:
    sk_joblib.register_parallel_backend('distributed', DaskDistributedBackend)
    sk_joblib.register_parallel_backend('dask.distributed', DaskDistributedBackend)
