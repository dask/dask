from __future__ import print_function, division, absolute_import

from distutils.version import LooseVersion
import uuid

from tornado import gen

from .client import Client, _wait
from .utils import ignoring


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

if joblib:
    from joblib.parallel import (ParallelBackendBase,
        AutoBatchingMixin)
elif sk_joblib:
    from sklearn.externals.joblib.parallel import (
        ParallelBackendBase, AutoBatchingMixin)
else:
    raise RuntimeError("Joblib backend requires either `joblib` >= '0.10.2' "
                       " or `sklearn` > '0.17.1'. Please install or upgrade")


class DaskDistributedBackend(ParallelBackendBase, AutoBatchingMixin):
    MIN_IDEAL_BATCH_DURATION = 0.2
    MAX_IDEAL_BATCH_DURATION = 1.0

    def __init__(self, scheduler_host='127.0.0.1:8786', loop=None):
        self.client = Client(scheduler_host, loop=loop)
        self.futures = set()

    def configure(self, n_jobs=1, parallel=None, **backend_args):
        return self.effective_n_jobs(n_jobs)

    def effective_n_jobs(self, n_jobs=1):
        return sum(self.client.ncores().values())

    def apply_async(self, func, *args, **kwargs):
        callback = kwargs.pop('callback', None)
        kwargs['key'] = 'joblib-' + str(uuid.uuid4())
        future = self.client.submit(func, *args, **kwargs)
        self.futures.add(future)

        @gen.coroutine
        def callback_wrapper():
            result = yield _wait([future])
            self.futures.remove(future)
            callback(result)  # gets called in separate thread

        self.client.loop.add_callback(callback_wrapper)

        future.get = future.result  # monkey patch to achieve AsyncResult API
        return future

    def abort_everything(self, ensure_ready=True):
        # Tell the client to cancel any task submitted via this instance
        # as joblib.Parallel will never access those results.
        self.client.cancel(self.futures)
        self.futures.clear()


DistributedBackend = DaskDistributedBackend


# Register the backend with any available versions of joblib
if joblib:
    joblib.register_parallel_backend('distributed', DaskDistributedBackend)
    joblib.register_parallel_backend('dask.distributed', DaskDistributedBackend)
if sk_joblib:
    sk_joblib.register_parallel_backend('distributed', DaskDistributedBackend)
    sk_joblib.register_parallel_backend('dask.distributed', DaskDistributedBackend)
