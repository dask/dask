from __future__ import print_function, division, absolute_import

from joblib._parallel_backends import ParallelBackendBase, AutoBatchingMixin
from joblib.parallel import register_parallel_backend
from tornado import gen

from .executor import Executor, _wait


class DistributedBackend(ParallelBackendBase, AutoBatchingMixin):
    MIN_IDEAL_BATCH_DURATION = 0.2
    MAX_IDEAL_BATCH_DURATION = 1.0

    def __init__(self, scheduler_host='127.0.0.1:8786', loop=None):
        self.executor = Executor(scheduler_host, loop=loop)

    def configure(self, n_jobs=1, parallel=None, **backend_args):
        return self.effective_n_jobs(n_jobs)

    def effective_n_jobs(self, n_jobs=1):
        return sum(self.executor.ncores().values())

    def apply_async(self, func, *args, **kwargs):
        callback = kwargs.pop('callback', None)
        kwargs['pure'] = False
        future = self.executor.submit(func, *args, **kwargs)

        @gen.coroutine
        def callback_wrapper():
            result = yield _wait([future])
            callback(result)  # gets called in separate thread

        self.executor.loop.add_callback(callback_wrapper)

        future.get = future.result  # monkey patch to achieve AsyncResult API
        return future


register_parallel_backend('distributed', DistributedBackend)
