from __future__ import print_function, division, absolute_import

from threading import Thread

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from ..utils import sync, ignoring, All
from ..executor import Executor
from ..nanny import Nanny
from ..scheduler import Scheduler
from ..worker import Worker, _ncores


class Local(object):
    def __init__(self, n_workers=None, cores_per_worker=None, processes=True, loop=None, start=True, scheduler_port=8786, **kwargs):
        if n_workers is None:
            if processes:
                n_workers = _ncores
                cores_per_worker = 1
            else:
                n_workers = 1
                cores_per_worker = _ncores

        self.loop = loop or IOLoop()
        self.scheduler = Scheduler(loop=loop, **kwargs)
        self.scheduler.start(scheduler_port)
        self.workers = []

        if start:
            _start_worker = self.start_worker
        else:
            _start_worker = lambda *args, **kwargs: loop.add_callback(self._start_worker, *args, **kwargs)
        for i in range(n_workers):
            _start_worker(separate_process=processes,
                          ncores=cores_per_worker)

        if start:
            self._thread = Thread(target=self.loop.start)
            self._thread.daemon = True
            self._thread.start()

    def __str__(self):
        return "LocalCluster(%s, workers=%d, ncores=%d)" % (
                self.scheduler_address,
                len(self.workers),
                sum(w.ncores for w in self.workers))

    __repr__ = __str__

    @gen.coroutine
    def _start_worker(self, port=0, separate_process=True, **kwargs):
        if separate_process:
            W = Nanny
        else:
            W = Worker
        w = W(self.scheduler.ip, self.scheduler.port, loop=self.loop, **kwargs)
        yield w._start(port)
        self.workers.append(w)
        return w

    def start_worker(self, port=0, separate_process=True, **kwargs):
        return sync(self.loop, self._start_worker, port,
                separate_process=separate_process, **kwargs)

    @gen.coroutine
    def _stop_worker(self, w):
        yield w._close()
        self.workers.remove(w)

    def stop_worker(self, w):
        sync(self.loop, self._stop_worker, w)

    @gen.coroutine
    def _close(self):
        with ignoring(gen.TimeoutError, StreamClosedError, OSError):
            yield All([w._close() for w in self.workers])
        with ignoring(gen.TimeoutError, StreamClosedError, OSError):
            yield self.scheduler.close()
        self.workers.clear()

    def close(self):
        sync(self.loop, self._close)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def scheduler_address(self):
        return self.scheduler.address
