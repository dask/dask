from __future__ import print_function, division, absolute_import

from datetime import datetime, timedelta
import json
import logging
from multiprocessing import Process, Queue, queues
import os
import shutil
import subprocess
import sys
import tempfile
from time import time, sleep

from tornado.ioloop import IOLoop
from tornado import gen

from .compatibility import JSONDecodeError
from .core import Server, rpc, write
from .utils import get_ip, ignoring, log_errors, tmpfile
from .worker import _ncores, Worker, run

environment = os.path.dirname(sys.executable)

logger = logging.getLogger(__name__)

class Nanny(Server):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """
    def __init__(self, scheduler_ip, scheduler_port, ip=None, worker_port=0,
                 ncores=None, loop=None, local_dir=None, services=None,
                 name=None, memory_limit=None, environment=environment,
                 quiet=False, **kwargs):
        self.ip = ip or get_ip()
        self.worker_port = None
        self._given_worker_port = worker_port
        self.ncores = ncores or _ncores
        if not local_dir:
            local_dir = tempfile.mkdtemp(prefix='nanny-')
            self._should_cleanup_local_dir = True
            def _cleanup_local_dir():
                if os.path.exists(local_dir):
                    shutil.rmtree(local_dir)
            atexit.register(_cleanup_local_dir)
        else:
            self._should_cleanup_local_dir = False
        self.local_dir = local_dir
        self.worker_dir = ''
        self.status = None
        self.process = None
        self.loop = loop or IOLoop.current()
        self.scheduler = rpc(ip=scheduler_ip, port=scheduler_port)
        self.services = services
        self.name = name
        self.memory_limit = memory_limit
        self.environment = environment
        self.quiet = quiet

        handlers = {'instantiate': self.instantiate,
                    'kill': self._kill,
                    'terminate': self._close,
                    'monitor_resources': self.monitor_resources,
                    'run': self.run}

        super(Nanny, self).__init__(handlers, io_loop=self.loop, **kwargs)

    @gen.coroutine
    def _start(self, port=0):
        """ Start nanny, start local process, start watching """
        self.listen(port)
        logger.info('        Start Nanny at: %20s:%d', self.ip, self.port)
        yield self.instantiate()
        self.loop.add_callback(self._watch)
        assert self.worker_port
        self.status = 'running'

    def start(self, port=0):
        self.loop.add_callback(self._start, port)

    @gen.coroutine
    def _kill(self, stream=None, timeout=5):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        while not self.worker_port:
            yield gen.sleep(0.1)

        if self.process is not None:
            try:
                # Ask worker to close
                worker = rpc(ip='127.0.0.1', port=self.worker_port)
                result = yield gen.with_timeout(
                            timedelta(seconds=min(1, timeout)),
                            worker.terminate(report=False),
                            io_loop=self.loop)
            except gen.TimeoutError:
                logger.info("Worker non-responsive.  Terminating.")
            except Exception as e:
                logger.exception(e)

            try:
                # Tell scheduler that worker is gone
                result = yield gen.with_timeout(timedelta(seconds=timeout),
                            self.scheduler.unregister(address=self.worker_address),
                            io_loop=self.loop)
                if result not in ('OK', 'already-removed'):
                    logger.critical("Unable to unregister with scheduler %s. "
                            "Nanny: %s, Worker: %s", result, self.address_tuple,
                            self.worker_address)
                else:
                    logger.info("Unregister worker %s:%d from scheduler",
                                self.ip, self.worker_port)
            except gen.TimeoutError:
                logger.info("Nanny %s:%d failed to unregister worker %s:%d",
                        self.ip, self.port, self.ip, self.worker_port,
                        exc_info=True)
            except Exception as e:
                logger.exception(e)

            if self.process:
                with ignoring(OSError):
                    self.process.terminate()
                processes_to_close.remove(self.process)

                start = time()
                while self.process.poll() is None and time() < start + timeout:
                    sleep(0.01)

                self.process = None
                self.cleanup()
                logger.info("Nanny %s:%d kills worker process %s:%d",
                            self.ip, self.port, self.ip, self.worker_port)
        raise gen.Return('OK')

    @gen.coroutine
    def instantiate(self, stream=None, environment=None):
        """ Start a local worker process

        Blocks until the process is up and the scheduler is properly informed
        """
        if environment:
            if not os.path.isabs(environment):
                environment = os.path.join(self.local_dir, environment)
            self.environment = environment

        with log_errors():
            if self.process and self.process.poll() is None:
                raise ValueError("Existing process still alive. Please kill first")
            with tmpfile() as fn:
                self.process = run_worker(self.environment, self.ip,
                        self.scheduler.ip, self.scheduler.port, self.ncores,
                        self.port, self._given_worker_port, self.name,
                        self.memory_limit, self.loop, fn, self.quiet)

                while not os.path.exists(fn):
                    yield gen.sleep(0.01)

                while True:
                    try:
                        with open(fn) as f:
                            msg = json.load(f)
                        self.worker_port = msg['port']
                        self.worker_dir = msg['local_directory']
                        break
                    except JSONDecodeError:
                        yield gen.sleep(0.01)

            logger.info("Nanny %s:%d starts worker process %s:%d",
                        self.ip, self.port, self.ip, self.worker_port)
            raise gen.Return('OK')

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    def cleanup(self):
        if self.worker_dir and os.path.exists(self.worker_dir):
            shutil.rmtree(self.worker_dir)
        self.worker_dir = None
        if self.process:
            with ignoring(OSError):
                self.process.terminate()

    def __del__(self):
        if self._should_cleanup_local_dir and os.path.exists(self.local_dir):
            shutil.rmtree(self.local_dir)
        self.cleanup()

    @gen.coroutine
    def _watch(self, wait_seconds=0.10):
        """ Watch the local process, if it dies then spin up a new one """
        while True:
            if closing[0] or self.status == 'closed':
                yield self._close()
                break
            elif self.process and self.process.poll() is not None:
                logger.warn("Discovered failed worker.  Restarting. Status: %s",
                            self.status)
                self.cleanup()
                yield self.scheduler.unregister(address=self.worker_address)
                yield self.instantiate()
            else:
                yield gen.sleep(wait_seconds)

    @gen.coroutine
    def _close(self, stream=None, timeout=5, report=None):
        """ Close the nanny process, stop listening """
        logger.info("Closing Nanny at %s:%d", self.ip, self.port)
        self.status = 'closed'
        yield self._kill(timeout=timeout)
        self.scheduler.close_streams()
        self.stop()
        raise gen.Return('OK')

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

    @property
    def address_tuple(self):
        return (self.ip, self.port)

    @property
    def worker_address_tuple(self):
        return (self.ip, self.worker_port)

    @property
    def worker_address(self):
        return '%s:%d' % (self.ip, self.worker_port)

    def resource_collect(self):
        try:
            import psutil
        except ImportError:
            return {}
        p = psutil.Process(self.process.pid)
        return {'timestamp': datetime.now().isoformat(),
                'cpu_percent': psutil.cpu_percent(),
                'status': p.status(),
                'memory_percent': p.memory_percent(),
                'memory_info_ex': p.memory_info_ex()._asdict(),
                'disk_io_counters': psutil.disk_io_counters()._asdict(),
                'net_io_counters': psutil.net_io_counters()._asdict()}

    @gen.coroutine
    def monitor_resources(self, stream, interval=1):
        while not stream.closed():
            if self.process:
                yield write(stream, self.resource_collect())
            yield gen.sleep(interval)


def run_worker(environment, ip, scheduler_ip, scheduler_port, ncores,
        nanny_port, worker_port, name, memory_limit, io_loop, fn, quiet):

    if environment.endswith('python'):
        environment = os.path.dirname(environment)
    if os.path.exists(os.path.join(environment, 'bin')):
        environment = os.path.join(environment, 'bin')
    executable = os.path.join(environment, 'python')

    args = ['-m' 'distributed.cli.dask_worker']
    if sys.platform.startswith('win'):
        args.append('--scheduler')

    args.extend(['%s:%d' %(scheduler_ip, scheduler_port),
                 '--no-nanny',
                 '--host', ip,
                 '--worker-port', worker_port,
                 '--nanny-port', nanny_port,
                 '--nthreads', ncores,
                 '--nprocs', 1,
                 '--temp-filename', fn])

    if name:
        args.extend(['--name', name])

    if memory_limit:
        args.extend(['--memory-limit', memory_limit])

    proc = subprocess.Popen([executable] + list(map(str, args)),
            stderr=subprocess.PIPE if quiet else None)

    processes_to_close.append(proc)

    return proc


import atexit

closing = [False]

processes_to_close = []

def _closing():
    for proc in processes_to_close:
        if proc.poll() is not None:
            try:
                proc.terminate()
            except OSError:
                pass

    closing[0] = True

atexit.register(_closing)
