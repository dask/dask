from __future__ import print_function, division, absolute_import

from datetime import datetime, timedelta
import json
import logging
from multiprocessing.queues import Empty
import os
import shutil
import subprocess
import sys
import tempfile
from time import sleep
import weakref

from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError
from tornado import gen

from .compatibility import JSONDecodeError
from .core import Server, rpc, write, RPCClosed
from .metrics import disk_io_counters, net_io_counters, time
from .protocol import to_serialize
from .utils import get_ip, ignoring, log_errors, mp_context, tmpfile
from .worker import _ncores, Worker, run, TOTAL_MEMORY

nanny_environment = os.path.dirname(sys.executable)

logger = logging.getLogger(__name__)

class Nanny(Server):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """
    def __init__(self, scheduler_ip, scheduler_port, ip=None, worker_port=0,
                 ncores=None, loop=None, local_dir=None, services=None,
                 name=None, memory_limit=TOTAL_MEMORY, reconnect=True,
                 validate=False, environment=nanny_environment, quiet=False,
                 resources=None, silence_logs=None, **kwargs):
        self.ip = ip or get_ip()
        self.worker_port = None
        self._given_worker_port = worker_port
        self.ncores = ncores or _ncores
        self.reconnect = reconnect
        self.validate = validate
        self.resources = resources
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
        self.should_watch = True

        handlers = {'instantiate': self.instantiate,
                    'kill': self._kill,
                    'restart': self.restart,
                    'terminate': self._close,
                    'monitor_resources': self.monitor_resources,
                    'run': self.run}

        if silence_logs:
            logger.setLevel(silence_logs)
        self.silence_logs = silence_logs

        super(Nanny, self).__init__(handlers, io_loop=self.loop, **kwargs)

    def __str__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.ncores)

    __repr__ = __str__

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
    def _kill(self, stream=None, timeout=10):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        while not self.worker_port:
            yield gen.sleep(0.1)

        if self.process is None:
            raise gen.Return('OK')

        should_watch, self.should_watch = self.should_watch, False

        if isalive(self.process):
            try:
                # Ask worker to close
                with rpc(ip='127.0.0.1', port=self.worker_port) as worker:
                    result = yield gen.with_timeout(
                                timedelta(seconds=min(1, timeout)),
                                worker.terminate(report=False),
                                io_loop=self.loop)

            except gen.TimeoutError:
                logger.info("Worker non-responsive.  Terminating.")
            except StreamClosedError:
                pass
            except BaseException as e:
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
                logger.warn("Nanny %s:%d failed to unregister worker %s:%d",
                        self.ip, self.port, self.ip, self.worker_port,
                        exc_info=True)
            except (StreamClosedError, RPCClosed):
                pass
            except Exception as e:
                logger.exception(e)

        if self.process:
            with ignoring(OSError):
                self.process.terminate()
            join(self.process, timeout)
            processes_to_close.discard(self.process)

            start = time()
            while isalive(self.process) and time() < start + timeout:
                sleep(0.01)

            self.process = None
            self.cleanup()
            logger.info("Nanny %s:%d kills worker process %s:%d",
                        self.ip, self.port, self.ip, self.worker_port)

        self.should_watch = should_watch
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

        should_watch, self.should_watch = self.should_watch, False

        try:
            if isalive(self.process):
                raise ValueError("Existing process still alive. Please kill first")

            if self.environment != nanny_environment:
                with tmpfile() as fn:
                    self.process = run_worker_subprocess(self.environment, self.ip,
                            self.scheduler.ip, self.scheduler.port, self.ncores,
                            self.port, self._given_worker_port, self.name,
                            self.memory_limit, self.loop, fn, self.quiet,
                            self.resources)

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
            else:
                q = mp_context.Queue()
                self.process = mp_context.Process(
                    target=run_worker_fork,
                    args=(q, self.ip, self.scheduler.ip,
                          self.scheduler.port, self.ncores,
                          self.port, self._given_worker_port,
                          self.local_dir),
                    kwargs={'services': self.services,
                            'name': self.name,
                            'memory_limit': self.memory_limit,
                            'reconnect': self.reconnect,
                            'resources': self.resources,
                            'validate': self.validate,
                            'silence_logs': self.silence_logs})
                self.process.daemon = True
                self.process.start()
                while True:
                    try:
                        msg = q.get_nowait()
                        if isinstance(msg, Exception):
                            raise msg
                        self.worker_port = msg['port']
                        self.worker_dir = msg['dir']
                        assert self.worker_port
                        break
                    except Empty:
                        yield gen.sleep(0.1)

            logger.info("Nanny %s:%d starts worker process %s:%d",
                        self.ip, self.port, self.ip, self.worker_port)
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            self.should_watch = should_watch

        raise gen.Return('OK')

    @gen.coroutine
    def restart(self, stream=None, environment=None):
        self.should_watch = False
        yield self._kill()
        yield self.instantiate(environment=environment)
        self.should_watch = True
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
    def _watch(self, wait_seconds=0.20):
        """ Watch the local process, if it dies then spin up a new one """
        while True:
            if closing[0] or self.status == 'closed':
                yield self._close()
                break
            elif self.should_watch and self.process and not isalive(self.process):
                logger.warn("Discovered failed worker")
                self.cleanup()
                try:
                    yield self.scheduler.unregister(address=self.worker_address)
                except StreamClosedError:
                    if self.reconnect:
                        yield gen.sleep(wait_seconds)
                    else:
                        yield self._close()
                        break
                logger.warn('Restarting worker...')
                yield self.instantiate()
            else:
                yield gen.sleep(wait_seconds)

    @gen.coroutine
    def _close(self, stream=None, timeout=5, report=None):
        """ Close the nanny process, stop listening """
        if self.status == 'closed':
            raise gen.Return('OK')
        logger.info("Closing Nanny at %s:%d", self.ip, self.port)
        self.status = 'closed'
        yield self._kill(timeout=timeout)
        self.rpc.close()
        self.scheduler.close_rpc()
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
        if self.worker_port:
            return '%s:%d' % (self.ip, self.worker_port)
        else:
            return '%s:port-unassigned' % self.ip

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
                'disk_io_counters': disk_io_counters()._asdict(),
                'net_io_counters': net_io_counters()._asdict()}

    @gen.coroutine
    def monitor_resources(self, stream, interval=1):
        while not stream.closed():
            if self.process:
                yield write(stream, self.resource_collect())
            yield gen.sleep(interval)


def run_worker_subprocess(environment, ip, scheduler_ip, scheduler_port, ncores,
        nanny_port, worker_port, name, memory_limit, io_loop, fn, quiet,
        resources):

    if environment.endswith('python'):
        environment = os.path.dirname(environment)
    if os.path.exists(os.path.join(environment, 'bin')):
        environment = os.path.join(environment, 'bin')
    executable = os.path.join(environment, 'python')

    args = ['-m', 'distributed.cli.dask_worker']

    args.extend(['%s:%d' %(scheduler_ip, scheduler_port),
                 '--no-nanny',
                 '--host', ip,
                 '--worker-port', worker_port,
                 '--nanny-port', nanny_port,
                 '--nthreads', ncores,
                 '--nprocs', 1,
                 '--resources', '"%s"' % ' '.join('='.join(map(str, item))
                                                  for item in resources),
                 '--temp-filename', fn])

    if name:
        args.extend(['--name', name])

    if memory_limit:
        args.extend(['--memory-limit', memory_limit])

    proc = subprocess.Popen([executable] + list(map(str, args)),
            stderr=subprocess.PIPE if quiet else None)

    processes_to_close.add(proc)

    return proc


def run_worker_fork(q, ip, scheduler_ip, scheduler_port, ncores, nanny_port,
                    worker_port, local_dir, **kwargs):
    """ Function run by the Nanny when creating the worker """
    from distributed import Worker  # pragma: no cover
    from tornado.ioloop import IOLoop  # pragma: no cover
    IOLoop.clear_instance()  # pragma: no cover
    loop = IOLoop()  # pragma: no cover
    loop.make_current()  # pragma: no cover
    worker = Worker(scheduler_ip, scheduler_port, ncores=ncores, ip=ip,
                    service_ports={'nanny': nanny_port}, local_dir=local_dir,
                    loop=loop, **kwargs)  # pragma: no cover

    @gen.coroutine  # pragma: no cover
    def run():
        try:  # pragma: no cover
            yield worker._start(worker_port)  # pragma: no cover
        except Exception as e:  # pragma: no cover
            logger.exception(e)  # pragma: no cover
            q.put(e)  # pragma: no cover
        else:
            assert worker.port  # pragma: no cover
            q.put({'port': worker.port, 'dir': worker.local_dir})  # pragma: no cover

        while worker.status != 'closed':
            yield gen.sleep(0.1)

        logger.info("Worker closed")

    try:
        loop.run_sync(run)
    finally:
        loop.stop()
        loop.close(all_fds=True)


def isalive(proc):
    if proc is None:
        return False
    elif isinstance(proc, subprocess.Popen):
        return proc.poll() is None
    else:
        return proc.is_alive()


def join(proc, timeout):
    if proc is None or isinstance(proc, subprocess.Popen):
        return
    proc.join(timeout)


import atexit

closing = [False]

processes_to_close = weakref.WeakSet()

def _closing():
    for proc in processes_to_close:
        try:
            proc.terminate()
        except OSError:
            pass

    closing[0] = True

atexit.register(_closing)
