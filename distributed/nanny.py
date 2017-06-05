from __future__ import print_function, division, absolute_import

import atexit
from datetime import datetime, timedelta
import logging
from multiprocessing.queues import Empty
import os
import shutil
from time import sleep
import weakref

from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen

from .comm import get_address_host, get_local_address_for
from .core import rpc, RPCClosed, CommClosedError, coerce_to_address
from .metrics import disk_io_counters, net_io_counters, time
from .node import ServerNode
from .security import Security
from .utils import get_ip, ignoring, mp_context, log_errors, silence_logging
from .worker import _ncores, run


logger = logging.getLogger(__name__)


class Nanny(ServerNode):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """
    worker_dir = ''
    process = None
    status = None

    def __init__(self, scheduler_ip, scheduler_port=None, worker_port=0,
                 ncores=None, loop=None, local_dir=None, services=None,
                 name=None, memory_limit='auto', reconnect=True,
                 validate=False, quiet=False, resources=None, silence_logs=None,
                 death_timeout=None, preload=(), security=None, **kwargs):
        if scheduler_port is None:
            scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
        self.worker_address = None
        self._given_worker_port = worker_port
        self.ncores = ncores or _ncores
        self.reconnect = reconnect
        self.validate = validate
        self.resources = resources
        self.death_timeout = death_timeout
        self.preload = preload

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args('worker')
        self.listen_args = self.security.get_listen_args('worker')

        self.local_dir = local_dir

        self.loop = loop or IOLoop.current()
        self.scheduler = rpc(scheduler_addr, connection_args=self.connection_args)
        self.services = services
        self.name = name
        self.memory_limit = memory_limit
        self.quiet = quiet
        self.should_watch = True

        if silence_logs:
            silence_logging(level=silence_logs)
        self.silence_logs = silence_logs

        handlers = {'instantiate': self.instantiate,
                    'kill': self._kill,
                    'restart': self.restart,
                    'terminate': self._close,
                    'monitor_resources': self.monitor_resources,
                    'run': self.run}

        super(Nanny, self).__init__(handlers, io_loop=self.loop,
                                    connection_args=self.connection_args,
                                    **kwargs)

    def __str__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.ncores)

    __repr__ = __str__

    @gen.coroutine
    def _start(self, addr_or_port=0):
        """ Start nanny, start local process, start watching """

        # XXX Factor this out
        if not addr_or_port:
            # Default address is the required one to reach the scheduler
            self.listen(get_local_address_for(self.scheduler.address),
                        listen_args=self.listen_args)
            self.ip = get_address_host(self.address)
        elif isinstance(addr_or_port, int):
            # addr_or_port is an integer => assume TCP
            self.ip = get_ip(
                get_address_host(self.scheduler.address)
            )
            self.listen((self.ip, addr_or_port),
                        listen_args=self.listen_args)
        else:
            self.listen(addr_or_port, listen_args=self.listen_args)
            self.ip = get_address_host(self.address)

        logger.info('        Start Nanny at: %r', self.address)
        response = yield self.instantiate()
        if response == 'OK':
            self.loop.add_callback(self._watch)
            assert self.worker_address
            self.status = 'running'

    def start(self, addr_or_port=0):
        self.loop.add_callback(self._start, addr_or_port)

    @gen.coroutine
    def _kill(self, comm=None, timeout=10):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        timeout_time = time() + timeout

        while not self.worker_address:
            yield gen.sleep(0.1)
            if time() > timeout_time:
                raise gen.TimeoutError()

        if self.process is None:
            raise gen.Return('OK')

        should_watch, self.should_watch = self.should_watch, False

        if isalive(self.process):
            try:
                # Ask worker to close
                with self.rpc(self.worker_address) as worker:
                    result = yield gen.with_timeout(
                                timedelta(seconds=min(1, timeout)),
                                worker.terminate(report=False),
                    )

            except gen.TimeoutError:
                logger.info("Worker non-responsive.  Terminating.")
            except CommClosedError:
                pass
            except BaseException as e:
                if self.loop._running:
                    logger.exception(e)

            allowed_errors = (gen.TimeoutError, CommClosedError, EnvironmentError, RPCClosed)
            try:
                # Tell scheduler that worker is gone
                result = yield gen.with_timeout(timedelta(seconds=timeout),
                            self.scheduler.unregister(address=self.worker_address),
                            quiet_exceptions=allowed_errors)
                if result not in ('OK', 'already-removed'):
                    logger.critical("Unable to unregister with scheduler %s. "
                            "Nanny: %s, Worker: %s", result, self.address,
                            self.worker_address)
                else:
                    logger.info("Unregister worker %r from scheduler",
                                self.worker_address)
            except allowed_errors as e:
                # Maybe the scheduler is gone, or it is unresponsive
                logger.warning("Nanny %r failed to unregister worker %r: %s",
                               self.address, self.worker_address, e)
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
            logger.info("Nanny %r kills worker process %r",
                        self.address, self.worker_address)

        self.should_watch = should_watch
        return

    @gen.coroutine
    def instantiate(self, comm=None):
        """ Start a local worker process

        Blocks until the process is up and the scheduler is properly informed
        """
        should_watch, self.should_watch = self.should_watch, False

        try:
            if isalive(self.process):
                raise ValueError("Existing process still alive. Please kill first")

            q = mp_context.Queue()
            self.process = mp_context.Process(
                target=run_worker_fork,
                args=(q, self.scheduler.address, self.ncores,
                      self.port, self.ip, self._given_worker_port,
                      self.local_dir),
                kwargs={'services': self.services,
                        'name': self.name,
                        'memory_limit': self.memory_limit,
                        'reconnect': self.reconnect,
                        'resources': self.resources,
                        'validate': self.validate,
                        'silence_logs': self.silence_logs,
                        'death_timeout': self.death_timeout,
                        'preload': self.preload,
                        'security': self.security})
            self.process.daemon = True
            processes_to_close.add(self.process)
            self.process.start()
            start = time()
            while True:
                if self.death_timeout and time() > start + self.death_timeout:
                    yield self._close(timeout=1)
                try:
                    msg = q.get_nowait()
                    if isinstance(msg, Exception):
                        raise msg
                    self.worker_address = msg['address']
                    self.worker_dir = msg['dir']
                    assert self.worker_address
                    break
                except Empty:
                    yield gen.sleep(0.1)

            logger.info("Nanny %r starts worker process %r",
                        self.address, self.worker_address)
        except gen.Return:
            raise
        except Exception as e:
            logger.exception(e)
            raise
        finally:
            self.should_watch = should_watch

        raise gen.Return('OK')

    @gen.coroutine
    def restart(self, comm=None):
        self.should_watch = False
        yield self._kill()
        yield self.instantiate()
        self.should_watch = True
        raise gen.Return('OK')

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    def cleanup(self):
        with ignoring(Exception):
            with log_errors():
                if self.worker_dir and os.path.exists(self.worker_dir):
                    shutil.rmtree(self.worker_dir)
                self.worker_dir = None
                if self.process:
                    with ignoring(OSError):
                        self.process.terminate()

    def __del__(self):
        self.cleanup()

    @gen.coroutine
    def _watch(self, wait_seconds=0.20):
        """ Watch the local process, if it dies then spin up a new one """
        while True:
            if closing[0] or self.status == 'closed':
                yield self._close()
                break
            elif self.should_watch and self.process and not isalive(self.process):
                logger.warning("Discovered failed worker")
                self.cleanup()
                try:
                    yield self.scheduler.unregister(address=self.worker_address)
                except (EnvironmentError, CommClosedError):
                    if self.reconnect:
                        yield gen.sleep(wait_seconds)
                    else:
                        yield self._close()
                        break
                if self.status != 'closed':
                    logger.warning('Restarting worker...')
                    yield self.instantiate()
            else:
                yield gen.sleep(wait_seconds)

    @gen.coroutine
    def _close(self, comm=None, timeout=5, report=None):
        """ Close the nanny process, stop listening """
        if self.status == 'closed':
            raise gen.Return('OK')
        logger.info("Closing Nanny at %r", self.address)
        self.status = 'closed'
        with ignoring(gen.TimeoutError):
            yield self._kill(timeout=timeout)
        self.rpc.close()
        self.scheduler.close_rpc()
        self.stop()
        raise gen.Return('OK')

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
                'memory_info': p.memory_info()._asdict(),
                'disk_io_counters': disk_io_counters()._asdict(),
                'net_io_counters': net_io_counters()._asdict()}

    @gen.coroutine
    def monitor_resources(self, comm, interval=1):
        while not comm.closed():
            if self.process:
                yield comm.write(self.resource_collect())
            yield gen.sleep(interval)


def run_worker_fork(q, scheduler_addr, ncores, nanny_port,
                    worker_ip, worker_port, local_dir, silence_logs,
                    **kwargs):
    """
    Create a worker in a forked child.
    """
    from distributed import Worker  # pragma: no cover
    from tornado.ioloop import IOLoop  # pragma: no cover

    try:
        from dask.multiprocessing import initialize_worker_process
    except ImportError:   # old Dask version
        pass
    else:
        initialize_worker_process()

    if silence_logs:
        logger.setLevel(silence_logs)

    IOLoop.clear_instance()  # pragma: no cover
    loop = IOLoop()  # pragma: no cover
    loop.make_current()  # pragma: no cover
    worker = Worker(scheduler_addr, ncores=ncores,
                    service_ports={'nanny': nanny_port},
                    local_dir=local_dir, silence_logs=silence_logs,
                    **kwargs)  # pragma: no cover

    @gen.coroutine  # pragma: no cover
    def run():
        try:  # pragma: no cover
            yield worker._start(worker_port)  # pragma: no cover
        except Exception as e:  # pragma: no cover
            logger.exception(e)  # pragma: no cover
            q.put(e)  # pragma: no cover
        else:
            assert worker.port  # pragma: no cover
            q.put({'address': worker.address, 'dir': worker.local_dir})  # pragma: no cover

        yield worker.wait_until_closed()

        logger.info("Worker closed")
    try:
        loop.run_sync(run)
    except TimeoutError:
        logger.info("Worker timed out")
    except KeyboardInterrupt:
        pass
    finally:
        loop.stop()
        loop.close()


def isalive(proc):
    return proc is not None and proc.is_alive()


def join(proc, timeout):
    if proc is not None:
        proc.join(timeout)


closing = [False]

processes_to_close = weakref.WeakSet()


@atexit.register
def _closing():
    for proc in processes_to_close:
        try:
            proc.terminate()
        except OSError:
            pass

    closing[0] = True
