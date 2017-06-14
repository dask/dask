from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
from multiprocessing.queues import Empty
import os
import shutil
import threading

from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from tornado.locks import Event

from .comm import get_address_host, get_local_address_for
from .core import rpc, RPCClosed, CommClosedError, coerce_to_address
from .node import ServerNode
from .process import AsyncProcess
from .security import Security
from .utils import get_ip, mp_context, silence_logging
from .worker import _ncores, run


logger = logging.getLogger(__name__)


class Nanny(ServerNode):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """
    process = None
    status = None

    def __init__(self, scheduler_ip, scheduler_port=None, worker_port=0,
                 ncores=None, loop=None, local_dir=None, services=None,
                 name=None, memory_limit='auto', reconnect=True,
                 validate=False, quiet=False, resources=None, silence_logs=None,
                 death_timeout=None, preload=(), security=None, **kwargs):
        if scheduler_port is None:
            self.scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            self.scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
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
        self.scheduler = rpc(self.scheduler_addr, connection_args=self.connection_args)
        self.services = services
        self.name = name
        self.memory_limit = memory_limit
        self.quiet = quiet
        self.auto_restart = True

        if silence_logs:
            silence_logging(level=silence_logs)
        self.silence_logs = silence_logs

        handlers = {'instantiate': self.instantiate,
                    'kill': self.kill,
                    'restart': self.restart,
                    # cannot call it 'close' on the rpc side for naming conflict
                    'terminate': self._close,
                    'run': self.run}

        super(Nanny, self).__init__(handlers, io_loop=self.loop,
                                    connection_args=self.connection_args,
                                    **kwargs)

        self.status = 'init'

    def __str__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.ncores)

    __repr__ = __str__

    @gen.coroutine
    def _unregister(self, timeout=10):
        if self.process is None:
            return
        worker_address = self.process.worker_address
        if worker_address is None:
            return

        allowed_errors = (gen.TimeoutError, CommClosedError, EnvironmentError, RPCClosed)
        try:
            yield gen.with_timeout(timedelta(seconds=timeout),
                                   self.scheduler.unregister(address=self.worker_address),
                                   quiet_exceptions=allowed_errors)
        except allowed_errors:
            pass

    @property
    def worker_address(self):
        return None if self.process is None else self.process.worker_address

    @property
    def worker_dir(self):
        return None if self.process is None else self.process.worker_dir

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
            assert self.worker_address
            self.status = 'running'

    def start(self, addr_or_port=0):
        self.loop.add_callback(self._start, addr_or_port)

    @gen.coroutine
    def kill(self, comm=None, timeout=10):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        self.auto_restart = False
        if self.process is None:
            raise gen.Return('OK')

        deadline = self.loop.time() + timeout
        yield self.process.kill(grace_delay=0.8 * (deadline - self.loop.time()))
        yield self._unregister(deadline - self.loop.time())

    @gen.coroutine
    def instantiate(self, comm=None):
        """ Start a local worker process

        Blocks until the process is up and the scheduler is properly informed
        """
        if self.process is None:
            self.process = WorkerProcess(
                worker_args=(self.scheduler_addr,),
                worker_kwargs=dict(ncores=self.ncores,
                                   local_dir=self.local_dir,
                                   services=self.services,
                                   service_ports={'nanny': self.port},
                                   name=self.name,
                                   memory_limit=self.memory_limit,
                                   reconnect=self.reconnect,
                                   resources=self.resources,
                                   validate=self.validate,
                                   silence_logs=self.silence_logs,
                                   death_timeout=self.death_timeout,
                                   preload=self.preload,
                                   security=self.security),
                worker_start_args=(self._given_worker_port,),
                silence_logs=self.silence_logs,
                on_exit=self._on_exit,
            )

        self.auto_restart = True
        if self.death_timeout:
            try:
                yield gen.with_timeout(timedelta(seconds=self.death_timeout),
                                       self.process.start())
            except gen.TimeoutError:
                yield self._close(timeout=self.death_timeout)
                raise gen.Return('timed out')
        else:
            yield self.process.start()
        raise gen.Return('OK')

    @gen.coroutine
    def restart(self, comm=None):
        if self.process is not None:
            yield self.kill()
        yield self.instantiate()
        raise gen.Return('OK')

    def is_alive(self):
        return self.process is not None and self.process.status == 'running'

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    @gen.coroutine
    def _on_exit(self, exitcode):
        if self.status not in ('closing', 'closed'):
            try:
                yield self.scheduler.unregister(address=self.worker_address)
            except (EnvironmentError, CommClosedError):
                if not self.reconnect:
                    yield self._close()
                    return

            try:
                if self.status not in ('closing', 'closed'):
                    if self.auto_restart:
                        logger.warning("Restarting worker")
                        yield self.instantiate()
            except Exception:
                logger.error("Failed to restart worker after its process exited",
                             exc_info=True)

    @property
    def pid(self):
        return self.process and self.process.pid

    @gen.coroutine
    def _close(self, comm=None, timeout=5, report=None):
        """
        Close the worker process, stop all comms.
        """
        if self.status in ('closing', 'closed'):
            raise gen.Return('OK')
        self.status = 'closing'
        logger.info("Closing Nanny at %r", self.address)
        self.stop()
        try:
            if self.process is not None:
                yield self.kill(timeout=timeout)
        except Exception:
            pass
        self.process = None
        self.rpc.close()
        self.scheduler.close_rpc()
        self.status = 'closed'
        raise gen.Return('OK')


class WorkerProcess(object):

    def __init__(self, worker_args, worker_kwargs, worker_start_args,
                 silence_logs, on_exit):
        self.status = 'init'
        self.silence_logs = silence_logs
        self.worker_args = worker_args
        self.worker_kwargs = worker_kwargs
        self.worker_start_args = worker_start_args
        self.on_exit = on_exit
        self.process = None

        # Initialized when worker is ready
        self.worker_dir = None
        self.worker_address = None

    @gen.coroutine
    def start(self):
        """
        Ensure the worker process is started.
        """
        if self.status == 'running':
            return
        if self.status == 'starting':
            yield self.running.wait()
            return

        self.init_result_q = mp_context.Queue()
        self.child_stop_q = mp_context.Queue()
        self.process = AsyncProcess(
            target=self._run,
            kwargs=dict(worker_args=self.worker_args,
                        worker_kwargs=self.worker_kwargs,
                        worker_start_args=self.worker_start_args,
                        silence_logs=self.silence_logs,
                        init_result_q=self.init_result_q,
                        child_stop_q=self.child_stop_q),
        )
        self.process.daemon = True
        self.process.set_exit_callback(self._on_exit)
        self.running = Event()
        self.stopped = Event()
        self.status = 'starting'
        yield self.process.start()
        if self.status == 'starting':
            yield self._wait_until_running()

    def _on_exit(self, proc):
        if proc is not self.process:
            # Ignore exit of old process instance
            return
        self.mark_stopped()

    def _death_message(self, pid, exitcode):
        assert exitcode is not None
        if exitcode == 255:
            return "Worker process %d was killed by unknown signal" % (pid,)
        elif exitcode >= 0:
            return "Worker process %d exited with status %d" % (pid, exitcode,)
        else:
            return "Worker process %d was killed by signal %d" % (pid, -exitcode,)

    def is_alive(self):
        return self.process is not None and self.process.is_alive()

    @property
    def pid(self):
        return (self.process.pid
                if self.process and self.process.is_alive()
                else None)

    def mark_stopped(self):
        if self.status != 'stopped':
            r = self.process.exitcode
            assert r is not None
            if r != 0:
                msg = self._death_message(self.process.pid, r)
                logger.warning(msg)
            self.status = 'stopped'
            self.stopped.set()
            # Release resources
            self.init_result_q = None
            self.child_stop_q = None
            self.process = None
            # Best effort to clean up worker directory
            if self.worker_dir and os.path.exists(self.worker_dir):
                shutil.rmtree(self.worker_dir, ignore_errors=True)
            self.worker_dir = None
            # User hook
            if self.on_exit is not None:
                self.on_exit(r)

    @gen.coroutine
    def kill(self, grace_delay=10):
        """
        Ensure the worker process is stopped, waiting at most
        *grace_delay* seconds before terminating it abruptly.
        """
        loop = IOLoop.current()
        deadline = loop.time() + grace_delay

        if self.status == 'stopped':
            return
        if self.status == 'stopping':
            yield self.stopped.wait()
            return
        assert self.status in ('starting', 'running')
        self.status = 'stopping'

        process = self.process
        self.child_stop_q.put({'op': 'stop',
                               'timeout': max(0, deadline - loop.time()) * 0.8,
                               })

        while process.is_alive() and loop.time() < deadline:
            yield gen.sleep(0.05)

        if process.is_alive():
            logger.warning("Worker process still alive after %d seconds, killing",
                           grace_delay)
            try:
                yield process.terminate()
            except Exception as e:
                logger.error("Failed to kill worker process: %s", e)

    @gen.coroutine
    def _wait_until_running(self):
        delay = 0.05
        while True:
            if self.status != 'starting':
                raise ValueError("Worker not started")
            try:
                msg = self.init_result_q.get_nowait()
            except Empty:
                yield gen.sleep(delay)
                continue

            if isinstance(msg, Exception):
                yield self.process.join()
                raise msg
            else:
                self.worker_address = msg['address']
                self.worker_dir = msg['dir']
                assert self.worker_address
                self.status = 'running'
                self.running.set()
                raise gen.Return(msg)

    @classmethod
    def _run(cls, worker_args, worker_kwargs, worker_start_args,
             silence_logs, init_result_q, child_stop_q):  # pragma: no cover
        from distributed import Worker

        try:
            from dask.multiprocessing import initialize_worker_process
        except ImportError:   # old Dask version
            pass
        else:
            initialize_worker_process()

        if silence_logs:
            logger.setLevel(silence_logs)

        IOLoop.clear_instance()
        loop = IOLoop()
        loop.make_current()
        worker = Worker(*worker_args, **worker_kwargs)

        @gen.coroutine
        def do_stop(timeout):
            try:
                yield worker._close(report=False, nanny=False)
            finally:
                loop.stop()

        def watch_stop_q():
            """
            Wait for an incoming stop message and then stop the
            worker cleanly.
            """
            while True:
                try:
                    msg = child_stop_q.get(timeout=1000)
                except Empty:
                    pass
                else:
                    assert msg['op'] == 'stop'
                    loop.add_callback(do_stop, msg['timeout'])
                    break

        t = threading.Thread(target=watch_stop_q, name="Nanny stop queue watch")
        t.daemon = True
        t.start()

        @gen.coroutine
        def run():
            """
            Try to start worker and inform parent of outcome.
            """
            try:
                yield worker._start(*worker_start_args)
            except Exception as e:
                logger.exception(e)
                init_result_q.put(e)
            else:
                assert worker.address
                init_result_q.put({'address': worker.address,
                                   'dir': worker.local_dir})
                yield worker.wait_until_closed()
                logger.info("Worker closed")

        try:
            loop.run_sync(run)
        except TimeoutError:
            # Loop was stopped before wait_until_closed() returned, ignore
            pass
        except KeyboardInterrupt:
            pass
