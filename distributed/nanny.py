from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
from multiprocessing.queues import Empty
import os
import psutil
import shutil
import threading
import uuid
import warnings

import dask
from tornado import gen
from tornado.ioloop import IOLoop, TimeoutError
from tornado.locks import Event

from .comm import get_address_host, get_local_address_for, unparse_host_port
from .core import rpc, RPCClosed, CommClosedError, coerce_to_address
from .metrics import time
from .node import ServerNode
from .process import AsyncProcess
from .proctitle import enable_proctitle_on_children
from .security import Security
from .utils import (
    get_ip,
    mp_context,
    silence_logging,
    json_load_robust,
    PeriodicCallback,
)
from .worker import _ncores, run, parse_memory_limit, Worker

logger = logging.getLogger(__name__)


class Nanny(ServerNode):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """

    process = None
    status = None

    def __init__(
        self,
        scheduler_ip=None,
        scheduler_port=None,
        scheduler_file=None,
        worker_port=0,
        ncores=None,
        loop=None,
        local_dir="dask-worker-space",
        services=None,
        name=None,
        memory_limit="auto",
        reconnect=True,
        validate=False,
        quiet=False,
        resources=None,
        silence_logs=None,
        death_timeout=None,
        preload=(),
        preload_argv=[],
        security=None,
        contact_address=None,
        listen_address=None,
        worker_class=None,
        env=None,
        **worker_kwargs
    ):

        if scheduler_file:
            cfg = json_load_robust(scheduler_file)
            self.scheduler_addr = cfg["address"]
        elif scheduler_ip is None and dask.config.get("scheduler-address"):
            self.scheduler_addr = dask.config.get("scheduler-address")
        elif scheduler_port is None:
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
        self.preload_argv = preload_argv
        self.Worker = Worker if worker_class is None else worker_class
        self.env = env or {}
        self.worker_kwargs = worker_kwargs

        self.contact_address = contact_address
        self.memory_terminate_fraction = dask.config.get(
            "distributed.worker.memory.terminate"
        )

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("worker")
        self.listen_args = self.security.get_listen_args("worker")

        self.local_dir = local_dir

        self.loop = loop or IOLoop.current()
        self.scheduler = rpc(self.scheduler_addr, connection_args=self.connection_args)
        self.services = services
        self.name = name
        self.quiet = quiet
        self.auto_restart = True

        self.memory_limit = parse_memory_limit(memory_limit, self.ncores)

        if silence_logs:
            silence_logging(level=silence_logs)
        self.silence_logs = silence_logs

        handlers = {
            "instantiate": self.instantiate,
            "kill": self.kill,
            "restart": self.restart,
            # cannot call it 'close' on the rpc side for naming conflict
            "terminate": self.close,
            "run": self.run,
        }

        super(Nanny, self).__init__(
            handlers, io_loop=self.loop, connection_args=self.connection_args
        )

        if self.memory_limit:
            pc = PeriodicCallback(self.memory_monitor, 100, io_loop=self.loop)
            self.periodic_callbacks["memory"] = pc

        self._listen_address = listen_address
        self.status = "init"

    def __repr__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.ncores)

    @gen.coroutine
    def _unregister(self, timeout=10):
        if self.process is None:
            return
        worker_address = self.process.worker_address
        if worker_address is None:
            return

        allowed_errors = (
            gen.TimeoutError,
            CommClosedError,
            EnvironmentError,
            RPCClosed,
        )
        try:
            yield gen.with_timeout(
                timedelta(seconds=timeout),
                self.scheduler.unregister(address=self.worker_address),
                quiet_exceptions=allowed_errors,
            )
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
            self.listen(
                get_local_address_for(self.scheduler.address),
                listen_args=self.listen_args,
            )
            self.ip = get_address_host(self.address)
        elif isinstance(addr_or_port, int):
            # addr_or_port is an integer => assume TCP
            self.ip = get_ip(get_address_host(self.scheduler.address))
            self.listen((self.ip, addr_or_port), listen_args=self.listen_args)
        else:
            self.listen(addr_or_port, listen_args=self.listen_args)
            self.ip = get_address_host(self.address)

        logger.info("        Start Nanny at: %r", self.address)
        response = yield self.instantiate()
        if response == "running":
            assert self.worker_address
            self.status = "running"
        else:
            yield self.close()

        self.start_periodic_callbacks()

        raise gen.Return(self)

    def __await__(self):
        return self._start().__await__()

    def start(self, addr_or_port=0):
        self.loop.add_callback(self._start, addr_or_port)

    @gen.coroutine
    def kill(self, comm=None, timeout=2):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        self.auto_restart = False
        if self.process is None:
            raise gen.Return("OK")

        deadline = self.loop.time() + timeout
        yield self.process.kill(timeout=0.8 * (deadline - self.loop.time()))
        yield self._unregister(deadline - self.loop.time())

    @gen.coroutine
    def instantiate(self, comm=None):
        """ Start a local worker process

        Blocks until the process is up and the scheduler is properly informed
        """
        if self._listen_address:
            start_arg = self._listen_address
        else:
            host = self.listener.bound_address[0]
            start_arg = self.listener.prefix + unparse_host_port(
                host, self._given_worker_port
            )

        if self.process is None:
            worker_kwargs = dict(
                scheduler_ip=self.scheduler_addr,
                ncores=self.ncores,
                local_dir=self.local_dir,
                services=self.services,
                service_ports={"nanny": self.port},
                name=self.name,
                memory_limit=self.memory_limit,
                reconnect=self.reconnect,
                resources=self.resources,
                validate=self.validate,
                silence_logs=self.silence_logs,
                death_timeout=self.death_timeout,
                preload=self.preload,
                preload_argv=self.preload_argv,
                security=self.security,
                contact_address=self.contact_address,
            )
            worker_kwargs.update(self.worker_kwargs)
            self.process = WorkerProcess(
                worker_args=tuple(),
                worker_kwargs=worker_kwargs,
                worker_start_args=(start_arg,),
                silence_logs=self.silence_logs,
                on_exit=self._on_exit,
                worker=self.Worker,
                env=self.env,
            )

        self.auto_restart = True
        if self.death_timeout:
            try:
                result = yield gen.with_timeout(
                    timedelta(seconds=self.death_timeout), self.process.start()
                )
            except gen.TimeoutError:
                yield self.close(timeout=self.death_timeout)
                raise gen.Return("timed out")
        else:
            result = yield self.process.start()
        raise gen.Return(result)

    @gen.coroutine
    def restart(self, comm=None, timeout=2, executor_wait=True):
        start = time()

        @gen.coroutine
        def _():
            if self.process is not None:
                yield self.kill()
                yield self.instantiate()

        try:
            yield gen.with_timeout(timedelta(seconds=timeout), _())
        except gen.TimeoutError:
            logger.error("Restart timed out, returning before finished")
            raise gen.Return("timed out")
        else:
            raise gen.Return("OK")

    def memory_monitor(self):
        """ Track worker's memory.  Restart if it goes above terminate fraction """
        if self.status != "running":
            return
        process = self.process.process
        if process is None:
            return
        try:
            proc = psutil.Process(process.pid)
        except psutil.NoSuchProcess:
            return
        memory = proc.memory_info().rss
        frac = memory / self.memory_limit
        if self.memory_terminate_fraction and frac > self.memory_terminate_fraction:
            logger.warning(
                "Worker exceeded %d%% memory budget. Restarting",
                100 * self.memory_terminate_fraction,
            )
            process.terminate()

    def is_alive(self):
        return self.process is not None and self.process.status == "running"

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    @gen.coroutine
    def _on_exit(self, exitcode):
        if self.status not in ("closing", "closed"):
            try:
                yield self.scheduler.unregister(address=self.worker_address)
            except (EnvironmentError, CommClosedError):
                if not self.reconnect:
                    yield self.close()
                    return

            try:
                if self.status not in ("closing", "closed"):
                    if self.auto_restart:
                        logger.warning("Restarting worker")
                        yield self.instantiate()
            except Exception:
                logger.error(
                    "Failed to restart worker after its process exited", exc_info=True
                )

    @property
    def pid(self):
        return self.process and self.process.pid

    def _close(self, *args, **kwargs):
        warnings.warn("Worker._close has moved to Worker.close", stacklevel=2)
        return self.close(*args, **kwargs)

    @gen.coroutine
    def close(self, comm=None, timeout=5, report=None):
        """
        Close the worker process, stop all comms.
        """
        if self.status in ("closing", "closed"):
            raise gen.Return("OK")
        self.status = "closing"
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
        self.status = "closed"
        raise gen.Return("OK")


class WorkerProcess(object):
    def __init__(
        self,
        worker_args,
        worker_kwargs,
        worker_start_args,
        silence_logs,
        on_exit,
        worker,
        env,
    ):
        self.status = "init"
        self.silence_logs = silence_logs
        self.worker_args = worker_args
        self.worker_kwargs = worker_kwargs
        self.worker_start_args = worker_start_args
        self.on_exit = on_exit
        self.process = None
        self.Worker = worker
        self.env = env

        # Initialized when worker is ready
        self.worker_dir = None
        self.worker_address = None

    @gen.coroutine
    def start(self):
        """
        Ensure the worker process is started.
        """
        enable_proctitle_on_children()
        if self.status == "running":
            raise gen.Return(self.status)
        if self.status == "starting":
            yield self.running.wait()
            raise gen.Return(self.status)

        self.init_result_q = init_q = mp_context.Queue()
        self.child_stop_q = mp_context.Queue()
        uid = uuid.uuid4().hex

        self.process = AsyncProcess(
            target=self._run,
            kwargs=dict(
                worker_args=self.worker_args,
                worker_kwargs=self.worker_kwargs,
                worker_start_args=self.worker_start_args,
                silence_logs=self.silence_logs,
                init_result_q=self.init_result_q,
                child_stop_q=self.child_stop_q,
                uid=uid,
                Worker=self.Worker,
                env=self.env,
            ),
        )
        self.process.daemon = True
        self.process.set_exit_callback(self._on_exit)
        self.running = Event()
        self.stopped = Event()
        self.status = "starting"
        yield self.process.start()
        msg = yield self._wait_until_connected(uid)
        if not msg:
            raise gen.Return(self.status)
        self.worker_address = msg["address"]
        self.worker_dir = msg["dir"]
        assert self.worker_address
        self.status = "running"
        self.running.set()

        init_q.close()

        raise gen.Return(self.status)

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
            return "Worker process %d exited with status %d" % (pid, exitcode)
        else:
            return "Worker process %d was killed by signal %d" % (pid, -exitcode)

    def is_alive(self):
        return self.process is not None and self.process.is_alive()

    @property
    def pid(self):
        return self.process.pid if self.process and self.process.is_alive() else None

    def mark_stopped(self):
        if self.status != "stopped":
            r = self.process.exitcode
            assert r is not None
            if r != 0:
                msg = self._death_message(self.process.pid, r)
                logger.warning(msg)
            self.status = "stopped"
            self.stopped.set()
            # Release resources
            self.process.close()
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
    def kill(self, timeout=2, executor_wait=True):
        """
        Ensure the worker process is stopped, waiting at most
        *timeout* seconds before terminating it abruptly.
        """
        loop = IOLoop.current()
        deadline = loop.time() + timeout

        if self.status == "stopped":
            return
        if self.status == "stopping":
            yield self.stopped.wait()
            return
        assert self.status in ("starting", "running")
        self.status = "stopping"

        process = self.process
        self.child_stop_q.put(
            {
                "op": "stop",
                "timeout": max(0, deadline - loop.time()) * 0.8,
                "executor_wait": executor_wait,
            }
        )
        self.child_stop_q.close()

        while process.is_alive() and loop.time() < deadline:
            yield gen.sleep(0.05)

        if process.is_alive():
            logger.warning(
                "Worker process still alive after %d seconds, killing", timeout
            )
            try:
                yield process.terminate()
            except Exception as e:
                logger.error("Failed to kill worker process: %s", e)

    @gen.coroutine
    def _wait_until_connected(self, uid):
        delay = 0.05
        while True:
            if self.status != "starting":
                return
            try:
                msg = self.init_result_q.get_nowait()
            except Empty:
                yield gen.sleep(delay)
                continue

            if msg["uid"] != uid:  # ensure that we didn't cross queues
                continue

            if "exception" in msg:
                logger.error(
                    "Failed while trying to start worker process: %s", msg["exception"]
                )
                yield self.process.join()
                raise msg
            else:
                raise gen.Return(msg)

    @classmethod
    def _run(
        cls,
        worker_args,
        worker_kwargs,
        worker_start_args,
        silence_logs,
        init_result_q,
        child_stop_q,
        uid,
        env,
        Worker,
    ):  # pragma: no cover
        os.environ.update(env)
        try:
            from dask.multiprocessing import initialize_worker_process
        except ImportError:  # old Dask version
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
        def do_stop(timeout=5, executor_wait=True):
            try:
                yield worker.close(
                    report=False,
                    nanny=False,
                    executor_wait=executor_wait,
                    timeout=timeout,
                )
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
                    child_stop_q.close()
                    assert msg.pop("op") == "stop"
                    loop.add_callback(do_stop, **msg)
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
                logger.exception("Failed to start worker")
                init_result_q.put({"uid": uid, "exception": e})
                init_result_q.close()
            else:
                assert worker.address
                init_result_q.put(
                    {"address": worker.address, "dir": worker.local_dir, "uid": uid}
                )
                init_result_q.close()
                yield worker.wait_until_closed()
                logger.info("Worker closed")

        try:
            loop.run_sync(run)
        except TimeoutError:
            # Loop was stopped before wait_until_closed() returned, ignore
            pass
        except KeyboardInterrupt:
            pass
