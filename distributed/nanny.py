import asyncio
from contextlib import suppress
import errno
import logging
from multiprocessing.queues import Empty
import os
import psutil
import shutil
import threading
import uuid
import warnings
import weakref

import dask
from dask.system import CPU_COUNT
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado import gen

from .comm import get_address_host, unparse_host_port
from .comm.addressing import address_from_user_args
from .core import RPCClosed, CommClosedError, coerce_to_address, Status
from .metrics import time
from .node import ServerNode
from . import preloading
from .process import AsyncProcess
from .proctitle import enable_proctitle_on_children
from .security import Security
from .utils import (
    get_ip,
    mp_context,
    silence_logging,
    json_load_robust,
    parse_timedelta,
    parse_ports,
    TimeoutError,
)
from .worker import run, parse_memory_limit, Worker


logger = logging.getLogger(__name__)


class Nanny(ServerNode):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary. It is necessary if you want to use the
    ``Client.restart`` method, or to restart the worker automatically if
    it gets to the terminate fractiom of its memory limit.

    The parameters for the Nanny are mostly the same as those for the Worker.

    See Also
    --------
    Worker
    """

    _instances = weakref.WeakSet()
    process = None
    status = Status.undefined

    def __init__(
        self,
        scheduler_ip=None,
        scheduler_port=None,
        scheduler_file=None,
        worker_port=0,
        nthreads=None,
        ncores=None,
        loop=None,
        local_dir=None,
        local_directory=None,
        services=None,
        name=None,
        memory_limit="auto",
        reconnect=True,
        validate=False,
        quiet=False,
        resources=None,
        silence_logs=None,
        death_timeout=None,
        preload=None,
        preload_argv=None,
        preload_nanny=None,
        preload_nanny_argv=None,
        security=None,
        contact_address=None,
        listen_address=None,
        worker_class=None,
        env=None,
        interface=None,
        host=None,
        port=None,
        protocol=None,
        config=None,
        **worker_kwargs,
    ):
        self._setup_logging(logger)
        self.loop = loop or IOLoop.current()

        if isinstance(security, dict):
            security = Security(**security)
        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args("worker")

        if scheduler_file:
            cfg = json_load_robust(scheduler_file)
            self.scheduler_addr = cfg["address"]
        elif scheduler_ip is None and dask.config.get("scheduler-address"):
            self.scheduler_addr = dask.config.get("scheduler-address")
        elif scheduler_port is None:
            self.scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            self.scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))

        if protocol is None:
            protocol_address = self.scheduler_addr.split("://")
            if len(protocol_address) == 2:
                protocol = protocol_address[0]

        if ncores is not None:
            warnings.warn("the ncores= parameter has moved to nthreads=")
            nthreads = ncores

        self._given_worker_port = worker_port
        self.nthreads = nthreads or CPU_COUNT
        self.reconnect = reconnect
        self.validate = validate
        self.resources = resources
        self.death_timeout = parse_timedelta(death_timeout)

        self.preload = preload
        if self.preload is None:
            self.preload = dask.config.get("distributed.worker.preload")
        self.preload_argv = preload_argv
        if self.preload_argv is None:
            self.preload_argv = dask.config.get("distributed.worker.preload-argv")

        if preload_nanny is None:
            preload_nanny = dask.config.get("distributed.nanny.preload")
        if preload_nanny_argv is None:
            preload_nanny_argv = dask.config.get("distributed.nanny.preload-argv")

        self.Worker = Worker if worker_class is None else worker_class
        self.env = env or {}
        self.config = config or {}
        worker_kwargs.update(
            {
                "port": worker_port,
                "interface": interface,
                "protocol": protocol,
                "host": host,
            }
        )
        self.worker_kwargs = worker_kwargs

        self.contact_address = contact_address
        self.memory_terminate_fraction = dask.config.get(
            "distributed.worker.memory.terminate"
        )

        if local_dir is not None:
            warnings.warn("The local_dir keyword has moved to local_directory")
            local_directory = local_dir

        if local_directory is None:
            local_directory = dask.config.get("temporary-directory") or os.getcwd()
            if not os.path.exists(local_directory):
                os.makedirs(local_directory)
            local_directory = os.path.join(local_directory, "dask-worker-space")

        self.local_directory = local_directory

        self.preloads = preloading.process_preloads(
            self, preload_nanny, preload_nanny_argv, file_dir=self.local_directory
        )

        self.services = services
        self.name = name
        self.quiet = quiet
        self.auto_restart = True

        self.memory_limit = parse_memory_limit(memory_limit, self.nthreads)

        if silence_logs:
            silence_logging(level=silence_logs)
        self.silence_logs = silence_logs

        handlers = {
            "instantiate": self.instantiate,
            "kill": self.kill,
            "restart": self.restart,
            # cannot call it 'close' on the rpc side for naming conflict
            "get_logs": self.get_logs,
            "terminate": self.close,
            "close_gracefully": self.close_gracefully,
            "run": self.run,
        }

        super(Nanny, self).__init__(
            handlers=handlers, io_loop=self.loop, connection_args=self.connection_args
        )

        self.scheduler = self.rpc(self.scheduler_addr)

        if self.memory_limit:
            pc = PeriodicCallback(self.memory_monitor, 100)
            self.periodic_callbacks["memory"] = pc

        if (
            not host
            and not interface
            and not self.scheduler_addr.startswith("inproc://")
        ):
            host = get_ip(get_address_host(self.scheduler.address))

        self._start_port = port
        self._start_host = host
        self._interface = interface
        self._protocol = protocol

        self._listen_address = listen_address
        Nanny._instances.add(self)
        self.status = Status.init

    def __repr__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.nthreads)

    async def _unregister(self, timeout=10):
        if self.process is None:
            return
        worker_address = self.process.worker_address
        if worker_address is None:
            return

        allowed_errors = (TimeoutError, CommClosedError, EnvironmentError, RPCClosed)
        with suppress(allowed_errors):
            await asyncio.wait_for(
                self.scheduler.unregister(address=self.worker_address), timeout
            )

    @property
    def worker_address(self):
        return None if self.process is None else self.process.worker_address

    @property
    def worker_dir(self):
        return None if self.process is None else self.process.worker_dir

    @property
    def local_dir(self):
        """ For API compatibility with Nanny """
        warnings.warn("The local_dir attribute has moved to local_directory")
        return self.local_directory

    async def start(self):
        """ Start nanny, start local process, start watching """

        await super().start()

        ports = parse_ports(self._start_port)
        for port in ports:
            start_address = address_from_user_args(
                host=self._start_host,
                port=port,
                interface=self._interface,
                protocol=self._protocol,
                security=self.security,
            )
            try:
                await self.listen(
                    start_address, **self.security.get_listen_args("worker")
                )
            except OSError as e:
                if len(ports) > 1 and e.errno == errno.EADDRINUSE:
                    continue
                else:
                    raise e
            else:
                self._start_address = start_address
                break
        else:
            raise ValueError(
                f"Could not start Nanny on host {self._start_host}"
                f"with port {self._start_port}"
            )

        self.ip = get_address_host(self.address)

        for preload in self.preloads:
            await preload.start()

        logger.info("        Start Nanny at: %r", self.address)
        response = await self.instantiate()
        if response == "running":
            assert self.worker_address
            self.status = Status.running
        else:
            await self.close()

        self.start_periodic_callbacks()

        return self

    async def kill(self, comm=None, timeout=2):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        self.auto_restart = False
        if self.process is None:
            return "OK"

        deadline = self.loop.time() + timeout
        await self.process.kill(timeout=0.8 * (deadline - self.loop.time()))

    async def instantiate(self, comm=None):
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
                nthreads=self.nthreads,
                local_directory=self.local_directory,
                services=self.services,
                nanny=self.address,
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
                worker_kwargs=worker_kwargs,
                worker_start_args=(start_arg,),
                silence_logs=self.silence_logs,
                on_exit=self._on_exit_sync,
                worker=self.Worker,
                env=self.env,
                config=self.config,
            )

        self.auto_restart = True
        if self.death_timeout:
            try:
                result = await asyncio.wait_for(
                    self.process.start(), self.death_timeout
                )
            except TimeoutError:
                await self.close(timeout=self.death_timeout)
                logger.error(
                    "Timed out connecting Nanny '%s' to scheduler '%s'",
                    self,
                    self.scheduler_addr,
                )
                raise

        else:
            result = await self.process.start()
        return result

    async def restart(self, comm=None, timeout=2, executor_wait=True):
        start = time()

        async def _():
            if self.process is not None:
                await self.kill()
                await self.instantiate()

        try:
            await asyncio.wait_for(_(), timeout)
        except TimeoutError:
            logger.error("Restart timed out, returning before finished")
            return "timed out"
        else:
            return "OK"

    @property
    def _psutil_process(self):
        pid = self.process.process.pid
        try:
            proc = self._psutil_process_obj
        except AttributeError:
            self._psutil_process_obj = psutil.Process(pid)

        if self._psutil_process_obj.pid != pid:
            self._psutil_process_obj = psutil.Process(pid)

        return self._psutil_process_obj

    def memory_monitor(self):
        """ Track worker's memory.  Restart if it goes above terminate fraction """
        if self.status != Status.running:
            return
        process = self.process.process
        if process is None:
            return
        try:
            proc = self._psutil_process
            memory = proc.memory_info().rss
        except (ProcessLookupError, psutil.NoSuchProcess, psutil.AccessDenied):
            return
        frac = memory / self.memory_limit

        if self.memory_terminate_fraction and frac > self.memory_terminate_fraction:
            logger.warning(
                "Worker exceeded %d%% memory budget. Restarting",
                100 * self.memory_terminate_fraction,
            )
            process.terminate()

    def is_alive(self):
        return self.process is not None and self.process.is_alive()

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    def _on_exit_sync(self, exitcode):
        self.loop.add_callback(self._on_exit, exitcode)

    async def _on_exit(self, exitcode):
        if self.status not in (Status.closing, Status.closed):
            try:
                await self.scheduler.unregister(address=self.worker_address)
            except (EnvironmentError, CommClosedError):
                if not self.reconnect:
                    await self.close()
                    return

            try:
                if self.status not in (
                    Status.closing,
                    Status.closed,
                    Status.closing_gracefully,
                ):
                    if self.auto_restart:
                        logger.warning("Restarting worker")
                        await self.instantiate()
                elif self.status == Status.closing_gracefully:
                    await self.close()

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

    def close_gracefully(self, comm=None):
        """
        A signal that we shouldn't try to restart workers if they go away

        This is used as part of the cluster shutdown process.
        """
        self.status = Status.closing_gracefully

    async def close(self, comm=None, timeout=5, report=None):
        """
        Close the worker process, stop all comms.
        """
        if self.status == Status.closing:
            await self.finished()
            assert self.status == Status.closed

        if self.status == Status.closed:
            return "OK"

        self.status = Status.closing
        logger.info("Closing Nanny at %r", self.address)

        for preload in self.preloads:
            await preload.teardown()

        self.stop()
        try:
            if self.process is not None:
                await self.kill(timeout=timeout)
        except Exception:
            pass
        self.process = None
        await self.rpc.close()
        self.status = Status.closed
        if comm:
            await comm.write("OK")
        await ServerNode.close(self)


class WorkerProcess:
    def __init__(
        self,
        worker_kwargs,
        worker_start_args,
        silence_logs,
        on_exit,
        worker,
        env,
        config,
    ):
        self.status = Status.init
        self.silence_logs = silence_logs
        self.worker_kwargs = worker_kwargs
        self.worker_start_args = worker_start_args
        self.on_exit = on_exit
        self.process = None
        self.Worker = worker
        self.env = env
        self.config = config

        # Initialized when worker is ready
        self.worker_dir = None
        self.worker_address = None

    async def start(self):
        """
        Ensure the worker process is started.
        """
        enable_proctitle_on_children()
        if self.status == Status.running:
            return self.status
        if self.status == Status.starting:
            await self.running.wait()
            return self.status

        self.init_result_q = init_q = mp_context.Queue()
        self.child_stop_q = mp_context.Queue()
        uid = uuid.uuid4().hex

        self.process = AsyncProcess(
            target=self._run,
            name="Dask Worker process (from Nanny)",
            kwargs=dict(
                worker_kwargs=self.worker_kwargs,
                worker_start_args=self.worker_start_args,
                silence_logs=self.silence_logs,
                init_result_q=self.init_result_q,
                child_stop_q=self.child_stop_q,
                uid=uid,
                Worker=self.Worker,
                env=self.env,
                config=self.config,
            ),
        )
        self.process.daemon = dask.config.get("distributed.worker.daemon", default=True)
        self.process.set_exit_callback(self._on_exit)
        self.running = asyncio.Event()
        self.stopped = asyncio.Event()
        self.status = Status.starting

        try:
            await self.process.start()
        except OSError:
            logger.exception("Nanny failed to start process", exc_info=True)
            self.process.terminate()
            return

        msg = await self._wait_until_connected(uid)
        if not msg:
            return self.status
        self.worker_address = msg["address"]
        self.worker_dir = msg["dir"]
        assert self.worker_address
        self.status = "running"
        self.running.set()

        init_q.close()

        return self.status

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
        if self.status != Status.stopped:
            r = self.process.exitcode
            assert r is not None
            if r != 0:
                msg = self._death_message(self.process.pid, r)
                logger.info(msg)
            self.status = Status.stopped
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

    async def kill(self, timeout=2, executor_wait=True):
        """
        Ensure the worker process is stopped, waiting at most
        *timeout* seconds before terminating it abruptly.
        """
        loop = IOLoop.current()
        deadline = loop.time() + timeout

        if self.status == Status.stopped:
            return
        if self.status == Status.stopping:
            await self.stopped.wait()
            return
        assert self.status in (Status.starting, Status.running)
        self.status = Status.stopping

        process = self.process
        self.child_stop_q.put(
            {
                "op": "stop",
                "timeout": max(0, deadline - loop.time()) * 0.8,
                "executor_wait": executor_wait,
            }
        )
        await asyncio.sleep(0)  # otherwise we get broken pipe errors
        self.child_stop_q.close()

        while process.is_alive() and loop.time() < deadline:
            await asyncio.sleep(0.05)

        if process.is_alive():
            logger.warning(
                "Worker process still alive after %d seconds, killing", timeout
            )
            try:
                await process.terminate()
            except Exception as e:
                logger.error("Failed to kill worker process: %s", e)

    async def _wait_until_connected(self, uid):
        delay = 0.05
        while True:
            if self.status != Status.starting:
                return
            try:
                msg = self.init_result_q.get_nowait()
            except Empty:
                await asyncio.sleep(delay)
                continue

            if msg["uid"] != uid:  # ensure that we didn't cross queues
                continue

            if "exception" in msg:
                logger.error(
                    "Failed while trying to start worker process: %s", msg["exception"]
                )
                await self.process.join()
                raise msg["exception"]
            else:
                return msg

    @classmethod
    def _run(
        cls,
        worker_kwargs,
        worker_start_args,
        silence_logs,
        init_result_q,
        child_stop_q,
        uid,
        env,
        config,
        Worker,
    ):  # pragma: no cover
        os.environ.update(env)
        dask.config.set(config)
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
        worker = Worker(**worker_kwargs)

        async def do_stop(timeout=5, executor_wait=True):
            try:
                await worker.close(
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

        async def run():
            """
            Try to start worker and inform parent of outcome.
            """
            try:
                await worker
            except Exception as e:
                logger.exception("Failed to start worker")
                init_result_q.put({"uid": uid, "exception": e})
                init_result_q.close()
            else:
                try:
                    assert worker.address
                except ValueError:
                    pass
                else:
                    init_result_q.put(
                        {
                            "address": worker.address,
                            "dir": worker.local_directory,
                            "uid": uid,
                        }
                    )
                    init_result_q.close()
                    await worker.finished()
                    logger.info("Worker closed")

        try:
            loop.run_sync(run)
        except (TimeoutError, gen.TimeoutError):
            # Loop was stopped before wait_until_closed() returned, ignore
            pass
        except KeyboardInterrupt:
            # At this point the loop is not running thus we have to run
            # do_stop() explicitly.
            loop.run_sync(do_stop)
