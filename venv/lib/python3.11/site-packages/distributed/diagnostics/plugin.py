from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import socket
import subprocess
import sys
import tempfile
import uuid
import zipfile
from collections.abc import Awaitable
from typing import TYPE_CHECKING, Any, Callable, ClassVar

from dask.typing import Key
from dask.utils import _deprecated_kwarg, funcname, tmpfile

from distributed.protocol.pickle import dumps

if TYPE_CHECKING:
    # circular imports
    # Needed to avoid Sphinx WARNING: more than one target found for cross-reference
    # 'WorkerState'"
    # https://github.com/agronholm/sphinx-autodoc-typehints#dealing-with-circular-imports
    from distributed import scheduler as scheduler_module
    from distributed.scheduler import Scheduler
    from distributed.scheduler import TaskStateState as SchedulerTaskStateState
    from distributed.worker import Worker
    from distributed.worker_state_machine import TaskStateState as WorkerTaskStateState

logger = logging.getLogger(__name__)


class SchedulerPlugin:
    """Interface to extend the Scheduler

    A plugin enables custom hooks to run when specific events occur.
    The scheduler will run the methods of this plugin whenever the corresponding
    method of the scheduler is run.  This runs user code within the scheduler
    thread that can perform arbitrary operations in synchrony with the scheduler
    itself.

    Plugins are often used for diagnostics and measurement, but have full
    access to the scheduler and could in principle affect core scheduling.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

    The ``idempotent`` attribute is used to control whether or not the plugin should
    be ignored upon registration if a scheduler plugin with the same name already exists.
    If ``True``, the plugin is ignored, otherwise the existing plugin is replaced.
    Defaults to ``False``.

    Examples
    --------
    >>> class Counter(SchedulerPlugin):
    ...     def __init__(self):
    ...         self.counter = 0
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if start == 'processing' and finish == 'memory':
    ...             self.counter += 1
    ...
    ...     def restart(self, scheduler):
    ...         self.counter = 0

    >>> plugin = Counter()
    >>> scheduler.add_plugin(plugin)  # doctest: +SKIP
    """

    idempotent: bool = False

    async def start(self, scheduler: Scheduler) -> None:
        """Run when the scheduler starts up

        This runs at the end of the Scheduler startup process
        """

    async def before_close(self) -> None:
        """Runs prior to any Scheduler shutdown logic"""

    async def close(self) -> None:
        """Run when the scheduler closes down

        This runs at the beginning of the Scheduler shutdown process, but after
        workers have been asked to shut down gracefully
        """

    def update_graph(
        self,
        scheduler: Scheduler,
        *,
        client: str,
        keys: set[Key],
        tasks: list[Key],
        annotations: dict[str, dict[Key, Any]],
        priority: dict[Key, tuple[int | float, ...]],
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        """Run when a new graph / tasks enter the scheduler

        Parameters
        ----------
            scheduler:
                The `Scheduler` instance.
            client:
                The unique Client id.
            keys:
                The keys the Client is interested in when calling `update_graph`.
            tasks:
                The
            annotations:
                Fully resolved annotations as applied to the tasks in the format::

                    {
                        "annotation": {
                            "key": "value,
                            ...
                        },
                        ...
                    }
            priority:
                Task calculated priorities as assigned to the tasks.
            stimulus_id:
                ID of the stimulus causing the graph update
            **kwargs:
                It is recommended to allow plugins to accept more parameters to
                ensure future compatibility.
        """

    def restart(self, scheduler: Scheduler) -> None:
        """Run when the scheduler restarts itself"""

    def transition(
        self,
        key: Key,
        start: SchedulerTaskStateState,
        finish: SchedulerTaskStateState,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        """Run whenever a task changes state

        For a description of the transition mechanism and the available states,
        see :ref:`Scheduler task states <scheduler-task-state>`.

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish :
            Final state of the transition.
        stimulus_id :
            ID of stimulus causing the transition.
        *args, **kwargs :
            More options passed when transitioning
            This may include worker ID, compute time, etc.
        """

    def add_worker(self, scheduler: Scheduler, worker: str) -> None | Awaitable[None]:
        """Run when a new worker enters the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.add_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.add_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.add_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None | Awaitable[None]:
        """Run when a worker leaves the cluster

        If this method is synchronous, it is immediately and synchronously executed
        without ``Scheduler.remove_worker`` ever yielding to the event loop.
        If it is asynchronous, it will be awaited after all synchronous
        ``SchedulerPlugin.remove_worker`` hooks have executed.

        .. warning::

            There are no guarantees about the execution order between individual
            ``SchedulerPlugin.remove_worker`` hooks and the ordering may be subject
            to change without deprecation cycle.
        """

    def add_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a new client connects"""

    def remove_client(self, scheduler: Scheduler, client: str) -> None:
        """Run when a client disconnects"""

    def valid_workers_downscaling(
        self, scheduler: Scheduler, workers: list[scheduler_module.WorkerState]
    ) -> list[scheduler_module.WorkerState]:
        """Determine which workers can be removed from the cluster

        This method is called when the scheduler is about to downscale the cluster
        by removing workers. The method should return a set of worker states that
        can be removed from the cluster.

        Parameters
        ----------
        workers : list
            The list of worker states that are candidates for removal.
        stimulus_id : str
            ID of stimulus causing the downscaling.

        Returns
        -------
        list
            The list of worker states that can be removed from the cluster.
        """
        return workers

    def log_event(self, topic: str, msg: Any) -> None:
        """Run when an event is logged"""


class WorkerPlugin:
    """Interface to extend the Worker

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle.

    A plugin enables custom code to run at each of step of a Workers's life. Whenever such
    an event happens, the corresponding method on this class will be called. Note that the
    user code always runs within the Worker's main thread.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

    The ``idempotent`` attribute is used to control whether or not the plugin should
    be ignored upon registration if a worker plugin with the same name already exists.
    If ``True``, the plugin is ignored, otherwise the existing plugin is replaced.
    Defaults to ``False``.

    Examples
    --------
    >>> class ErrorLogger(WorkerPlugin):
    ...     def __init__(self, logger):
    ...         self.logger = logger
    ...
    ...     def setup(self, worker):
    ...         self.worker = worker
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if finish == 'error':
    ...             ts = self.worker.tasks[key]
    ...             exc_info = (type(ts.exception), ts.exception, ts.traceback)
    ...             self.logger.error(
    ...                 "Error during computation of '%s'.", key,
    ...                 exc_info=exc_info
    ...             )

    >>> import logging
    >>> plugin = ErrorLogger(logging)
    >>> client.register_plugin(plugin)  # doctest: +SKIP
    """

    idempotent: bool = False

    def setup(self, worker: Worker) -> None | Awaitable[None]:
        """
        Run when the plugin is attached to a worker. This happens when the plugin is registered
        and attached to existing workers, or when a worker is created after the plugin has been
        registered.
        """

    def teardown(self, worker: Worker) -> None | Awaitable[None]:
        """Run when the worker to which the plugin is attached is closed, or
        when the plugin is removed."""

    def transition(
        self,
        key: Key,
        start: WorkerTaskStateState,
        finish: WorkerTaskStateState,
        **kwargs: Any,
    ) -> None:
        """
        Throughout the lifecycle of a task (see :doc:`Worker State
        <worker-state>`), Workers are instructed by the scheduler to compute
        certain tasks, resulting in transitions in the state of each task. The
        Worker owning the task is then notified of this state transition.

        Whenever a task changes its state, this method will be called.

        .. warning::

            This is an advanced feature and the transition mechanism and details
            of task states are subject to change without deprecation cycle.

        Parameters
        ----------
        key :
        start :
            Start state of the transition.
            One of waiting, ready, executing, long-running, memory, error.
        finish :
            Final state of the transition.
        kwargs :
            More options passed when transitioning
        """


class NannyPlugin:
    """Interface to extend the Nanny

    A worker plugin enables custom code to run at different stages of the Workers'
    lifecycle. A nanny plugin does the same thing, but benefits from being able
    to run code before the worker is started, or to restart the worker if
    necessary.

    To implement a plugin:

    1. inherit from this class
    2. override some of its methods
    3. register the plugin using :meth:`Client.register_plugin<distributed.Client.register_plugin>`.

    The ``idempotent`` attribute is used to control whether or not the plugin should
    be ignored upon registration if a nanny plugin with the same name already exists.
    If ``True``, the plugin is ignored, otherwise the existing plugin is replaced.
    Defaults to ``False``.

    The ``restart`` attribute is used to control whether or not a running ``Worker``
    needs to be restarted when registering the plugin.

    See Also
    --------
    WorkerPlugin
    SchedulerPlugin
    """

    idempotent: bool = False
    restart: bool = False

    def setup(self, nanny):
        """
        Run when the plugin is attached to a nanny. This happens when the plugin is registered
        and attached to existing nannies, or when a nanny is created after the plugin has been
        registered.
        """

    def teardown(self, nanny):
        """Run when the nanny to which the plugin is attached to is closed"""


def _get_plugin_name(plugin: SchedulerPlugin | WorkerPlugin | NannyPlugin) -> str:
    """Return plugin name.

    If plugin has no name attribute a random name is used.

    """
    if hasattr(plugin, "name"):
        return plugin.name
    else:
        return funcname(type(plugin)) + "-" + str(uuid.uuid4())


class SchedulerUploadFile(SchedulerPlugin):
    name = "upload_file"

    def __init__(self, filepath: str, load: bool = True):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        self.filename = os.path.basename(filepath)
        self.load = load
        with open(filepath, "rb") as f:
            self.data = f.read()

    async def start(self, scheduler: Scheduler) -> None:
        await scheduler.upload_file(self.filename, self.data, load=self.load)


class InstallPlugin(SchedulerPlugin):
    """Scheduler plugin to install software on the cluster

    This accepts an function that installs software on the scheduler and
    all workers. You can also optionally ask for the worker to restart
    after performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       software in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    install_fn
        Callable used to install the software; must be idempotent.
    restart_workers
        Whether or not to restart the worker after installing the packages
        Only functions if the worker has an attached nanny process

    See Also
    --------
    CondaInstall
    PipInstall
    """

    idempotent = True
    _lock: ClassVar[asyncio.Lock | None] = None

    _install_fn: Callable[[], None]
    name: str
    restart_workers: bool
    _scheduler: Scheduler

    def __init__(
        self,
        install_fn: Callable[[], None],
        restart_workers: bool,
    ):
        self._install_fn = install_fn
        self.restart_workers = restart_workers
        self.name = f"{self.__class__.__name__}-{uuid.uuid4()}"

    async def start(self, scheduler: Scheduler) -> None:
        from distributed.core import clean_exception
        from distributed.protocol.serialize import Serialized, deserialize

        self._scheduler = scheduler

        if InstallPlugin._lock is None:
            InstallPlugin._lock = asyncio.Lock()

        async with InstallPlugin._lock:
            self._install_fn()

            if self.restart_workers:
                nanny_plugin = _InstallNannyPlugin(self._install_fn, self.name)
                responses = await scheduler.register_nanny_plugin(
                    comm=None,
                    plugin=dumps(nanny_plugin),
                    name=self.name,
                    idempotent=True,
                )
            else:
                worker_plugin = _InstallWorkerPlugin(self._install_fn, self.name)
                responses = await scheduler.register_worker_plugin(
                    comm=None,
                    plugin=dumps(worker_plugin),
                    name=self.name,
                    idempotent=True,
                )
            for response in responses.values():
                if response["status"] == "error":
                    response = {  # type: ignore[unreachable]
                        k: deserialize(v.header, v.frames)
                        for k, v in response.items()
                        if isinstance(v, Serialized)
                    }
                    _, exc, tb = clean_exception(**response)
                    raise exc.with_traceback(tb)

    async def close(self) -> None:
        assert InstallPlugin._lock is not None

        async with InstallPlugin._lock:
            if self.restart_workers:
                await self._scheduler.unregister_nanny_plugin(comm=None, name=self.name)
            else:
                await self._scheduler.unregister_worker_plugin(
                    comm=None, name=self.name
                )


class _InstallNannyPlugin(NannyPlugin):
    """Nanny plugin to install software on the nanny and then restart the workers

    This accepts an installer which will installs software on all nannies.

    .. note::

       This will increase the time it takes to start up
       the cluster. If possible, we recommend including the
       software in the cluster environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    install_fn
        Function that installs software
    name:
        Name of the plugin

    See Also
    --------
    InstallPlugin
    """

    _install_fn: Callable[[], None]
    name: str
    restart = True

    def __init__(self, _install_fn: Callable[[], None], name: str):
        self._install_fn = _install_fn
        self.name = name

    async def setup(self, nanny):
        from distributed.semaphore import Semaphore

        async with await Semaphore(
            max_leases=1,
            name=socket.gethostname(),
            scheduler_rpc=nanny.scheduler,
            loop=nanny.loop,
        ):
            self._install_fn()


class _InstallWorkerPlugin(WorkerPlugin):
    """Worker plugin to install software on the worker

    This accepts an installer which will installs software on all workers.

    .. note::

       This will increase the time it takes to start up
       the cluster. If possible, we recommend including the
       software in the cluster environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    install_fn
        Callable that installs software
    name:
        Name of the plugin

    See Also
    --------
    InstallPlugin
    """

    _install_fn: Callable[[], None]
    name: str

    def __init__(self, install_fn: Callable[[], None], name: str):
        self._install_fn = install_fn
        self.name = name

    async def setup(self, worker):
        from distributed.semaphore import Semaphore

        async with await Semaphore(
            max_leases=1,
            name=socket.gethostname(),
            scheduler_rpc=worker.scheduler,
            loop=worker.loop,
        ):
            self._install_fn()


class CondaInstall(InstallPlugin):
    """A plugin to conda install a set of packages

    This accepts a set of packages to install on the scheduler and all
    workers as well as options to use when installing.

    You can also optionally ask for the workers to restart after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       the cluster. If possible, we recommend including the
       libraries in the cluster environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    packages
        A list of packages (with optional versions) to install using conda
    conda_options
        Additional options to pass to conda
    restart_workers
        Whether or not to restart the worker after installing the packages
        Only functions if the workers have an attached nanny process

    Examples
    --------
    >>> from dask.distributed import CondaInstall
    >>> plugin = CondaInstall(packages=["scikit-learn"], conda_options=["--update-deps"])

    >>> client.register_plugin(plugin)

    See Also
    --------
    InstallPlugin
    PipInstall
    """

    def __init__(
        self,
        packages: list[str],
        conda_options: list[str] | None = None,
        restart_workers: bool = False,
    ):
        installer = _CondaInstaller(packages, conda_options)
        super().__init__(installer, restart_workers=restart_workers)


class _CondaInstaller:
    INSTALLER = "conda"

    packages: list[str]
    conda_options: list[str]

    def __init__(self, packages: list[str], conda_options: list[str] | None = None):
        self.packages = packages
        self.conda_options = conda_options or []

    def __call__(self) -> None:
        logger.info(
            "%s installing the following packages: %s",
            self.INSTALLER,
            self.packages,
        )
        try:
            from conda.cli.python_api import Commands, run_command
        except ModuleNotFoundError as e:  # pragma: nocover
            msg = (
                "conda install failed because conda could not be found. "
                "Please make sure that conda is installed."
            )
            logger.error(msg)
            raise RuntimeError(msg) from e
        try:
            _, stderr, returncode = run_command(
                Commands.INSTALL, self.conda_options + self.packages
            )
        except Exception as e:
            msg = "conda install failed"
            logger.error(msg)
            raise RuntimeError(msg) from e

        if returncode != 0:
            msg = f"conda install failed with '{stderr.decode().strip()}'"
            logger.error(msg)
            raise RuntimeError(msg)


class PipInstall(InstallPlugin):
    """A plugin to pip install a set of packages

    This accepts a set of packages to install on all workers as well as
    options to use when installing.
    You can also optionally ask for the worker to restart itself after
    performing this installation.

    .. note::

       This will increase the time it takes to start up
       each worker. If possible, we recommend including the
       libraries in the worker environment or image. This is
       primarily intended for experimentation and debugging.

    Parameters
    ----------
    packages
        A list of packages to install using pip.
        Packages should follow the structure defined for
        `requirement files <https://pip.pypa.io/en/stable/reference/requirements-file-format/#structure>`_.
        Packages also may include
        `environment variables <https://pip.pypa.io/en/stable/reference/requirements-file-format/#using-environment-variables>`_.
    pip_options
        Additional options to pass to pip
    restart_workers
        Whether or not to restart the worker after installing the packages;
        only functions if the worker has an attached nanny process.

    Examples
    --------
    >>> from dask.distributed import PipInstall
    >>> plugin = PipInstall(packages=["scikit-learn"], pip_options=["--upgrade"])
    >>> client.register_plugin(plugin)

    Install package from a private repository using a ``TOKEN`` environment variable.

    >>> from dask.distributed import PipInstall
    >>> plugin = PipInstall(packages=["private_package@git+https://${TOKEN}@github.com/dask/private_package.git])
    >>> client.register_plugin(plugin)

    See Also
    --------
    InstallPlugin
    CondaInstall
    """

    def __init__(
        self,
        packages: list[str],
        pip_options: list[str] | None = None,
        restart_workers: bool = False,
    ):
        installer = _PipInstaller(packages, pip_options)
        super().__init__(installer, restart_workers=restart_workers)


class _PipInstaller:
    INSTALLER = "pip"

    packages: list[str]
    pip_options: list[str]

    def __init__(self, packages: list[str], pip_options: list[str] | None = None):
        self.packages = packages
        self.pip_options = pip_options or []

    def __call__(self) -> None:
        logger.info(
            "%s installing the following packages: %s",
            self.INSTALLER,
            self.packages,
        )
        # Use a requirements file under the hood to support
        # environment variables
        # See https://pip.pypa.io/en/stable/reference/requirements-file-format/#using-environment-variables
        with tempfile.NamedTemporaryFile(mode="w+") as f:
            f.write("\n".join(self.packages))
            f.flush()
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "pip",
                    "install",
                    *self.pip_options,
                    "-r",
                    f.name,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = proc.communicate()
            returncode = proc.wait()
            if returncode != 0:
                msg = f"pip install failed with '{stderr.decode().strip()}'"
                logger.error(msg)
                raise RuntimeError(msg)


# Adapted from https://github.com/dask/distributed/issues/3560#issuecomment-596138522
class UploadFile(WorkerPlugin):
    """A WorkerPlugin to upload a local file to workers.

    Parameters
    ----------
    filepath: str
        A path to the file (.py, egg, or zip) to upload

    Examples
    --------
    >>> from distributed.diagnostics.plugin import UploadFile

    >>> client.register_plugin(UploadFile("/path/to/file.py"))  # doctest: +SKIP
    """

    name = "upload_file"

    def __init__(self, filepath: str, load: bool = True):
        """
        Initialize the plugin by reading in the data from the given file.
        """
        self.filename = os.path.basename(filepath)
        self.load = load
        with open(filepath, "rb") as f:
            self.data = f.read()

    async def setup(self, worker):
        response = await worker.upload_file(
            filename=self.filename, data=self.data, load=self.load
        )
        assert len(self.data) == response["nbytes"]


class ForwardLoggingPlugin(WorkerPlugin):
    """
    A ``WorkerPlugin`` to forward python logging records from worker to client.
    See :meth:`Client.forward_logging` for full documentation and usage. Needs
    to be used in coordination with :meth:`Client.subscribe_topic`, the details
    of which :meth:`Client.forward_logging` handles for you.

    Parameters
    ----------
    logger_name : str
        The name of the logger to begin forwarding.

    level : str | int
        Optionally restrict forwarding to ``LogRecord``s of this level or
        higher, even if the forwarded logger's own level is lower.

    topic : str
        The name of the topic to which to the worker should log the forwarded log
        records.
    """

    def __init__(self, logger_name, level, topic):
        self.logger_name = logger_name
        self.level = level
        self.topic = topic
        self.handler = None

    def setup(self, worker):
        self.handler = _ForwardingLogHandler(worker, self.topic, level=self.level)
        logger = logging.getLogger(self.logger_name)
        logger.addHandler(self.handler)

    def teardown(self, worker):
        if self.handler is not None:
            logger = logging.getLogger(self.logger_name)
            logger.removeHandler(self.handler)


class _ForwardingLogHandler(logging.Handler):
    """
    Handler class that gets installed inside workers by
    :class:`ForwardLoggingPlugin`. Not intended to be instantiated by the user
    directly.

    In each affected worker, ``ForwardLoggingPlugin`` adds an instance of this
    handler to one or more loggers (possibly the root logger). Tasks running on
    the worker may then use the affected logger as normal, with the side effect
    that any ``LogRecord``s handled by the logger (or by a logger below it in
    the hierarchy) will be published to the dask client as a
    ``topic`` event.
    """

    def __init__(self, worker, topic, level=logging.NOTSET):
        super().__init__(level)
        self.worker = worker
        self.topic = topic

    def prepare_record_attributes(self, record):
        # Adapted from the CPython standard library's
        # logging.handlers.SocketHandler.makePickle; see its source at:
        # https://github.com/python/cpython/blob/main/Lib/logging/handlers.py
        ei = record.exc_info
        if ei:
            # just to get traceback text into record.exc_text ...
            _ = self.format(record)
        # If msg or args are objects, they may not be available on the receiving
        # end. So we convert the msg % args to a string, save it as msg and zap
        # the args.
        d = dict(record.__dict__)
        d["msg"] = record.getMessage()
        d["args"] = None
        d["exc_info"] = None
        # delete 'message' if present: redundant with 'msg'
        d.pop("message", None)
        return d

    def emit(self, record):
        attributes = self.prepare_record_attributes(record)
        self.worker.log_event(self.topic, attributes)


class Environ(NannyPlugin):
    restart = True

    def __init__(self, environ: dict | None = None):
        environ = environ or {}
        self.environ = {k: str(v) for k, v in environ.items()}

    async def setup(self, nanny):
        nanny.env.update(self.environ)


UPLOAD_DIRECTORY_MODES = ["all", "scheduler", "workers"]


class UploadDirectory(SchedulerPlugin):
    """Scheduler to upload a local directory to the cluster.

    Parameters
    ----------
    path:
        Path to the directory to upload
    scheduler:
        Whether to upload the directory to the scheduler

    Examples
    --------
    >>> from distributed.diagnostics.plugin import UploadDirectory
    >>> client.register_plugin(UploadDirectory("/path/to/directory"))  # doctest: +SKIP
    """

    @_deprecated_kwarg("restart", "restart_workers")
    def __init__(
        self,
        path,
        restart_workers=False,
        update_path=False,
        skip_words=(".git", ".github", ".pytest_cache", "tests", "docs"),
        skip=(lambda fn: os.path.splitext(fn)[1] == ".pyc",),
        mode="workers",
    ):
        path = os.path.expanduser(path)
        self.path = os.path.split(path)[-1]
        self.restart_workers = restart_workers
        self.update_path = update_path

        if mode not in UPLOAD_DIRECTORY_MODES:
            raise ValueError(
                f"{mode=} not supported, expected one of {UPLOAD_DIRECTORY_MODES}"
            )
        self.mode = mode

        self.name = "upload-directory-" + os.path.split(path)[-1]

        with tmpfile(extension="zip") as fn:
            with zipfile.ZipFile(fn, "w", zipfile.ZIP_DEFLATED) as z:
                for root, dirs, files in os.walk(path):
                    for file in files:
                        filename = os.path.join(root, file)
                        if any(predicate(filename) for predicate in skip):
                            continue
                        dirs = filename.split(os.sep)
                        if any(word in dirs for word in skip_words):
                            continue

                        archive_name = os.path.relpath(
                            os.path.join(root, file), os.path.join(path, "..")
                        )
                        z.write(filename, archive_name)

            with open(fn, mode="rb") as f:
                self.data = f.read()

    async def start(self, scheduler):
        from distributed.core import clean_exception
        from distributed.protocol.serialize import Serialized, deserialize

        if self.mode in ("all", "scheduler"):
            _extract_data(
                scheduler.local_directory, self.path, self.data, self.update_graph
            )

        if self.mode in ("all", "workers"):
            nanny_plugin = _UploadDirectoryNannyPlugin(
                self.path, self.data, self.restart_workers, self.update_path, self.name
            )
            responses = await scheduler.register_nanny_plugin(
                comm=None,
                plugin=dumps(nanny_plugin),
                name=self.name,
                idempotent=False,
            )

            for response in responses.values():
                if response["status"] == "error":
                    response = {
                        k: deserialize(v.header, v.frames)
                        for k, v in response.items()
                        if isinstance(v, Serialized)
                    }
                    _, exc, tb = clean_exception(**response)
                    raise exc.with_traceback(tb)


class _UploadDirectoryNannyPlugin(NannyPlugin):
    def __init__(self, path, data, restart, update_path, name):
        self.path = path
        self.data = data
        self.name = name
        self.restart = restart
        self.update_path = update_path

    def setup(self, nanny):
        _extract_data(nanny.local_directory, self.path, self.data, self.update_path)


def _extract_data(base_path, path, data, update_path):
    with tmpfile(extension="zip") as fn:
        with open(fn, mode="wb") as f:
            f.write(data)

        import zipfile

        with zipfile.ZipFile(fn) as z:
            z.extractall(path=base_path)

        if update_path:
            path = os.path.join(base_path, path)
            if path not in sys.path:
                sys.path.insert(0, path)


class forward_stream:
    def __init__(self, stream, worker):
        self._worker = worker
        self._original_methods = {}
        self._stream = getattr(sys, stream)
        if stream == "stdout":
            self._file = 1
        elif stream == "stderr":
            self._file = 2
        else:
            raise ValueError(
                f"Expected stream to be 'stdout' or 'stderr'; got '{stream}'"
            )

        self._file = 1 if stream == "stdout" else 2
        self._buffer = []

    def _write(self, write_fn, data):
        self._forward(data)
        write_fn(data)

    def _forward(self, data):
        self._buffer.append(data)
        # Mimic line buffering
        if "\n" in data or "\r" in data:
            self._send()

    def _send(self):
        msg = {"args": self._buffer, "file": self._file, "sep": "", "end": ""}
        self._worker.log_event("print", msg)
        self._buffer = []

    def _flush(self, flush_fn):
        self._send()
        flush_fn()

    def _close(self, close_fn):
        self._send()
        close_fn()

    def _intercept(self, method_name, interceptor):
        original_method = getattr(self._stream, method_name)
        self._original_methods[method_name] = original_method
        setattr(
            self._stream, method_name, functools.partial(interceptor, original_method)
        )

    def __enter__(self):
        self._intercept("write", self._write)
        self._intercept("flush", self._flush)
        self._intercept("close", self._close)
        return self._stream

    def __exit__(self, exc_type, exc_value, traceback):
        self._stream.flush()
        for attr, original in self._original_methods.items():
            setattr(self._stream, attr, original)
        self._original_methods = {}


class ForwardOutput(WorkerPlugin):
    """A Worker Plugin that forwards ``stdout`` and ``stderr`` from workers to clients

    This plugin forwards all output sent to ``stdout`` and ``stderr` on all workers
    to all clients where it is written to the respective streams. Analogous to the
    terminal, this plugin uses line buffering. To ensure that an output is written
    without a newline, make sure to flush the stream.

    .. warning::

        Using this plugin will forward **all** output in ``stdout`` and ``stderr`` from
        every worker to every client. If the output is very chatty, this will add
        significant strain on the scheduler. Proceed with caution!

    Examples
    --------
    >>> from dask.distributed import ForwardOutput
    >>> plugin = ForwardOutput()

    >>> client.register_plugin(plugin)
    """

    def setup(self, worker):
        self._exit_stack = contextlib.ExitStack()
        self._exit_stack.enter_context(forward_stream("stdout", worker=worker))
        self._exit_stack.enter_context(forward_stream("stderr", worker=worker))

    def teardown(self, worker):
        self._exit_stack.close()
