from __future__ import annotations

import copy
import logging
import sys
import warnings
import weakref
from json import dumps
from typing import Any

import dask
import dask.config

from distributed.deploy.spec import ProcessInterface, SpecCluster

logger = logging.getLogger(__name__)


class Process(ProcessInterface):
    """A superclass for SSH Workers and Nannies

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self, **kwargs):
        self.connection = None
        self.proc = None
        super().__init__(**kwargs)

    async def start(self):
        assert self.connection
        weakref.finalize(
            self, self.proc.kill
        )  # https://github.com/ronf/asyncssh/issues/112
        await super().start()

    async def close(self):
        if self.proc:
            self.proc.kill()  # https://github.com/ronf/asyncssh/issues/112
        if self.connection:
            self.connection.close()
        await super().close()


class Worker(Process):
    """A Remote Dask Worker controlled by SSH

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    address: str
        The hostname where we should run this worker
    worker_class: str
        The python class to use to create the worker.
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this worker.
    kwargs: dict
        These will be passed through the dask worker CLI to the
        dask.distributed.Worker class
    """

    def __init__(  # type: ignore[no-untyped-def]
        self,
        scheduler: str,
        address: str,
        connect_options: dict,
        kwargs: dict,
        worker_module="deprecated",
        worker_class="distributed.Nanny",
        remote_python=None,
        loop=None,
        name=None,
    ):
        super().__init__()

        if worker_module != "deprecated":
            raise ValueError(
                "worker_module has been deprecated in favor of worker_class. "
                "Please specify a Python class rather than a CLI module."
            )

        self.address = address
        self.scheduler = scheduler
        self.worker_class = worker_class
        self.connect_options = connect_options
        self.kwargs = copy.copy(kwargs)
        self.name = name
        self.remote_python = remote_python
        if kwargs.get("nprocs") is not None and kwargs.get("n_workers") is not None:
            raise ValueError(
                "Both nprocs and n_workers were specified. Use n_workers only."
            )
        elif kwargs.get("nprocs") is not None:
            warnings.warn(
                "The nprocs argument will be removed in a future release. It has been "
                "renamed to n_workers.",
                FutureWarning,
            )
            self.n_workers = self.kwargs.pop("nprocs", 1)
        else:
            self.n_workers = self.kwargs.pop("n_workers", 1)

    @property
    def nprocs(self):
        warnings.warn(
            "The nprocs attribute will be removed in a future release. It has been "
            "renamed to n_workers.",
            FutureWarning,
        )
        return self.n_workers

    @nprocs.setter
    def nprocs(self, value):
        warnings.warn(
            "The nprocs attribute will be removed in a future release. It has been "
            "renamed to n_workers.",
            FutureWarning,
        )
        self.n_workers = value

    async def start(self):
        try:
            import asyncssh  # import now to avoid adding to module startup time
        except ImportError:
            raise ImportError(
                "Dask's SSHCluster requires the `asyncssh` package to be installed. "
                "Please install it using pip or conda."
            )

        self.connection = await asyncssh.connect(self.address, **self.connect_options)

        result = await self.connection.run("uname")
        if result.exit_status == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                dask.config.serialize(dask.config.global_config)
            )
        else:
            result = await self.connection.run("cmd /c ver")
            if result.exit_status == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    dask.config.serialize(dask.config.global_config)
                )
            else:
                raise Exception(
                    "Worker failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
                )

        if not self.remote_python:
            self.remote_python = sys.executable

        cmd = " ".join(
            [
                set_env,
                self.remote_python,
                "-m",
                "distributed.cli.dask_spec",
                self.scheduler,
                "--spec",
                "'%s'"
                % dumps(
                    {
                        i: {
                            "cls": self.worker_class,
                            "opts": {
                                **self.kwargs,
                            },
                        }
                        for i in range(self.n_workers)
                    }
                ),
            ]
        )

        self.proc = await self.connection.create_process(cmd)

        # We watch stderr in order to get the address, then we return
        started_workers = 0
        while started_workers < self.n_workers:
            line = await self.proc.stderr.readline()
            if not line.strip():
                raise Exception("Worker failed to start")
            logger.info(line.strip())
            if "worker at" in line:
                started_workers += 1
        logger.debug("%s", line)
        await super().start()


class Scheduler(Process):
    """A Remote Dask Scheduler controlled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this scheduler.
    kwargs: dict
        These will be passed through the dask scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(
        self,
        address: str,
        connect_options: dict,
        kwargs: dict,
        remote_python: str | None = None,
    ):
        super().__init__()

        self.address = address
        self.kwargs = kwargs
        self.connect_options = connect_options
        self.remote_python = remote_python or sys.executable

    async def start(self):
        try:
            import asyncssh  # import now to avoid adding to module startup time
        except ImportError:
            raise ImportError(
                "Dask's SSHCluster requires the `asyncssh` package to be installed. "
                "Please install it using pip or conda."
            )

        logger.debug("Created Scheduler Connection")

        self.connection = await asyncssh.connect(self.address, **self.connect_options)

        result = await self.connection.run("uname")
        if result.exit_status == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                dask.config.serialize(dask.config.global_config)
            )
        else:
            result = await self.connection.run("cmd /c ver")
            if result.exit_status == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    dask.config.serialize(dask.config.global_config)
                )
            else:
                raise Exception(
                    "Scheduler failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
                )

        cmd = " ".join(
            [
                set_env,
                self.remote_python,
                "-m",
                "distributed.cli.dask_spec",
                "--spec",
                "'%s'" % dumps({"cls": "distributed.Scheduler", "opts": self.kwargs}),
            ]
        )
        self.proc = await self.connection.create_process(cmd)

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if not line.strip():
                raise Exception("Worker failed to start")
            logger.info(line.strip())
            if "Scheduler at" in line:
                self.address = line.split("Scheduler at:")[1].strip()
                break
        logger.debug("%s", line)
        await super().start()


old_cluster_kwargs = {
    "scheduler_addr",
    "scheduler_port",
    "worker_addrs",
    "nthreads",
    "nprocs",
    "n_workers",
    "ssh_username",
    "ssh_port",
    "ssh_private_key",
    "nohost",
    "logdir",
    "remote_python",
    "memory_limit",
    "worker_port",
    "nanny_port",
    "remote_dask_worker",
}


def SSHCluster(
    hosts: list[str] | None = None,
    connect_options: dict | list[dict] | None = None,
    worker_options: dict | None = None,
    scheduler_options: dict | None = None,
    worker_module: str = "deprecated",
    worker_class: str = "distributed.Nanny",
    remote_python: str | list[str] | None = None,
    **kwargs: Any,
) -> SpecCluster:
    """Deploy a Dask cluster using SSH

    The SSHCluster function deploys a Dask Scheduler and Workers for you on a
    set of machine addresses that you provide.  The first address will be used
    for the scheduler while the rest will be used for the workers (feel free to
    repeat the first hostname if you want to have the scheduler and worker
    co-habitate one machine.)

    You may configure the scheduler and workers by passing
    ``scheduler_options`` and ``worker_options`` dictionary keywords.  See the
    ``dask.distributed.Scheduler`` and ``dask.distributed.Worker`` classes for
    details on the available options, but the defaults should work in most
    situations.

    You may configure your use of SSH itself using the ``connect_options``
    keyword, which passes values to the ``asyncssh.connect`` function.  For
    more information on these see the documentation for the ``asyncssh``
    library https://asyncssh.readthedocs.io .

    Parameters
    ----------
    hosts
        List of hostnames or addresses on which to launch our cluster.
        The first will be used for the scheduler and the rest for workers.
    connect_options
        Keywords to pass through to :func:`asyncssh.connect`.
        This could include things such as ``port``, ``username``, ``password``
        or ``known_hosts``. See docs for :func:`asyncssh.connect` and
        :class:`asyncssh.SSHClientConnectionOptions` for full information.
        If a list it must have the same length as ``hosts``.
    worker_options
        Keywords to pass on to workers.
    scheduler_options
        Keywords to pass on to scheduler.
    worker_class
        The python class to use to create the worker(s).
    remote_python
        Path to Python on remote nodes.

    Examples
    --------
    Create a cluster with one worker:

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(["localhost", "localhost"])
    >>> client = Client(cluster)

    Create a cluster with three workers, each with two threads
    and host the dashdoard on port 8797:

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 2},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)

    Create a cluster with two workers on each host:

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 2, "n_workers": 2},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)

    An example using a different worker class, in particular the
    ``CUDAWorker`` from the ``dask-cuda`` project:

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "hostwithgpus", "anothergpuhost"],
    ...     connect_options={"known_hosts": None},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"},
    ...     worker_class="dask_cuda.CUDAWorker")
    >>> client = Client(cluster)

    See Also
    --------
    dask.distributed.Scheduler
    dask.distributed.Worker
    asyncssh.connect
    """
    connect_options = connect_options or {}
    worker_options = worker_options or {}
    scheduler_options = scheduler_options or {}

    if worker_module != "deprecated":
        raise ValueError(
            "worker_module has been deprecated in favor of worker_class. "
            "Please specify a Python class rather than a CLI module."
        )

    if set(kwargs) & old_cluster_kwargs:
        from distributed.deploy.old_ssh import SSHCluster as OldSSHCluster

        warnings.warn(
            "Note that the SSHCluster API has been replaced.  "
            "We're routing you to the older implementation.  "
            "This will be removed in the future"
        )
        kwargs.setdefault("worker_addrs", hosts)
        return OldSSHCluster(**kwargs)  # type: ignore

    if not hosts:
        raise ValueError(
            f"`hosts` must be a non empty list, value {repr(hosts)!r} found."
        )
    if isinstance(connect_options, list) and len(connect_options) != len(hosts):
        raise RuntimeError(
            "When specifying a list of connect_options you must provide a "
            "dictionary for each address."
        )

    if isinstance(remote_python, list) and len(remote_python) != len(hosts):
        raise RuntimeError(
            "When specifying a list of remote_python you must provide a "
            "path for each address."
        )

    scheduler = {
        "cls": Scheduler,
        "options": {
            "address": hosts[0],
            "connect_options": (
                connect_options
                if isinstance(connect_options, dict)
                else connect_options[0]
            ),
            "kwargs": scheduler_options,
            "remote_python": (
                remote_python[0] if isinstance(remote_python, list) else remote_python
            ),
        },
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {
                "address": host,
                "connect_options": (
                    connect_options
                    if isinstance(connect_options, dict)
                    else connect_options[i + 1]
                ),
                "kwargs": worker_options,
                "worker_class": worker_class,
                "remote_python": (
                    remote_python[i + 1]
                    if isinstance(remote_python, list)
                    else remote_python
                ),
            },
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, name="SSHCluster", **kwargs)
