import logging
import sys
from typing import List, Union
import warnings
import weakref

import dask

from .spec import SpecCluster, ProcessInterface
from ..utils import cli_keywords
from ..scheduler import Scheduler as _Scheduler
from ..worker import Worker as _Worker
from ..utils import serialize_for_cli

logger = logging.getLogger(__name__)


class Process(ProcessInterface):
    """ A superclass for SSH Workers and Nannies

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
        self.proc.kill()  # https://github.com/ronf/asyncssh/issues/112
        self.connection.close()
        await super().close()

    def __repr__(self):
        return "<SSH %s: status=%s>" % (type(self).__name__, self.status)


class Worker(Process):
    """ A Remote Dask Worker controled by SSH

    Parameters
    ----------
    scheduler: str
        The address of the scheduler
    address: str
        The hostname where we should run this worker
    worker_module: str
        The python module to run to start the worker.
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this worker.
    kwargs: dict
        These will be passed through the dask-worker CLI to the
        dask.distributed.Worker class
    """

    def __init__(
        self,
        scheduler: str,
        address: str,
        connect_options: dict,
        kwargs: dict,
        worker_module="distributed.cli.dask_worker",
        remote_python=None,
        loop=None,
        name=None,
    ):
        super().__init__()

        self.address = address
        self.scheduler = scheduler
        self.worker_module = worker_module
        self.connect_options = connect_options
        self.kwargs = kwargs
        self.name = name
        self.remote_python = remote_python

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        self.connection = await asyncssh.connect(self.address, **self.connect_options)

        result = await self.connection.run("uname")
        if result.exit_status == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                serialize_for_cli(dask.config.global_config)
            )
        else:
            result = await self.connection.run("cmd /c ver")
            if result.exit_status == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    serialize_for_cli(dask.config.global_config)
                )
            else:
                raise Exception(
                    "Worker failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
                )

        cmd = " ".join(
            [
                set_env,
                self.remote_python or sys.executable,
                "-m",
                self.worker_module,
                self.scheduler,
                "--name",
                str(self.name),
            ]
            + cli_keywords(self.kwargs, cls=_Worker, cmd=self.worker_module)
        )

        self.proc = await self.connection.create_process(cmd)

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if not line.strip():
                raise Exception("Worker failed to start")
            logger.info(line.strip())
            if "worker at" in line:
                self.address = line.split("worker at:")[1].strip()
                self.status = "running"
                break
        logger.debug("%s", line)
        await super().start()


class Scheduler(Process):
    """ A Remote Dask Scheduler controlled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_options: dict
        kwargs to be passed to asyncssh connections
    remote_python: str
        Path to Python on remote node to run this scheduler.
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(
        self, address: str, connect_options: dict, kwargs: dict, remote_python=None
    ):
        super().__init__()

        self.address = address
        self.kwargs = kwargs
        self.connect_options = connect_options
        self.remote_python = remote_python

    async def start(self):
        import asyncssh  # import now to avoid adding to module startup time

        logger.debug("Created Scheduler Connection")

        self.connection = await asyncssh.connect(self.address, **self.connect_options)

        result = await self.connection.run("uname")
        if result.exit_status == 0:
            set_env = 'env DASK_INTERNAL_INHERIT_CONFIG="{}"'.format(
                serialize_for_cli(dask.config.global_config)
            )
        else:
            result = await self.connection.run("cmd /c ver")
            if result.exit_status == 0:
                set_env = "set DASK_INTERNAL_INHERIT_CONFIG={} &&".format(
                    serialize_for_cli(dask.config.global_config)
                )
            else:
                raise Exception(
                    "Scheduler failed to set DASK_INTERNAL_INHERIT_CONFIG variable "
                )

        cmd = " ".join(
            [
                set_env,
                self.remote_python or sys.executable,
                "-m",
                "distributed.cli.dask_scheduler",
            ]
            + cli_keywords(self.kwargs, cls=_Scheduler)
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
    hosts: List[str] = None,
    connect_options: Union[List[dict], dict] = {},
    worker_options: dict = {},
    scheduler_options: dict = {},
    worker_module: str = "distributed.cli.dask_worker",
    remote_python: str = None,
    **kwargs
):
    """ Deploy a Dask cluster using SSH

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
    hosts: List[str]
        List of hostnames or addresses on which to launch our cluster.
        The first will be used for the scheduler and the rest for workers.
    connect_options: dict or list of dict, optional
        Keywords to pass through to ``asyncssh.connect``.
        If a list it must have the same length as ``hosts``.
    worker_options: dict, optional
        Keywords to pass on to workers.
    scheduler_options: dict, optional
        Keywords to pass on to scheduler.
    worker_module: str, optional
        Python module to call to start the worker.
    remote_python: str, optional
        Path to Python on remote nodes.

    Examples
    --------
    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "localhost", "localhost", "localhost"],
    ...     connect_options={"known_hosts": None},
    ...     worker_options={"nthreads": 2},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"}
    ... )
    >>> client = Client(cluster)

    An example using a different worker module, in particular the
    ``dask-cuda-worker`` command from the ``dask-cuda`` project.

    >>> from dask.distributed import Client, SSHCluster
    >>> cluster = SSHCluster(
    ...     ["localhost", "hostwithgpus", "anothergpuhost"],
    ...     connect_options={"known_hosts": None},
    ...     scheduler_options={"port": 0, "dashboard_address": ":8797"},
    ...     worker_module='dask_cuda.dask_cuda_worker')
    >>> client = Client(cluster)

    See Also
    --------
    dask.distributed.Scheduler
    dask.distributed.Worker
    asyncssh.connect
    """
    if set(kwargs) & old_cluster_kwargs:
        from .old_ssh import SSHCluster as OldSSHCluster

        warnings.warn(
            "Note that the SSHCluster API has been replaced.  "
            "We're routing you to the older implementation.  "
            "This will be removed in the future"
        )
        kwargs.setdefault("worker_addrs", hosts)
        return OldSSHCluster(**kwargs)

    if isinstance(connect_options, list) and len(connect_options) != len(hosts):
        raise RuntimeError(
            "When specifying a list of connect_options you must provide a "
            "dictionary for each address."
        )

    scheduler = {
        "cls": Scheduler,
        "options": {
            "address": hosts[0],
            "connect_options": connect_options
            if isinstance(connect_options, dict)
            else connect_options[0],
            "kwargs": scheduler_options,
            "remote_python": remote_python,
        },
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {
                "address": host,
                "connect_options": connect_options
                if isinstance(connect_options, dict)
                else connect_options[i + 1],
                "kwargs": worker_options,
                "worker_module": worker_module,
                "remote_python": remote_python,
            },
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, name="SSHCluster", **kwargs)
