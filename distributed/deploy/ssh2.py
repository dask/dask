import logging
import sys
import warnings
import weakref

import asyncssh

from .spec import SpecCluster, ProcessInterface
from ..utils import cli_keywords
from ..scheduler import Scheduler as _Scheduler
from ..worker import Worker as _Worker

logger = logging.getLogger(__name__)

warnings.warn(
    "the distributed.deploy.ssh2 module is experimental "
    "and will move/change in the future without notice"
)


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
        weakref.finalize(self, self.proc.terminate)
        await super().start()

    async def close(self):
        self.proc.terminate()
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
    connect_kwargs: dict
        kwargs to be passed to asyncssh connections
    kwargs: dict
        These will be passed through the dask-worker CLI to the
        dask.distributed.Worker class
    """

    def __init__(
        self,
        scheduler: str,
        address: str,
        connect_kwargs: dict,
        kwargs: dict,
        loop=None,
        name=None,
    ):
        self.address = address
        self.scheduler = scheduler
        self.connect_kwargs = connect_kwargs
        self.kwargs = kwargs
        self.name = name

        super().__init__()

    async def start(self):
        self.connection = await asyncssh.connect(self.address, **self.connect_kwargs)
        self.proc = await self.connection.create_process(
            " ".join(
                [
                    sys.executable,
                    "-m",
                    "distributed.cli.dask_worker",
                    self.scheduler,
                    "--name",
                    str(self.name),
                ]
                + cli_keywords(self.kwargs, cls=_Worker)
            )
        )

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
    """ A Remote Dask Scheduler controled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_kwargs: dict
        kwargs to be passed to asyncssh connections
    kwargs: dict
        These will be passed through the dask-scheduler CLI to the
        dask.distributed.Scheduler class
    """

    def __init__(self, address: str, connect_kwargs: dict, kwargs: dict):
        self.address = address
        self.kwargs = kwargs
        self.connect_kwargs = connect_kwargs

        super().__init__()

    async def start(self):
        logger.debug("Created Scheduler Connection")

        self.connection = await asyncssh.connect(self.address, **self.connect_kwargs)

        self.proc = await self.connection.create_process(
            " ".join(
                [sys.executable, "-m", "distributed.cli.dask_scheduler"]
                + cli_keywords(self.kwargs, cls=_Scheduler)
            )
        )

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


def SSHCluster(
    hosts, connect_kwargs={}, worker_kwargs={}, scheduler_kwargs={}, **kwargs
):
    """ Deploy a Dask cluster using SSH

    Parameters
    ----------
    hosts: List[str]
        List of hostnames or addresses on which to launch our cluster
        The first will be used for the scheduler and the rest for workers
    connect_kwargs:
        Keywords to pass through to asyncssh.connect
        known_hosts: List[str] or None
            The list of keys which will be used to validate the server host
            key presented during the SSH handshake.  If this is not specified,
            the keys will be looked up in the file .ssh/known_hosts.  If this
            is explicitly set to None, server host key validation will be disabled.
    scheduler_kwargs:
        Keywords to pass on to dask-scheduler
    worker_kwargs:
        Keywords to pass on to dask-worker

    Examples
    --------
    >>> from dask.distributed import Client
    >>> from distributed.deploy.ssh2 import SSHCluster  # experimental for now
    >>> cluster = SSHCluster(
    ...     ["localhost"] * 4,
    ...     connect_kwargs={"known_hosts": None},
    ...     worker_kwargs={"nthreads": 2},
    ...     scheduler_kwargs={"port": 0, "dashboard_address": ":8797"})
    >>> client = Client(cluster)
    """
    scheduler = {
        "cls": Scheduler,
        "options": {
            "address": hosts[0],
            "connect_kwargs": connect_kwargs,
            "kwargs": scheduler_kwargs,
        },
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {
                "address": host,
                "connect_kwargs": connect_kwargs,
                "kwargs": worker_kwargs,
            },
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, name="SSHCluster", **kwargs)
