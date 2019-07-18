import asyncio
import logging
import sys
import warnings
import weakref

import asyncssh

from .spec import SpecCluster

logger = logging.getLogger(__name__)

warnings.warn(
    "the distributed.deploy.ssh2 module is experimental "
    "and will move/change in the future without notice"
)


class Process:
    """ A superclass for SSH Workers and Nannies

    See Also
    --------
    Worker
    Scheduler
    """

    def __init__(self):
        self.lock = asyncio.Lock()
        self.connection = None
        self.proc = None
        self.status = "created"

    def __await__(self):
        async def _():
            async with self.lock:
                if not self.connection:
                    await self.start()
                    assert self.connection
                    weakref.finalize(self, self.proc.terminate)
            return self

        return _().__await__()

    async def close(self):
        self.proc.terminate()
        self.connection.close()
        self.status = "closed"

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
    kwargs:
        TODO
    """

    def __init__(self, scheduler: str, address: str, connect_kwargs: dict, **kwargs):
        self.address = address
        self.scheduler = scheduler
        self.connect_kwargs = connect_kwargs
        self.kwargs = kwargs

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
                    "--name",  # we need to have name for SpecCluster
                    str(self.kwargs["name"]),
                ]
            )
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if "worker at" in line:
                self.address = line.split("worker at:")[1].strip()
                self.status = "running"
                break
        logger.debug("%s", line)


class Scheduler(Process):
    """ A Remote Dask Scheduler controled by SSH

    Parameters
    ----------
    address: str
        The hostname where we should run this worker
    connect_kwargs: dict
        kwargs to be passed to asyncssh connections
    kwargs:
        TODO
    """

    def __init__(self, address: str, connect_kwargs: dict, **kwargs):
        self.address = address
        self.kwargs = kwargs
        self.connect_kwargs = connect_kwargs

        super().__init__()

    async def start(self):
        logger.debug("Created Scheduler Connection")

        self.connection = await asyncssh.connect(self.address, **self.connect_kwargs)

        self.proc = await self.connection.create_process(
            " ".join([sys.executable, "-m", "distributed.cli.dask_scheduler"])
        )

        # We watch stderr in order to get the address, then we return
        while True:
            line = await self.proc.stderr.readline()
            if "Scheduler at" in line:
                self.address = line.split("Scheduler at:")[1].strip()
                break
        logger.debug("%s", line)


def SSHCluster(hosts, connect_kwargs, **kwargs):
    """ Deploy a Dask cluster using SSH

    Parameters
    ----------
    hosts: List[str]
        List of hostnames or addresses on which to launch our cluster
        The first will be used for the scheduler and the rest for workers
    connect_kwargs:
        known_hosts: List[str] or None
            The list of keys which will be used to validate the server host
            key presented during the SSH handshake.  If this is not specified,
            the keys will be looked up in the file .ssh/known_hosts.  If this
            is explicitly set to None, server host key validation will be disabled.
        TODO
    kwargs:
        TODO
    ----
    This doesn't handle any keyword arguments yet.  It is a proof of concept
    """
    scheduler = {
        "cls": Scheduler,
        "options": {"address": hosts[0], "connect_kwargs": connect_kwargs},
    }
    workers = {
        i: {
            "cls": Worker,
            "options": {"address": host, "connect_kwargs": connect_kwargs},
        }
        for i, host in enumerate(hosts[1:])
    }
    return SpecCluster(workers, scheduler, **kwargs)
