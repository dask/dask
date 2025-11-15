from __future__ import annotations

import abc
import asyncio
import copy
import json
import logging
import math
import os
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

import psutil
import toolz

from dask.system import CPU_COUNT

from distributed.compatibility import WINDOWS
from distributed.deploy.spec import ProcessInterface, SpecCluster
from distributed.deploy.utils import nprocesses_nthreads
from distributed.utils import Deadline
from distributed.worker_memory import parse_memory_limit

logger = logging.getLogger(__name__)


class Subprocess(ProcessInterface, abc.ABC):
    process: asyncio.subprocess.Process | None

    def __init__(self):
        if WINDOWS:
            # FIXME: distributed#7434
            raise RuntimeError("Subprocess does not support Windows.")
        self.process = None
        super().__init__()

    async def start(self) -> None:
        await self._start()
        await super().start()

    @abc.abstractmethod
    async def _start(self) -> None:
        """Start the subprocess"""

    async def close(self) -> None:
        if self.process and self.process.returncode is None:
            for child in psutil.Process(self.process.pid).children(recursive=True):
                child.kill()
            self.process.kill()
            await self.process.communicate()
        self.process = None
        await super().close()


class SubprocessScheduler(Subprocess):
    """A local Dask scheduler running in a dedicated subprocess

    Parameters
    ----------
    scheduler_kwargs:
        Keywords to pass on to the ``Scheduler`` class constructor
    """

    scheduler_kwargs: dict
    timeout: int
    address: str | None

    def __init__(
        self,
        scheduler_kwargs: dict | None = None,
        timeout: int = 30,
    ):
        self.scheduler_kwargs = {
            "scheduler_file": os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        }
        if scheduler_kwargs:
            self.scheduler_kwargs.update(scheduler_kwargs)
        self.timeout = timeout
        super().__init__()

    async def _start(self):
        cmd = [
            sys.executable,
            "-m",
            "dask",
            "spec",
            "--spec",
            json.dumps(
                {"cls": "distributed.Scheduler", "opts": {**self.scheduler_kwargs}}
            ),
        ]
        logger.info(" ".join(cmd))
        deadline = Deadline.after(self.timeout)
        self.process = await asyncio.create_subprocess_exec(
            *cmd,
            stderr=asyncio.subprocess.PIPE,
        )

        scheduler_file = Path(self.scheduler_kwargs["scheduler_file"])
        while not (
            deadline.expired
            or scheduler_file.exists()
            or self.process.returncode is not None
        ):
            await asyncio.sleep(0.1)
        if deadline.expired or self.process.returncode is not None:
            assert self.process.stderr
            logger.error((await self.process.stderr.read()).decode())
            if deadline.expired:
                raise RuntimeError(f"Scheduler failed to start within {self.timeout}s")
            raise RuntimeError(
                f"Scheduler failed to start and exited with code {self.process.returncode}"
            )

        with scheduler_file.open(mode="r") as f:
            identity = json.load(f)
            self.address = identity["address"]
        logger.info("Scheduler at %r", self.address)


class SubprocessWorker(Subprocess):
    """A local Dask worker running in a dedicated subprocess

    Parameters
    ----------
    scheduler:
        Address of the scheduler
    worker_class:
        Python class to use to create the worker, defaults to 'distributed.Nanny'
    name:
        Name of the worker
    worker_kwargs:
        Keywords to pass on to the ``Worker`` class constructor
    """

    name: str | None
    scheduler: str
    worker_class: str
    worker_kwargs: dict

    def __init__(
        self,
        scheduler: str,
        worker_class: str = "distributed.Nanny",
        name: str | None = None,
        worker_kwargs: dict | None = None,
    ) -> None:
        self.name = name
        self.scheduler = scheduler
        self.worker_class = worker_class
        self.worker_kwargs = copy.copy(worker_kwargs or {})
        super().__init__()

    async def _start(self) -> None:
        cmd = [
            sys.executable,
            "-m",
            "dask",
            "spec",
            self.scheduler,
            "--spec",
            json.dumps({"cls": self.worker_class, "opts": {**self.worker_kwargs}}),
        ]
        logger.info(" ".join(cmd))
        self.process = await asyncio.create_subprocess_exec(*cmd)


def SubprocessCluster(
    host: str | None = None,
    scheduler_port: int = 0,
    scheduler_kwargs: dict | None = None,
    dashboard_address: str = ":8787",
    worker_class: str = "distributed.Nanny",
    n_workers: int | None = None,
    threads_per_worker: int | None = None,
    worker_kwargs: dict | None = None,
    silence_logs: int = logging.WARN,
    **kwargs: Any,
) -> SpecCluster:
    """Create a scheduler and workers that run in dedicated subprocesses

    This creates a "cluster" of a scheduler and workers running in dedicated subprocesses.

    .. warning::

       This function is experimental

    Parameters
    ----------
    host:
        Host address on which the scheduler will listen, defaults to localhost
    scheduler_port:
        Port fo the scheduler, defaults to 0 to choose a random port
    scheduler_kwargs:
            Keywords to pass on to scheduler
    dashboard_address:
        Address on which to listen for the Bokeh diagnostics server like
        'localhost:8787' or '0.0.0.0:8787', defaults to ':8787'

        Set to ``None`` to disable the dashboard.
        Use ':0' for a random port.
    worker_class:
        Worker class to instantiate workers from, defaults to 'distributed.Nanny'
    n_workers:
        Number of workers to start
    threads:
        Number of threads per each worker
    worker_kwargs:
        Keywords to pass on to the ``Worker`` class constructor
    silence_logs:
        Level of logs to print out to stdout, defaults to ``logging.WARN``

        Use a falsy value like False or None to disable log silencing.

    Examples
    --------
    >>> cluster = SubprocessCluster()  # Create a subprocess cluster  #doctest: +SKIP
    >>> cluster  # doctest: +SKIP
    SubprocessCluster(SubprocessCluster, 'tcp://127.0.0.1:61207', workers=5, threads=10, memory=16.00 GiB)

    >>> c = Client(cluster)  # connect to subprocess cluster  # doctest: +SKIP

    Scale the cluster to three workers

    >>> cluster.scale(3)  # doctest: +SKIP
    """
    if WINDOWS:
        # FIXME: distributed#7434
        raise RuntimeError("SubprocessCluster does not support Windows.")
    if not host:
        host = "127.0.0.1"
    worker_kwargs = worker_kwargs or {}
    scheduler_kwargs = scheduler_kwargs or {}

    if n_workers is None and threads_per_worker is None:
        n_workers, threads_per_worker = nprocesses_nthreads()
    if n_workers is None and threads_per_worker is not None:
        n_workers = max(1, CPU_COUNT // threads_per_worker)
    if n_workers and threads_per_worker is None:
        # Overcommit threads per worker, rather than undercommit
        threads_per_worker = max(1, int(math.ceil(CPU_COUNT / n_workers)))
    if n_workers and "memory_limit" not in worker_kwargs:
        worker_kwargs["memory_limit"] = parse_memory_limit(
            "auto", 1, n_workers, logger=logger
        )
    assert n_workers is not None

    scheduler_kwargs = toolz.merge(
        {
            "host": host,
            "port": scheduler_port,
            "dashboard": dashboard_address is not None,
            "dashboard_address": dashboard_address,
        },
        scheduler_kwargs,
    )
    worker_kwargs = toolz.merge(
        {
            "host": host,
            "nthreads": threads_per_worker,
            "silence_logs": silence_logs,
        },
        worker_kwargs,
    )

    scheduler = {
        "cls": SubprocessScheduler,
        "options": {
            "scheduler_kwargs": scheduler_kwargs,
        },
    }
    worker = {
        "cls": SubprocessWorker,
        "options": {"worker_class": worker_class, "worker_kwargs": worker_kwargs},
    }
    workers = {i: worker for i in range(n_workers)}
    return SpecCluster(
        workers=workers,
        scheduler=scheduler,
        worker=worker,
        name="SubprocessCluster",
        silence_logs=silence_logs,
        **kwargs,
    )
