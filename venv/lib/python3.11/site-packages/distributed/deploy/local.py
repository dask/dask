from __future__ import annotations

import logging
import math
import warnings

import toolz

from dask.system import CPU_COUNT
from dask.widgets import get_template

from distributed.deploy.spec import SpecCluster
from distributed.deploy.utils import nprocesses_nthreads
from distributed.nanny import Nanny
from distributed.scheduler import Scheduler
from distributed.security import Security
from distributed.worker import Worker
from distributed.worker_memory import parse_memory_limit

logger = logging.getLogger(__name__)


class LocalCluster(SpecCluster):
    """Create local Scheduler and Workers

    This creates a "cluster" of a scheduler and workers running on the local
    machine.

    Parameters
    ----------
    n_workers: int
        Number of workers to start
    memory_limit: str, float, int, or None, default "auto"
        Sets the memory limit *per worker*.

        Notes regarding argument data type:

        * If None or 0, no limit is applied.
        * If "auto", the total system memory is split evenly between the workers.
        * If a float, that fraction of the system memory is used *per worker*.
        * If a string giving a number of bytes (like ``"1GiB"``), that amount is used *per worker*.
        * If an int, that number of bytes is used *per worker*.

        Note that the limit will only be enforced when ``processes=True``, and the limit is only
        enforced on a best-effort basis — it's still possible for workers to exceed this limit.
    processes: bool
        Whether to use processes (True) or threads (False).  Defaults to True, unless
        worker_class=Worker, in which case it defaults to False.
    threads_per_worker: int
        Number of threads per each worker
    scheduler_port: int
        Port of the scheduler. Use 0 to choose a random port (default). 8786 is a common choice.
    silence_logs: logging level
        Level of logs to print out to stdout.  ``logging.WARN`` by default.
        Use a falsey value like False or None for no change.
    host: string
        Host address on which the scheduler will listen, defaults to only localhost
    ip: string
        Deprecated.  See ``host`` above.
    dashboard_address: str
        Address on which to listen for the Bokeh diagnostics server like
        'localhost:8787' or '0.0.0.0:8787'.  Defaults to ':8787'.
        Set to ``None`` to disable the dashboard.
        Use ':0' for a random port.
        When specifying only a port like ':8787', the dashboard will bind to the given interface from the ``host`` parameter.
        If ``host`` is empty, binding will occur on all interfaces '0.0.0.0'.
        To avoid firewall issues when deploying locally, set ``host`` to 'localhost'.
    worker_dashboard_address: str
        Address on which to listen for the Bokeh worker diagnostics server like
        'localhost:8787' or '0.0.0.0:8787'.  Defaults to None which disables the dashboard.
        Use ':0' for a random port.
    diagnostics_port: int
        Deprecated.  See dashboard_address.
    asynchronous: bool (False by default)
        Set to True if using this cluster within async/await functions or within
        Tornado gen.coroutines.  This should remain False for normal use.
    blocked_handlers: List[str]
        A list of strings specifying a blocklist of handlers to disallow on the
        Scheduler, like ``['feed', 'run_function']``
    service_kwargs: Dict[str, Dict]
        Extra keywords to hand to the running services
    security : Security or bool, optional
        Configures communication security in this cluster. Can be a security
        object, or True. If True, temporary self-signed credentials will
        be created automatically.
    protocol: str (optional)
        Protocol to use like ``tcp://``, ``tls://``, ``inproc://``
        This defaults to sensible choice given other keyword arguments like
        ``processes`` and ``security``
    interface: str (optional)
        Network interface to use.  Defaults to lo/localhost
    worker_class: Worker
        Worker class used to instantiate workers from. Defaults to Worker if
        processes=False and Nanny if processes=True or omitted.
    **worker_kwargs:
        Extra worker arguments. Any additional keyword arguments will be passed
        to the ``Worker`` class constructor.

    Examples
    --------
    >>> cluster = LocalCluster()  # Create a local cluster  # doctest: +SKIP
    >>> cluster  # doctest: +SKIP
    LocalCluster("127.0.0.1:8786", workers=8, threads=8)

    >>> c = Client(cluster)  # connect to local cluster  # doctest: +SKIP

    Scale the cluster to three workers

    >>> cluster.scale(3)  # doctest: +SKIP

    Pass extra keyword arguments to Bokeh

    >>> LocalCluster(service_kwargs={'dashboard': {'prefix': '/foo'}})  # doctest: +SKIP
    """

    def __init__(
        self,
        name=None,
        n_workers=None,
        threads_per_worker=None,
        processes=None,
        loop=None,
        start=None,
        host=None,
        ip=None,
        scheduler_port=0,
        silence_logs=logging.WARN,
        dashboard_address=":8787",
        worker_dashboard_address=None,
        diagnostics_port=None,
        services=None,
        worker_services=None,
        service_kwargs=None,
        asynchronous=False,
        security=None,
        protocol=None,
        blocked_handlers=None,
        interface=None,
        worker_class=None,
        scheduler_kwargs=None,
        scheduler_sync_interval=1,
        **worker_kwargs,
    ):
        if ip is not None:
            # In the future we should warn users about this move
            # warnings.warn("The ip keyword has been moved to host")
            host = ip

        if diagnostics_port is not None:
            warnings.warn(
                "diagnostics_port has been deprecated. "
                "Please use `dashboard_address=` instead"
            )
            dashboard_address = diagnostics_port

        if threads_per_worker == 0:
            warnings.warn(
                "Setting `threads_per_worker` to 0 has been deprecated. "
                "Please set to None or to a specific int."
            )
            threads_per_worker = None

        if "dashboard" in worker_kwargs:
            warnings.warn(
                "Setting `dashboard` is discouraged. "
                "Please set `dashboard_address` to affect the scheduler (more common) "
                "and `worker_dashboard_address` for the worker (less common)."
            )

        if processes is None:
            processes = worker_class is None or issubclass(worker_class, Nanny)
        if worker_class is None:
            worker_class = Nanny if processes else Worker

        self.status = None
        self.processes = processes

        if security is None:
            # Falsey values load the default configuration
            security = Security()
        elif security is True:
            # True indicates self-signed temporary credentials should be used
            security = Security.temporary()
        elif not isinstance(security, Security):
            raise TypeError("security must be a Security object")

        if protocol is None:
            if host and "://" in host:
                protocol = host.split("://")[0]
            elif security and security.require_encryption:
                protocol = "tls://"
            elif not self.processes and not scheduler_port:
                protocol = "inproc://"
            else:
                protocol = "tcp://"
        if not protocol.endswith("://"):
            protocol = protocol + "://"

        if host is None and not protocol.startswith("inproc") and not interface:
            host = "127.0.0.1"

        services = services or {}
        worker_services = worker_services or {}
        if n_workers is None and threads_per_worker is None:
            if processes:
                n_workers, threads_per_worker = nprocesses_nthreads()
            else:
                n_workers = 1
                threads_per_worker = CPU_COUNT
        if n_workers is None and threads_per_worker is not None:
            n_workers = max(1, CPU_COUNT // threads_per_worker) if processes else 1
        if n_workers and threads_per_worker is None:
            # Overcommit threads per worker, rather than undercommit
            threads_per_worker = max(1, int(math.ceil(CPU_COUNT / n_workers)))
        if n_workers and "memory_limit" not in worker_kwargs:
            worker_kwargs["memory_limit"] = parse_memory_limit(
                "auto", 1, n_workers, logger=logger
            )

        worker_kwargs.update(
            {
                "host": host,
                "nthreads": threads_per_worker,
                "services": worker_services,
                "dashboard_address": worker_dashboard_address,
                "dashboard": worker_dashboard_address is not None,
                "interface": interface,
                "protocol": protocol,
                "security": security,
                "silence_logs": silence_logs,
            }
        )

        scheduler = {
            "cls": Scheduler,
            "options": toolz.merge(
                dict(
                    host=host,
                    services=services,
                    service_kwargs=service_kwargs,
                    security=security,
                    port=scheduler_port,
                    interface=interface,
                    protocol=protocol,
                    dashboard=dashboard_address is not None,
                    dashboard_address=dashboard_address,
                    blocked_handlers=blocked_handlers,
                ),
                scheduler_kwargs or {},
            ),
        }

        worker = {"cls": worker_class, "options": worker_kwargs}
        workers = {i: worker for i in range(n_workers)}

        super().__init__(
            name=name,
            scheduler=scheduler,
            workers=workers,
            worker=worker,
            loop=loop,
            asynchronous=asynchronous,
            silence_logs=silence_logs,
            security=security,
            scheduler_sync_interval=scheduler_sync_interval,
        )

    def start_worker(self, *args, **kwargs):
        raise NotImplementedError(
            "The `cluster.start_worker` function has been removed. "
            "Please see the `cluster.scale` method instead."
        )

    def _repr_html_(self, cluster_status=None):
        cluster_status = get_template("local_cluster.html.j2").render(
            status=self.status.name,
            processes=self.processes,
            cluster_status=cluster_status,
        )
        return super()._repr_html_(cluster_status=cluster_status)
