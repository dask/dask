import atexit
import logging
import math
import multiprocessing
import warnings
import weakref

from dask.utils import factors

from .spec import SpecCluster
from ..nanny import Nanny
from ..scheduler import Scheduler
from ..security import Security
from ..worker import Worker, parse_memory_limit

logger = logging.getLogger(__name__)


class LocalCluster(SpecCluster):
    """ Create local Scheduler and Workers

    This creates a "cluster" of a scheduler and workers running on the local
    machine.

    Parameters
    ----------
    n_workers: int
        Number of workers to start
    processes: bool
        Whether to use processes (True) or threads (False).  Defaults to True
    threads_per_worker: int
        Number of threads per each worker
    scheduler_port: int
        Port of the scheduler.  8786 by default, use 0 to choose a random port
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
    diagnostics_port: int
        Deprecated.  See dashboard_address.
    asynchronous: bool (False by default)
        Set to True if using this cluster within async/await functions or within
        Tornado gen.coroutines.  This should remain False for normal use.
    worker_kwargs: dict
        Extra worker arguments, will be passed to the Worker constructor.
    blocked_handlers: List[str]
        A list of strings specifying a blacklist of handlers to disallow on the Scheduler,
        like ``['feed', 'run_function']``
    service_kwargs: Dict[str, Dict]
        Extra keywords to hand to the running services
    security : Security
    protocol: str (optional)
        Protocol to use like ``tcp://``, ``tls://``, ``inproc://``
        This defaults to sensible choice given other keyword arguments like
        ``processes`` and ``security``
    interface: str (optional)
        Network interface to use.  Defaults to lo/localhost
    worker_class: Worker
        Worker class used to instantiate workers from.

    Examples
    --------
    >>> cluster = LocalCluster()  # Create a local cluster with as many workers as cores  # doctest: +SKIP
    >>> cluster  # doctest: +SKIP
    LocalCluster("127.0.0.1:8786", workers=8, threads=8)

    >>> c = Client(cluster)  # connect to local cluster  # doctest: +SKIP

    Scale the cluster to three workers

    >>> cluster.scale(3)  # doctest: +SKIP

    Pass extra keyword arguments to Bokeh

    >>> LocalCluster(service_kwargs={'bokeh': {'prefix': '/foo'}})  # doctest: +SKIP
    """

    def __init__(
        self,
        n_workers=None,
        threads_per_worker=None,
        processes=True,
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
        **worker_kwargs
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

        self.status = None
        self.processes = processes
        security = security or Security()

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
                threads_per_worker = multiprocessing.cpu_count()
        if n_workers is None and threads_per_worker is not None:
            n_workers = max(1, multiprocessing.cpu_count() // threads_per_worker)
        if n_workers and threads_per_worker is None:
            # Overcommit threads per worker, rather than undercommit
            threads_per_worker = max(
                1, int(math.ceil(multiprocessing.cpu_count() / n_workers))
            )
        if n_workers and "memory_limit" not in worker_kwargs:
            worker_kwargs["memory_limit"] = parse_memory_limit("auto", 1, n_workers)

        worker_kwargs.update(
            {
                "nthreads": threads_per_worker,
                "services": worker_services,
                "dashboard_address": worker_dashboard_address,
                "interface": interface,
                "protocol": protocol,
                "security": security,
                "silence_logs": silence_logs,
            }
        )

        scheduler = {
            "cls": Scheduler,
            "options": dict(
                host=host,
                services=services,
                service_kwargs=service_kwargs,
                security=security,
                port=scheduler_port,
                interface=interface,
                protocol=protocol,
                dashboard_address=dashboard_address,
                blocked_handlers=blocked_handlers,
            ),
        }

        worker = {
            "cls": worker_class or (Worker if not processes else Nanny),
            "options": worker_kwargs,
        }

        workers = {i: worker for i in range(n_workers)}

        super(LocalCluster, self).__init__(
            scheduler=scheduler,
            workers=workers,
            worker=worker,
            loop=loop,
            asynchronous=asynchronous,
            silence_logs=silence_logs,
            security=security,
        )

    def __repr__(self):
        return "%s(%r, workers=%d, nthreads=%d)" % (
            type(self).__name__,
            self.scheduler_address,
            len(self.workers),
            sum(w.nthreads for w in self.workers.values()),
        )

    def start_worker(self, *args, **kwargs):
        raise NotImplementedError(
            "The `cluster.start_worker` function has been removed. "
            "Please see the `cluster.scale` method instead."
        )


def nprocesses_nthreads(n=multiprocessing.cpu_count()):
    """
    The default breakdown of processes and threads for a given number of cores

    Parameters
    ----------
    n: int
        Number of available cores

    Examples
    --------
    >>> nprocesses_nthreads(4)
    (4, 1)
    >>> nprocesses_nthreads(32)
    (8, 4)

    Returns
    -------
    nprocesses, nthreads
    """
    if n <= 4:
        processes = n
    else:
        processes = min(f for f in factors(n) if f >= math.sqrt(n))
    threads = n // processes
    return (processes, threads)


clusters_to_close = weakref.WeakSet()


@atexit.register
def close_clusters():
    for cluster in list(clusters_to_close):
        cluster.close(timeout=10)
