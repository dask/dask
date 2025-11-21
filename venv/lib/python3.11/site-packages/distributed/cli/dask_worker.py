from __future__ import annotations

import asyncio
import atexit
import gc
import logging
import os
import sys
import warnings
from collections.abc import Iterator
from contextlib import suppress
from typing import Any

import click
from tlz import valmap
from tornado.ioloop import TimeoutError

import dask
from dask.system import CPU_COUNT

from distributed import Nanny
from distributed._signals import wait_for_signals
from distributed.comm import get_address_host_port
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.deploy.utils import nprocesses_nthreads
from distributed.preloading import validate_preload_argv
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)
from distributed.utils import import_term, parse_ports

logger = logging.getLogger("distributed.dask_worker")


pem_file_option_type = click.Path(exists=True, resolve_path=True)


@click.command(name="worker", context_settings=dict(ignore_unknown_options=True))
@click.argument("scheduler", type=str, required=False)
@click.option(
    "--tls-ca-file",
    type=pem_file_option_type,
    default=None,
    help="CA cert(s) file for TLS (in PEM format)",
)
@click.option(
    "--tls-cert",
    type=pem_file_option_type,
    default=None,
    help="certificate file for TLS (in PEM format)",
)
@click.option(
    "--tls-key",
    type=pem_file_option_type,
    default=None,
    help="private key file for TLS (in PEM format)",
)
@click.option(
    "--worker-port",
    default=None,
    help="Serving computation port, defaults to random. "
    "When creating multiple workers with --nworkers, a sequential range of "
    "worker ports may be used by specifying the first and last available "
    "ports like <first-port>:<last-port>. For example, --worker-port=3000:3026 "
    "will use ports 3000, 3001, ..., 3025, 3026.",
)
@click.option(
    "--nanny-port",
    default=None,
    help="Serving nanny port, defaults to random. "
    "When creating multiple nannies with --nworkers, a sequential range of "
    "nanny ports may be used by specifying the first and last available "
    "ports like <first-port>:<last-port>. For example, --nanny-port=3000:3026 "
    "will use ports 3000, 3001, ..., 3025, 3026.",
)
@click.option(
    "--dashboard-address",
    type=str,
    default=":0",
    help="Address on which to listen for diagnostics dashboard",
)
@click.option(
    "--dashboard/--no-dashboard",
    "dashboard",
    default=True,
    required=False,
    help="Launch the Dashboard [default: --dashboard]",
)
@click.option(
    "--listen-address",
    type=str,
    default=None,
    help="The address to which the worker binds. Example: tcp://0.0.0.0:9000 or tcp://:9000 for IPv4+IPv6",
)
@click.option(
    "--contact-address",
    type=str,
    default=None,
    help="The address the worker advertises to the scheduler for "
    "communication with it and other workers. "
    "Example: tcp://127.0.0.1:9000",
)
@click.option(
    "--host",
    type=str,
    default=None,
    help="Serving host. Should be an ip address that is"
    " visible to the scheduler and other workers. "
    "See --listen-address and --contact-address if you "
    "need different listen and contact addresses. "
    "See --interface.",
)
@click.option(
    "--interface", type=str, default=None, help="Network interface like 'eth0' or 'ib0'"
)
@click.option(
    "--protocol", type=str, default=None, help="Protocol like tcp, tls, or ucx"
)
@click.option("--nthreads", type=int, default=0, help="Number of threads per process.")
@click.option(
    "--nworkers",
    "n_workers",  # This sets the Python argument name
    type=str,
    default=None,
    show_default=True,
    help="Number of worker processes to launch. "
    "If negative, then (CPU_COUNT + 1 + nworkers) is used. "
    "Set to 'auto' to set nworkers and nthreads dynamically based on CPU_COUNT",
)
@click.option(
    "--name",
    type=str,
    default=None,
    help="A unique name for this worker like 'worker-1'. "
    "If used with --nworkers then the process number "
    "will be appended like name-0, name-1, name-2, ...",
)
@click.option(
    "--memory-limit",
    default="auto",
    show_default=True,
    help="""\b
    Bytes of memory per process that the worker can use.
    This can be:
    - an integer (bytes), note 0 is a special case for no memory management.
    - a float (fraction of total system memory).
    - a string (like 5GB or 5000M).
    - 'auto' for automatically computing the memory limit.
    """,
)
@click.option(
    "--nanny/--no-nanny",
    default=True,
    help="Start workers in nanny process for management [default: --nanny]",
)
@click.option("--pid-file", type=str, default="", help="File to write the process PID")
@click.option(
    "--local-directory", default=None, type=str, help="Directory to place worker files"
)
@click.option(
    "--resources",
    type=str,
    default=None,
    help='Resources for task constraints like "GPU=2 MEM=10e9". '
    "Resources are applied separately to each worker process "
    "(only relevant when starting multiple worker processes with '--nworkers').",
)
@click.option(
    "--scheduler-file",
    type=str,
    default=None,
    help="Filename to JSON encoded scheduler information. "
    "Use with dask scheduler --scheduler-file",
)
@click.option(
    "--death-timeout",
    type=str,
    default=None,
    help="Seconds to wait for a scheduler before closing",
)
@click.option(
    "--dashboard-prefix", type=str, default="", help="Prefix for the dashboard"
)
@click.option(
    "--lifetime",
    type=str,
    default=None,
    help="If provided, shut down the worker after this duration.",
)
@click.option(
    "--lifetime-stagger",
    type=str,
    default=None,
    help="Random amount by which to stagger lifetime values",
)
@click.option(
    "--worker-class",
    type=str,
    default="dask.distributed.Worker",
    show_default=True,
    help="Worker class used to instantiate workers from.",
)
@click.option(
    "--lifetime-restart/--no-lifetime-restart",
    "lifetime_restart",
    default=None,
    required=False,
    help="Whether or not to restart the worker after the lifetime lapses. "
    "This assumes that you are using the --lifetime and --nanny keywords",
)
@click.option(
    "--preload",
    type=str,
    multiple=True,
    is_eager=True,
    help="Module that should be loaded by each worker process "
    'like "foo.bar" or "/path/to/foo.py"',
)
@click.argument(
    "preload_argv", nargs=-1, type=click.UNPROCESSED, callback=validate_preload_argv
)
@click.option(
    "--preload-nanny",
    type=str,
    multiple=True,
    is_eager=True,
    help="Module that should be loaded by each nanny "
    'like "foo.bar" or "/path/to/foo.py"',
)
@click.option(
    "--scheduler-sni",
    type=str,
    default=None,
    help="Scheduler SNI (if different from scheduler hostname)",
)
@click.version_option()
def main(  # type: ignore[no-untyped-def]
    scheduler,
    host,
    worker_port: str | None,
    listen_address,
    contact_address,
    nanny_port: str | None,
    nthreads,
    n_workers,
    nanny,
    name,
    pid_file,
    resources,
    dashboard,
    scheduler_file,
    dashboard_prefix,
    tls_ca_file,
    tls_cert,
    tls_key,
    dashboard_address,
    worker_class,
    preload_nanny,
    **kwargs,
):
    """Launch a Dask worker attached to an existing scheduler"""

    if "dask-worker" in sys.argv[0]:
        warnings.warn(
            "dask-worker is deprecated and will be removed in a future release; use `dask worker` instead",
            FutureWarning,
            stacklevel=1,
        )

    g0, g1, g2 = gc.get_threshold()  # https://github.com/dask/distributed/issues/1653
    gc.set_threshold(g0 * 3, g1 * 3, g2 * 3)

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    sec = {
        k: v
        for k, v in [
            ("tls_ca_file", tls_ca_file),
            ("tls_worker_cert", tls_cert),
            ("tls_worker_key", tls_key),
        ]
        if v is not None
    }

    if n_workers == "auto":
        n_workers, nthreads = nprocesses_nthreads()
    elif n_workers is None:
        n_workers = 1
    else:
        n_workers = int(n_workers)

    if n_workers < 0:
        n_workers = CPU_COUNT + 1 + n_workers

    if n_workers <= 0:
        logger.error(
            "Failed to launch worker. Must specify --nworkers so that there's at least one process."
        )
        sys.exit(1)

    if n_workers > 1 and not nanny:
        logger.error(
            "Failed to launch worker.  You cannot use the --no-nanny argument when n_workers > 1."
        )
        sys.exit(1)

    if contact_address and not listen_address:
        logger.error(
            "Failed to launch worker. "
            "Must specify --listen-address when --contact-address is given"
        )
        sys.exit(1)

    if n_workers > 1 and listen_address:
        logger.error(
            "Failed to launch worker. "
            "You cannot specify --listen-address when n_workers > 1."
        )
        sys.exit(1)

    if (worker_port or host) and listen_address:
        logger.error(
            "Failed to launch worker. "
            "You cannot specify --listen-address when --worker-port or --host is given."
        )
        sys.exit(1)

    try:
        if listen_address:
            host, _ = get_address_host_port(listen_address, strict=True)
            worker_port = str(_)
            if ":" in host:
                # IPv6 -- bracket to pass as user args
                host = f"[{host}]"

        if contact_address:
            # we only need this to verify it is getting parsed
            (_, _) = get_address_host_port(contact_address, strict=True)
        else:
            # if contact address is not present we use the listen_address for contact
            contact_address = listen_address
    except ValueError as e:  # pragma: no cover
        logger.error("Failed to launch worker. " + str(e))
        sys.exit(1)

    if not nthreads:
        nthreads = CPU_COUNT // n_workers

    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)

        atexit.register(del_pid_file)

    if resources:
        resources = resources.replace(",", " ").split()
        resources = dict(pair.split("=") for pair in resources)
        resources = valmap(float, resources)
    else:
        resources = None

    worker_class = import_term(worker_class)

    port_kwargs = _apportion_ports(worker_port, nanny_port, n_workers, nanny)
    assert len(port_kwargs) == n_workers

    if nanny:
        kwargs["worker_class"] = worker_class
        kwargs["preload_nanny"] = preload_nanny
        kwargs["listen_address"] = listen_address
        t = Nanny
    else:
        t = worker_class

    if (
        not scheduler
        and not scheduler_file
        and dask.config.get("scheduler-address", None) is None
    ):
        raise ValueError(
            "Need to provide scheduler address like\n"
            "dask worker SCHEDULER_ADDRESS:8786"
        )

    with suppress(TypeError, ValueError):
        name = int(name)

    signal_fired = False

    async def run():
        nannies = [
            t(
                scheduler,
                scheduler_file=scheduler_file,
                nthreads=nthreads,
                resources=resources,
                security=sec,
                contact_address=contact_address,
                host=host,
                dashboard=dashboard,
                dashboard_address=dashboard_address,
                name=(
                    name
                    if n_workers == 1 or name is None or name == ""
                    else str(name) + "-" + str(i)
                ),
                **kwargs,
                **port_kwargs_i,
            )
            for i, port_kwargs_i in enumerate(port_kwargs)
        ]

        async def wait_for_nannies_to_finish():
            """Wait for all nannies to initialize and finish"""
            await asyncio.gather(*nannies)
            await asyncio.gather(*(n.finished() for n in nannies))

        async def wait_for_signals_and_close():
            """Wait for SIGINT or SIGTERM and close all nannies upon receiving one of those signals"""
            nonlocal signal_fired
            signum = await wait_for_signals()

            signal_fired = True
            if nanny:
                # Unregister all workers from scheduler
                await asyncio.gather(
                    *(n.close(timeout=10, reason=f"signal-{signum}") for n in nannies)
                )

        wait_for_signals_and_close_task = asyncio.create_task(
            wait_for_signals_and_close()
        )
        wait_for_nannies_to_finish_task = asyncio.create_task(
            wait_for_nannies_to_finish()
        )

        done, _ = await asyncio.wait(
            [wait_for_signals_and_close_task, wait_for_nannies_to_finish_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Re-raise exceptions from done tasks
        [task.result() for task in done]

    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    except (TimeoutError, asyncio.TimeoutError):
        # We already log the exception in nanny / worker. Don't do it again.
        if not signal_fired:
            logger.info("Timed out starting worker")
        sys.exit(1)
    finally:
        logger.info("End worker")


def _apportion_ports(
    worker_port: str | None, nanny_port: str | None, n_workers: int, nanny: bool
) -> list[dict[str, Any]]:
    """Spread out evenly --worker-port and/or --nanny-port ranges to the workers and
    nannies, avoiding overlap.

    Returns
    =======
    List of kwargs to pass to the Worker or Nanny constructors
    """
    seen = set()

    def parse_unique(s: str | None) -> Iterator[int | None]:
        ports = parse_ports(s)
        if ports in ([0], [None]):
            for _ in range(n_workers):
                yield ports[0]
        else:
            for port in ports:
                if port not in seen:
                    seen.add(port)
                    yield port

    worker_ports_iter = parse_unique(worker_port)
    nanny_ports_iter = parse_unique(nanny_port)

    # [(worker ports, nanny ports), ...]
    ports: list[tuple[set[int | None], set[int | None]]] = [
        (set(), set()) for _ in range(n_workers)
    ]

    ports_iter = iter(ports)
    more_wps = True
    more_nps = True
    while more_wps or more_nps:
        try:
            worker_ports_i, nanny_ports_i = next(ports_iter)
        except StopIteration:
            # Start again in round-robin
            ports_iter = iter(ports)
            continue

        try:
            worker_ports_i.add(next(worker_ports_iter))
        except StopIteration:
            more_wps = False
        try:
            nanny_ports_i.add(next(nanny_ports_iter))
        except StopIteration:
            more_nps = False

    kwargs = []
    for worker_ports_i, nanny_ports_i in ports:
        if not worker_ports_i or not nanny_ports_i:
            if nanny:
                raise ValueError(
                    f"Not enough ports in range --worker_port {worker_port} "
                    f"--nanny_port {nanny_port} for {n_workers} workers"
                )
            else:
                raise ValueError(
                    f"Not enough ports in range --worker_port {worker_port} "
                    f"for {n_workers} workers"
                )

        # None and int can't be sorted together,
        # but None and 0 are guaranteed to be alone
        wp: Any = sorted(worker_ports_i)
        if len(wp) == 1:
            wp = wp[0]
        if nanny:
            np: Any = sorted(nanny_ports_i)
            if len(np) == 1:
                np = np[0]
            kwargs_i = {"port": np, "worker_port": wp}
        else:
            kwargs_i = {"port": wp}

        kwargs.append(kwargs_i)

    return kwargs


if __name__ == "__main__":
    main()  # pragma: no cover
