import atexit
import logging
import multiprocessing
import gc
import os
from sys import exit
import warnings

import click
import dask
from distributed import Nanny, Worker
from distributed.security import Security
from distributed.cli.utils import check_python_3, install_signal_handlers
from distributed.comm import get_address_host_port
from distributed.preloading import validate_preload_argv
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

from toolz import valmap
from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen

logger = logging.getLogger("distributed.dask_worker")


pem_file_option_type = click.Path(exists=True, resolve_path=True)


@click.command(context_settings=dict(ignore_unknown_options=True))
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
    type=int,
    default=0,
    help="Serving computation port, defaults to random",
)
@click.option(
    "--nanny-port", type=int, default=0, help="Serving nanny port, defaults to random"
)
@click.option(
    "--bokeh-port", type=int, default=None, help="Deprecated.  See --dashboard-address"
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
    "--bokeh/--no-bokeh",
    "bokeh",
    default=None,
    help="Deprecated.  See --dashboard/--no-dashboard.",
    required=False,
)
@click.option(
    "--listen-address",
    type=str,
    default=None,
    help="The address to which the worker binds. " "Example: tcp://0.0.0.0:9000",
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
    "--nprocs",
    type=int,
    default=1,
    show_default=True,
    help="Number of worker processes to launch.",
)
@click.option(
    "--name",
    type=str,
    default=None,
    help="A unique name for this worker like 'worker-1'. "
    "If used with --nprocs then the process number "
    "will be appended like name-0, name-1, name-2, ...",
)
@click.option(
    "--memory-limit",
    default="auto",
    show_default=True,
    help="Bytes of memory per process that the worker can use. "
    "This can be an integer (bytes), "
    "float (fraction of total system memory), "
    "string (like 5GB or 5000M), "
    "'auto', or zero for no memory management",
)
@click.option(
    "--reconnect/--no-reconnect",
    default=True,
    help="Reconnect to scheduler if disconnected [default: --reconnect]",
)
@click.option(
    "--nanny/--no-nanny",
    default=True,
    help="Start workers in nanny process for management [default: --nanny]",
)
@click.option("--pid-file", type=str, default="", help="File to write the process PID")
@click.option(
    "--local-directory", default="", type=str, help="Directory to place worker files"
)
@click.option(
    "--resources",
    type=str,
    default="",
    help='Resources for task constraints like "GPU=2 MEM=10e9". '
    "Resources are applied separately to each worker process "
    "(only relevant when starting multiple worker processes with '--nprocs').",
)
@click.option(
    "--scheduler-file",
    type=str,
    default="",
    help="Filename to JSON encoded scheduler information. "
    "Use with dask-scheduler --scheduler-file",
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
    default="",
    help="If provided, shut down the worker after this duration.",
)
@click.option(
    "--lifetime-stagger",
    type=str,
    default="0 seconds",
    show_default=True,
    help="Random amount by which to stagger lifetime values",
)
@click.option(
    "--lifetime-restart/--no-lifetime-restart",
    "lifetime_restart",
    default=False,
    show_default=True,
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
@click.version_option()
def main(
    scheduler,
    host,
    worker_port,
    listen_address,
    contact_address,
    nanny_port,
    nthreads,
    nprocs,
    nanny,
    name,
    pid_file,
    resources,
    dashboard,
    bokeh,
    bokeh_port,
    scheduler_file,
    dashboard_prefix,
    tls_ca_file,
    tls_cert,
    tls_key,
    dashboard_address,
    **kwargs
):
    g0, g1, g2 = gc.get_threshold()  # https://github.com/dask/distributed/issues/1653
    gc.set_threshold(g0 * 3, g1 * 3, g2 * 3)

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if bokeh_port is not None:
        warnings.warn(
            "The --bokeh-port flag has been renamed to --dashboard-address. "
            "Consider adding ``--dashboard-address :%d`` " % bokeh_port
        )
        dashboard_address = bokeh_port
    if bokeh is not None:
        warnings.warn(
            "The --bokeh/--no-bokeh flag has been renamed to --dashboard/--no-dashboard. "
        )
        dashboard = bokeh

    sec = Security(
        **{
            k: v
            for k, v in [
                ("tls_ca_file", tls_ca_file),
                ("tls_worker_cert", tls_cert),
                ("tls_worker_key", tls_key),
            ]
            if v is not None
        }
    )

    if nprocs > 1 and worker_port != 0:
        logger.error(
            "Failed to launch worker.  You cannot use the --port argument when nprocs > 1."
        )
        exit(1)

    if nprocs > 1 and not nanny:
        logger.error(
            "Failed to launch worker.  You cannot use the --no-nanny argument when nprocs > 1."
        )
        exit(1)

    if contact_address and not listen_address:
        logger.error(
            "Failed to launch worker. "
            "Must specify --listen-address when --contact-address is given"
        )
        exit(1)

    if nprocs > 1 and listen_address:
        logger.error(
            "Failed to launch worker. "
            "You cannot specify --listen-address when nprocs > 1."
        )
        exit(1)

    if (worker_port or host) and listen_address:
        logger.error(
            "Failed to launch worker. "
            "You cannot specify --listen-address when --worker-port or --host is given."
        )
        exit(1)

    try:
        if listen_address:
            (host, worker_port) = get_address_host_port(listen_address, strict=True)

        if contact_address:
            # we only need this to verify it is getting parsed
            (_, _) = get_address_host_port(contact_address, strict=True)
        else:
            # if contact address is not present we use the listen_address for contact
            contact_address = listen_address
    except ValueError as e:
        logger.error("Failed to launch worker. " + str(e))
        exit(1)

    if nanny:
        port = nanny_port
    else:
        port = worker_port

    if not nthreads:
        nthreads = multiprocessing.cpu_count() // nprocs

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

    loop = IOLoop.current()

    if nanny:
        kwargs.update({"worker_port": worker_port, "listen_address": listen_address})
        t = Nanny
    else:
        if nanny_port:
            kwargs["service_ports"] = {"nanny": nanny_port}
        t = Worker

    if (
        not scheduler
        and not scheduler_file
        and dask.config.get("scheduler-address", None) is None
    ):
        raise ValueError(
            "Need to provide scheduler address like\n"
            "dask-worker SCHEDULER_ADDRESS:8786"
        )

    nannies = [
        t(
            scheduler,
            scheduler_file=scheduler_file,
            nthreads=nthreads,
            loop=loop,
            resources=resources,
            security=sec,
            contact_address=contact_address,
            host=host,
            port=port,
            dashboard_address=dashboard_address if dashboard else None,
            service_kwargs={"dashboard": {"prefix": dashboard_prefix}},
            name=name if nprocs == 1 or not name else name + "-" + str(i),
            **kwargs
        )
        for i in range(nprocs)
    ]

    @gen.coroutine
    def close_all():
        # Unregister all workers from scheduler
        if nanny:
            yield [n.close(timeout=2) for n in nannies]

    def on_signal(signum):
        logger.info("Exiting on signal %d", signum)
        close_all()

    @gen.coroutine
    def run():
        yield nannies
        yield [n.finished() for n in nannies]

    install_signal_handlers(loop, cleanup=on_signal)

    try:
        loop.run_sync(run)
    except TimeoutError:
        # We already log the exception in nanny / worker. Don't do it again.
        raise TimeoutError("Timed out starting worker.") from None
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("End worker")


def go():
    check_python_3()
    main()


if __name__ == "__main__":
    go()
