from __future__ import annotations

import asyncio
import atexit
import gc
import logging
import os
import re
import sys
import warnings

import click

from distributed import Scheduler
from distributed._signals import wait_for_signals
from distributed.compatibility import asyncio_run
from distributed.config import get_loop_factory
from distributed.preloading import validate_preload_argv
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

logger = logging.getLogger("distributed.scheduler")


pem_file_option_type = click.Path(exists=True, resolve_path=True)


@click.command(name="scheduler", context_settings=dict(ignore_unknown_options=True))
@click.option("--host", type=str, default="", help="URI, IP or hostname of this server")
@click.option("--port", type=str, default=None, help="Serving port")
@click.option(
    "--interface",
    type=str,
    default=None,
    help="Preferred network interface like 'eth0' or 'ib0'",
)
@click.option(
    "--protocol", type=str, default=None, help="Protocol like tcp, tls, or ucx"
)
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
# XXX default port (or URI) values should be centralized somewhere
@click.option(
    "--dashboard-address",
    type=str,
    default=":8787",
    show_default=True,
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
    "--jupyter/--no-jupyter",
    "jupyter",
    default=False,
    required=False,
    help="Start a Jupyter Server in the same process.  Warning: This will make"
    "it possible for anyone with access to your dashboard address to run"
    "Python code",
)
@click.option("--show/--no-show", default=False, help="Show web UI [default: --show]")
@click.option(
    "--dashboard-prefix", type=str, default="", help="Prefix for the dashboard app"
)
@click.option(
    "--use-xheaders",
    type=bool,
    default=False,
    show_default=True,
    help="User xheaders in dashboard app for ssl termination in header",
)
@click.option("--pid-file", type=str, default="", help="File to write the process PID")
@click.option(
    "--scheduler-file",
    type=str,
    default="",
    help="File to write connection information. "
    "This may be a good way to share connection information if your "
    "cluster is on a shared network file system.",
)
@click.option(
    "--preload",
    type=str,
    multiple=True,
    is_eager=True,
    help="Module that should be loaded by the scheduler process  "
    'like "foo.bar" or "/path/to/foo.py".',
)
@click.argument(
    "preload_argv", nargs=-1, type=click.UNPROCESSED, callback=validate_preload_argv
)
@click.option(
    "--idle-timeout",
    default=None,
    type=str,
    help="Time of inactivity after which to kill the scheduler",
)
@click.version_option()
def main(
    host,
    port,
    protocol,
    interface,
    show,
    dashboard,
    dashboard_prefix,
    use_xheaders,
    pid_file,
    tls_ca_file,
    tls_cert,
    tls_key,
    dashboard_address,
    jupyter,
    **kwargs,
):
    """Launch a Dask scheduler."""

    if "dask-scheduler" in sys.argv[0]:
        warnings.warn(
            "dask-scheduler is deprecated and will be removed in a future release; use `dask scheduler` instead",
            FutureWarning,
            stacklevel=1,
        )

    g0, g1, g2 = gc.get_threshold()  # https://github.com/dask/distributed/issues/1653
    gc.set_threshold(g0 * 3, g1 * 3, g2 * 3)

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if interface and "," in interface:
        interface = interface.split(",")

    if protocol and "," in protocol:
        protocol = protocol.split(",")

    if port:
        if "," in port:
            port = [int(p) for p in port.split(",")]
        else:
            port = int(port)

    if port is None and (not host or not re.search(r":\d", host)):
        if isinstance(protocol, list):
            port = [8786] + [0] * (len(protocol) - 1)
        else:
            port = 8786

    if isinstance(protocol, list) or isinstance(port, list):
        if (not isinstance(protocol, list) or not isinstance(port, list)) or len(
            port
        ) != len(protocol):
            raise ValueError("--protocol and --port must both be lists of equal length")

    sec = {
        k: v
        for k, v in [
            ("tls_ca_file", tls_ca_file),
            ("tls_scheduler_cert", tls_cert),
            ("tls_scheduler_key", tls_key),
        ]
        if v is not None
    }

    if not host and (tls_ca_file or tls_cert or tls_key):
        host = "tls://"

    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)

        atexit.register(del_pid_file)

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    async def run():
        logger.info("-" * 47)

        scheduler = Scheduler(
            security=sec,
            host=host,
            port=port,
            protocol=protocol,
            interface=interface,
            dashboard=dashboard,
            dashboard_address=dashboard_address,
            http_prefix=dashboard_prefix,
            jupyter=jupyter,
            **kwargs,
        )
        logger.info("-" * 47)

        async def wait_for_scheduler_to_finish():
            """Wait for the scheduler to initialize and finish"""
            await scheduler
            await scheduler.finished()

        async def wait_for_signals_and_close():
            """Wait for SIGINT or SIGTERM and close the scheduler upon receiving one of those signals"""
            signum = await wait_for_signals()
            await scheduler.close(reason=f"signal-{signum}")

        wait_for_signals_and_close_task = asyncio.create_task(
            wait_for_signals_and_close()
        )
        wait_for_scheduler_to_finish_task = asyncio.create_task(
            wait_for_scheduler_to_finish()
        )

        done, _ = await asyncio.wait(
            [wait_for_signals_and_close_task, wait_for_scheduler_to_finish_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Re-raise exceptions from done tasks
        [task.result() for task in done]
        logger.info("Stopped scheduler at %r", scheduler.address)

    try:
        asyncio_run(run(), loop_factory=get_loop_factory())
    finally:
        logger.info("End scheduler")


if __name__ == "__main__":
    main()  # pragma: no cover
