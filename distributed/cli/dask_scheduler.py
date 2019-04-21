from __future__ import print_function, division, absolute_import

import atexit
import dask
import logging
import gc
import os
import shutil
import sys
import tempfile
import warnings

import click

from tornado.ioloop import IOLoop

from distributed import Scheduler
from distributed.security import Security
from distributed.utils import get_ip_interface
from distributed.cli.utils import (
    check_python_3,
    install_signal_handlers,
    uri_from_host_port,
)
from distributed.preloading import preload_modules, validate_preload_argv
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

logger = logging.getLogger("distributed.scheduler")


pem_file_option_type = click.Path(exists=True, resolve_path=True)


@click.command(context_settings=dict(ignore_unknown_options=True))
@click.option("--host", type=str, default="", help="URI, IP or hostname of this server")
@click.option("--port", type=int, default=None, help="Serving port")
@click.option(
    "--interface",
    type=str,
    default=None,
    help="Preferred network interface like 'eth0' or 'ib0'",
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
    "--bokeh-port", type=int, default=None, help="Deprecated.  See --dashboard-address"
)
@click.option(
    "--dashboard-address",
    type=str,
    default=":8787",
    help="Address on which to listen for diagnostics dashboard",
)
@click.option(
    "--bokeh/--no-bokeh",
    "_bokeh",
    default=True,
    show_default=True,
    required=False,
    help="Launch Bokeh Web UI",
)
@click.option("--show/--no-show", default=False, help="Show web UI")
@click.option(
    "--bokeh-whitelist",
    default=None,
    multiple=True,
    help="IP addresses to whitelist for bokeh.",
)
@click.option("--bokeh-prefix", type=str, default=None, help="Prefix for the bokeh app")
@click.option(
    "--use-xheaders",
    type=bool,
    default=False,
    show_default=True,
    help="User xheaders in bokeh app for ssl termination in header",
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
    "--local-directory", default="", type=str, help="Directory to place scheduler files"
)
@click.option(
    "--preload",
    type=str,
    multiple=True,
    is_eager=True,
    default="",
    help="Module that should be loaded by the scheduler process  "
    'like "foo.bar" or "/path/to/foo.py".',
)
@click.argument(
    "preload_argv", nargs=-1, type=click.UNPROCESSED, callback=validate_preload_argv
)
def main(
    host,
    port,
    bokeh_port,
    show,
    _bokeh,
    bokeh_whitelist,
    bokeh_prefix,
    use_xheaders,
    pid_file,
    scheduler_file,
    interface,
    local_directory,
    preload,
    preload_argv,
    tls_ca_file,
    tls_cert,
    tls_key,
    dashboard_address,
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

    sec = Security(
        tls_ca_file=tls_ca_file, tls_scheduler_cert=tls_cert, tls_scheduler_key=tls_key
    )

    if not host and (tls_ca_file or tls_cert or tls_key):
        host = "tls://"

    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)

        atexit.register(del_pid_file)

    local_directory_created = False
    if local_directory:
        if not os.path.exists(local_directory):
            os.mkdir(local_directory)
            local_directory_created = True
    else:
        local_directory = tempfile.mkdtemp(prefix="scheduler-")
        local_directory_created = True
    if local_directory not in sys.path:
        sys.path.insert(0, local_directory)

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    if interface:
        if host:
            raise ValueError("Can not specify both interface and host")
        else:
            host = get_ip_interface(interface)

    addr = uri_from_host_port(host, port, 8786)

    loop = IOLoop.current()
    logger.info("-" * 47)

    services = {}
    if _bokeh:
        try:
            from distributed.bokeh.scheduler import BokehScheduler

            services[("bokeh", dashboard_address)] = (
                BokehScheduler,
                {"prefix": bokeh_prefix},
            )
        except ImportError as error:
            if str(error).startswith("No module named"):
                logger.info("Web dashboard not loaded.  Unable to import bokeh")
            else:
                logger.info("Unable to import bokeh: %s" % str(error))

    scheduler = Scheduler(
        loop=loop, services=services, scheduler_file=scheduler_file, security=sec
    )
    scheduler.start(addr)
    if not preload:
        preload = dask.config.get("distributed.scheduler.preload")
    if not preload_argv:
        preload_argv = dask.config.get("distributed.scheduler.preload-argv")
    preload_modules(
        preload, parameter=scheduler, file_dir=local_directory, argv=preload_argv
    )

    logger.info("Local Directory: %26s", local_directory)
    logger.info("-" * 47)

    install_signal_handlers(loop)

    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
        if local_directory_created:
            shutil.rmtree(local_directory)

        logger.info("End scheduler at %r", addr)


def go():
    check_python_3()
    main()


if __name__ == "__main__":
    go()
