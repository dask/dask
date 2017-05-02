from __future__ import print_function, division, absolute_import

import atexit
import logging
import os
import shutil
import sys
import tempfile

import click

from distributed import Scheduler
from distributed.utils import ignoring, open_port, get_ip_interface
from distributed.http import HTTPScheduler
from distributed.cli.utils import (check_python_3, install_signal_handlers,
                                   uri_from_host_port)
from distributed.preloading import preload_modules
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')


@click.command()
@click.option('--port', type=int, default=None, help="Serving port")
# XXX default port (or URI) values should be centralized somewhere
@click.option('--http-port', type=int, default=9786,
              help="HTTP port for JSON API diagnostics")
@click.option('--bokeh-port', type=int, default=8787,
              help="Bokeh port for visual diagnostics")
@click.option('--bokeh-external-port', type=int, default=None,
              help="Optional port for the external (old) Bokeh server")
@click.option('--bokeh-internal-port', type=int, default=None,
              help="Deprecated. Use --bokeh-port instead")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default='',
              help="IP, hostname or URI of this server")
@click.option('--interface', type=str, default=None,
              help="Preferred network interface like 'eth0' or 'ib0'")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
@click.option('--prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--use-xheaders', type=bool, default=False, show_default=True,
              help="User xheaders in bokeh app for ssl termination in header")
@click.option('--pid-file', type=str, default='',
              help="File to write the process PID")
@click.option('--scheduler-file', type=str, default='',
              help="File to write connection information. "
              "This may be a good way to share connection information if your "
              "cluster is on a shared network file system.")
@click.option('--local-directory', default='', type=str,
              help="Directory to place scheduler files")
@click.option('--preload', type=str, multiple=True,
              help='Module that should be loaded by each worker process like "foo.bar" or "/path/to/foo.py"')
def main(host, port, http_port, bokeh_port, bokeh_external_port,
         bokeh_internal_port, show, _bokeh, bokeh_whitelist, prefix,
         use_xheaders, pid_file, scheduler_file, interface, local_directory,
         preload):

    if bokeh_internal_port:
        print("The --bokeh-internal-port keyword has been removed.\n"
              "The internal bokeh server is now the default bokeh server.\n"
              "Use --bokeh-port %d instead" % bokeh_internal_port)
        sys.exit(1)

    if pid_file:
        with open(pid_file, 'w') as f:
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
        local_directory = tempfile.mkdtemp(prefix='scheduler-')
        local_directory_created = True
    if local_directory not in sys.path:
        sys.path.insert(0, local_directory)

    if sys.platform.startswith('linux'):
        import resource   # module fails importing on Windows
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
    logger.info('-' * 47)

    services = {('http', http_port): HTTPScheduler}
    if _bokeh:
        with ignoring(ImportError):
            from distributed.bokeh.scheduler import BokehScheduler
            services[('bokeh', bokeh_port)] = BokehScheduler
    scheduler = Scheduler(loop=loop, services=services,
                          scheduler_file=scheduler_file)
    scheduler.start(addr)
    preload_modules(preload, parameter=scheduler, file_dir=local_directory)

    bokeh_proc = None
    if _bokeh and bokeh_external_port is not None:
        if bokeh_external_port == 0: # This is a hack and not robust
            bokeh_port = open_port() # This port may be taken by the OS
        try:                         # before we successfully pass it to Bokeh
            from distributed.bokeh.application import BokehWebInterface
            bokeh_proc = BokehWebInterface(http_port=http_port,
                    scheduler_address=scheduler.address,
                    bokeh_port=bokeh_external_port,
                    bokeh_whitelist=bokeh_whitelist, show=show, prefix=prefix,
                    use_xheaders=use_xheaders, quiet=False)
        except ImportError:
            logger.info("Please install Bokeh to get Web UI")
        except Exception as e:
            logger.warn("Could not start Bokeh web UI", exc_info=True)

    logger.info('Local Directory: %26s', local_directory)
    logger.info('-' * 47)
    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
        if bokeh_proc:
            bokeh_proc.close()
        if local_directory_created:
            shutil.rmtree(local_directory)

        logger.info("End scheduler at %r", addr)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
