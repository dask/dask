from __future__ import print_function, division, absolute_import

import atexit
from functools import partial
import logging
import os
import shutil
import sys
import tempfile

import click

from distributed import Scheduler
from distributed.security import Security
from distributed.utils import ignoring, open_port, get_ip_interface
from distributed.http import HTTPScheduler
from distributed.cli.utils import (check_python_3, install_signal_handlers,
                                   uri_from_host_port)
from distributed.preloading import preload_modules
from tornado.ioloop import IOLoop

logger = logging.getLogger('distributed.scheduler')


pem_file_option_type = click.Path(exists=True, resolve_path=True)

@click.command()
@click.option('--host', type=str, default='',
              help="URI, IP or hostname of this server")
@click.option('--port', type=int, default=None, help="Serving port")
@click.option('--interface', type=str, default=None,
              help="Preferred network interface like 'eth0' or 'ib0'")
@click.option('--tls-ca-file', type=pem_file_option_type, default=None,
              help="CA cert(s) file for TLS (in PEM format)")
@click.option('--tls-cert', type=pem_file_option_type, default=None,
              help="certificate file for TLS (in PEM format)")
@click.option('--tls-key', type=pem_file_option_type, default=None,
              help="private key file for TLS (in PEM format)")
# XXX default port (or URI) values should be centralized somewhere
@click.option('--http-port', type=int, default=9786,
              help="HTTP port for JSON API diagnostics")
@click.option('--bokeh-port', type=int, default=8787,
              help="Bokeh port for visual diagnostics")
@click.option('--bokeh-internal-port', type=int, default=None,
              help="Deprecated. Use --bokeh-port instead")
@click.option('--bokeh/--no-bokeh', '_bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--show/--no-show', default=False, help="Show web UI")
@click.option('--bokeh-whitelist', default=None, multiple=True,
              help="IP addresses to whitelist for bokeh.")
@click.option('--bokeh-prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--prefix', type=str, default=None,
              help="Deprecated, see --bokeh-prefix")
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
def main(host, port, http_port, bokeh_port, bokeh_internal_port, show, _bokeh,
         bokeh_whitelist, bokeh_prefix, use_xheaders, pid_file, scheduler_file,
         interface, local_directory, preload, prefix, tls_ca_file, tls_cert,
         tls_key):

    if bokeh_internal_port:
        print("The --bokeh-internal-port keyword has been removed.\n"
              "The internal bokeh server is now the default bokeh server.\n"
              "Use --bokeh-port %d instead" % bokeh_internal_port)
        sys.exit(1)

    if prefix:
        print("The --prefix keyword has moved to --bokeh-prefix")
        sys.exit(1)

    sec = Security(tls_ca_file=tls_ca_file,
                   tls_scheduler_cert=tls_cert,
                   tls_scheduler_key=tls_key,
                   )

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
            services[('bokeh', bokeh_port)] = partial(BokehScheduler,
                                                      prefix=bokeh_prefix)
    scheduler = Scheduler(loop=loop, services=services,
                          scheduler_file=scheduler_file,
                          security=sec)
    scheduler.start(addr)
    preload_modules(preload, parameter=scheduler, file_dir=local_directory)

    logger.info('Local Directory: %26s', local_directory)
    logger.info('-' * 47)
    try:
        loop.start()
        loop.close()
    finally:
        scheduler.stop()
        if local_directory_created:
            shutil.rmtree(local_directory)

        logger.info("End scheduler at %r", addr)


def go():
    install_signal_handlers()
    check_python_3()
    main()


if __name__ == '__main__':
    go()
