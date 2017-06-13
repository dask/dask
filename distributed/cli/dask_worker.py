from __future__ import print_function, division, absolute_import

import atexit
from datetime import timedelta
from functools import partial
import json
import logging
import os
import shutil
import signal
from sys import exit
from time import sleep

import click
from distributed import Nanny, Worker, rpc
from distributed.utils import All, get_ip_interface
from distributed.worker import _ncores
from distributed.http import HTTPWorker
from distributed.metrics import time
from distributed.security import Security
from distributed.cli.utils import check_python_3, uri_from_host_port

from toolz import valmap
from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen

logger = logging.getLogger('distributed.dask_worker')


pem_file_option_type = click.Path(exists=True, resolve_path=True)

@click.command()
@click.argument('scheduler', type=str, required=False)
@click.option('--tls-ca-file', type=pem_file_option_type, default=None,
              help="CA cert(s) file for TLS (in PEM format)")
@click.option('--tls-cert', type=pem_file_option_type, default=None,
              help="certificate file for TLS (in PEM format)")
@click.option('--tls-key', type=pem_file_option_type, default=None,
              help="private key file for TLS (in PEM format)")
@click.option('--worker-port', type=int, default=0,
              help="Serving computation port, defaults to random")
@click.option('--http-port', type=int, default=0,
              help="Serving http port, defaults to random")
@click.option('--nanny-port', type=int, default=0,
              help="Serving nanny port, defaults to random")
@click.option('--bokeh-port', type=int, default=8789,
              help="Bokeh port, defaults to 8789")
@click.option('--bokeh/--no-bokeh', 'bokeh', default=True, show_default=True,
              required=False, help="Launch Bokeh Web UI")
@click.option('--host', type=str, default=None,
              help="Serving host. Should be an ip address that is"
                   " visible to the scheduler and other workers. "
                   "See --interface.")
@click.option('--interface', type=str, default=None,
              help="Network interface like 'eth0' or 'ib0'")
@click.option('--nthreads', type=int, default=0,
              help="Number of threads per process.")
@click.option('--nprocs', type=int, default=1,
              help="Number of worker processes.  Defaults to one.")
@click.option('--name', type=str, default='',
              help="A unique name for this worker like 'worker-1'")
@click.option('--memory-limit', default='auto',
              help="Number of bytes before spilling data to disk. "
                   "This can be an integer (nbytes) "
                   "float (fraction of total memory) "
                   "or 'auto'")
@click.option('--reconnect/--no-reconnect', default=True,
              help="Reconnect to scheduler if disconnected")
@click.option('--nanny/--no-nanny', default=True,
              help="Start workers in nanny process for management")
@click.option('--pid-file', type=str, default='',
              help="File to write the process PID")
@click.option('--local-directory', default='', type=str,
              help="Directory to place worker files")
@click.option('--resources', type=str, default='',
              help='Resources for task constraints like "GPU=2 MEM=10e9"')
@click.option('--scheduler-file', type=str, default='',
              help='Filename to JSON encoded scheduler information. '
                   'Use with dask-scheduler --scheduler-file')
@click.option('--death-timeout', type=float, default=None,
              help="Seconds to wait for a scheduler before closing")
@click.option('--bokeh-prefix', type=str, default=None,
              help="Prefix for the bokeh app")
@click.option('--preload', type=str, multiple=True,
              help='Module that should be loaded by each worker process '
                   'like "foo.bar" or "/path/to/foo.py"')
def main(scheduler, host, worker_port, http_port, nanny_port, nthreads, nprocs,
         nanny, name, memory_limit, pid_file, reconnect,
         resources, bokeh, bokeh_port, local_directory, scheduler_file,
         interface, death_timeout, preload, bokeh_prefix,
         tls_ca_file, tls_cert, tls_key):
    sec = Security(tls_ca_file=tls_ca_file,
                   tls_worker_cert=tls_cert,
                   tls_worker_key=tls_key,
                   )

    if nanny:
        port = nanny_port
    else:
        port = worker_port

    if nprocs > 1 and worker_port != 0:
        logger.error("Failed to launch worker.  You cannot use the --port argument when nprocs > 1.")
        exit(1)

    if nprocs > 1 and name:
        logger.error("Failed to launch worker.  You cannot use the --name argument when nprocs > 1.")
        exit(1)

    if not nthreads:
        nthreads = _ncores // nprocs

    if pid_file:
        with open(pid_file, 'w') as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)
        atexit.register(del_pid_file)

    services = {('http', http_port): HTTPWorker}

    if bokeh:
        try:
            from distributed.bokeh.worker import BokehWorker
        except ImportError:
            pass
        else:
            if bokeh_prefix:
                result = (BokehWorker, {'prefix': bokeh_prefix})
            else:
                result = BokehWorker
            services[('bokeh', bokeh_port)] = result

    if resources:
        resources = resources.replace(',', ' ').split()
        resources = dict(pair.split('=') for pair in resources)
        resources = valmap(float, resources)
    else:
        resources = None

    loop = IOLoop.current()

    if nanny:
        kwargs = {'worker_port': worker_port}
        t = Nanny
    else:
        kwargs = {}
        if nanny_port:
            kwargs['service_ports'] = {'nanny': nanny_port}
        t = Worker

    if scheduler_file:
        while not os.path.exists(scheduler_file):
            sleep(0.01)
        for i in range(10):
            try:
                with open(scheduler_file) as f:
                    cfg = json.load(f)
                scheduler = cfg['address']
                break
            except (ValueError, KeyError):  # race with scheduler on file
                sleep(0.01)

    if not scheduler:
        raise ValueError("Need to provide scheduler address like\n"
                         "dask-worker SCHEDULER_ADDRESS:8786")

    if interface:
        if host:
            raise ValueError("Can not specify both interface and host")
        else:
            host = get_ip_interface(interface)

    if host or port:
        addr = uri_from_host_port(host, port, 0)
    else:
        # Choose appropriate address for scheduler
        addr = None

    nannies = [t(scheduler, ncores=nthreads,
                 services=services, name=name, loop=loop, resources=resources,
                 memory_limit=memory_limit, reconnect=reconnect,
                 local_dir=local_directory, death_timeout=death_timeout,
                 preload=preload, security=sec,
                 **kwargs)
               for i in range(nprocs)]

    @gen.coroutine
    def close_all():
        try:
            if nanny:
                yield [n._close(timeout=2) for n in nannies]
        finally:
            loop.stop()

    def handle_signal(signum, frame):
        logger.info("Exiting on signal %d", signum)
        if loop._running:
            loop.add_callback_from_signal(loop.stop)
        else:
            exit(0)

    # NOTE: We can't use the generic install_signal_handlers() function from
    # distributed.cli.utils because we're handling the signal differently.
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    for n in nannies:
        n.start(addr)

    @gen.coroutine
    def run():
        while all(n.status != 'closed' for n in nannies):
            yield gen.sleep(0.2)

    try:
        loop.run_sync(run)
    except (KeyboardInterrupt, TimeoutError):
        pass
    finally:
        logger.info("End worker")

    # Clean exit: unregister all workers from scheduler
    loop.run_sync(close_all)


def go():
    check_python_3()
    main()

if __name__ == '__main__':
    go()
