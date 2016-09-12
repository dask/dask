from __future__ import print_function, division, absolute_import

import atexit
from datetime import timedelta
import logging
import os
import shutil
import socket
from sys import argv, exit
import sys
from time import time, sleep

import click
from distributed import Nanny, Worker, sync, rpc
from distributed.nanny import isalive
from distributed.utils import get_ip, All
from distributed.worker import _ncores
from distributed.http import HTTPWorker
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop
from tornado import gen

logger = logging.getLogger('distributed.dask_worker')

global_nannies = []

import signal

def handle_signal(sig, frame):
    loop = IOLoop.instance()
    for nanny in global_nannies:
        try:
            shutil.rmtree(nanny.worker_dir)
        except (OSError, IOError, TypeError):
            pass
    if loop._running:
        loop.add_callback(loop.stop)
    else:
        exit(1)


@click.command()
@click.argument('scheduler', type=str)
@click.option('--worker-port', type=int, default=0,
              help="Serving worker port, defaults to randomly assigned")
@click.option('--http-port', type=int, default=0,
              help="Serving http port, defaults to randomly assigned")
@click.option('--nanny-port', type=int, default=0,
              help="Serving nanny port, defaults to randomly assigned")
@click.option('--host', type=str, default=None,
              help="Serving host. Defaults to an ip address that can hopefully"
                   " be visible from the scheduler network.")
@click.option('--nthreads', type=int, default=0,
              help="Number of threads per process. Defaults to number of cores")
@click.option('--nprocs', type=int, default=1,
              help="Number of worker processes.  Defaults to one.")
@click.option('--name', type=str, default='', help="Alias")
@click.option('--memory-limit', default=False,
              help="Number of bytes before spilling data to disk. "
              "This can be an integer (nbytes) float (fraction of total memory) or auto")
@click.option('--no-nanny', is_flag=True)
@click.option('--pid-file', type=str, default='',
              help="File to write the process PID")
@click.option('--temp-filename', default=None, help="Internal use only")
def main(scheduler, host, worker_port, http_port, nanny_port, nthreads, nprocs,
        no_nanny, name, memory_limit, pid_file, temp_filename):
    if no_nanny:
        port = worker_port
    else:
        port = nanny_port

    try:
        scheduler_host, scheduler_port = scheduler.split(':')
        scheduler_ip = socket.gethostbyname(scheduler_host)
        scheduler_port = int(scheduler_port)
    except IndexError:
        logger.info("Usage:  dask-worker scheduler_host:scheduler_port")

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

    loop = IOLoop.current()

    if memory_limit == 'auto':
        import psutil
        memory_limit = psutil.virtual_memory().total * 0.60

    if memory_limit:
        memory_limit = float(memory_limit)
        if memory_limit < 1.0:
            import psutil
            memory_limit = psutil.virtual_memory().total * memory_limit
        memory_limit /= nprocs
        memory_limit = int(memory_limit)

    if no_nanny:
        kwargs = {}
        if nanny_port:
            kwargs['service_ports'] = {'nanny': nanny_port}
        t = Worker
    else:
        kwargs = {'worker_port': worker_port}
        t = Nanny

    if host is not None:
        ip = socket.gethostbyname(host)
    else:
        # lookup the ip address of a local interface on a network that
        # reach the scheduler
        ip = get_ip(scheduler_ip, scheduler_port)
    nannies = [t(scheduler_ip, scheduler_port, ncores=nthreads, ip=ip,
                 services=services, name=name, loop=loop,
                 memory_limit=memory_limit, **kwargs)
               for i in range(nprocs)]

    for nanny in nannies:
        nanny.start(port)
        if t is Nanny:
            global_nannies.append(nanny)

    if temp_filename:
        @gen.coroutine
        def f():
            while nannies[0].status != 'running':
                yield gen.sleep(0.01)
            import json
            msg = {'port': nannies[0].port,
                   'local_directory': nannies[0].local_dir}
            with open(temp_filename, 'w') as f:
                json.dump(msg, f)
        loop.add_callback(f)

    loop.start()
    logger.info("End worker")
    loop.close()

    loop2 = IOLoop()

    @gen.coroutine
    def f():
        scheduler = rpc(ip=nannies[0].scheduler.ip,
                        port=nannies[0].scheduler.port)
        if not no_nanny:
            yield gen.with_timeout(timedelta(seconds=2),
                    All([scheduler.unregister(address=n.worker_address, close=True)
                        for n in nannies if n.process and n.worker_port]), io_loop=loop2)

    loop2.run_sync(f)

    if not no_nanny:
        for n in nannies:
            n.process.terminate()

    if not no_nanny:
        start = time()
        while (any(isalive(n.process) for n in nannies)
                and time() < start + 1):
            sleep(0.1)

    for nanny in nannies:
        nanny.stop()


def go():
    # NOTE: We can't use the generic install_signal_handlers() function from
    # distributed.cli.utils because we're handling the signal differently.
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    check_python_3()
    main()

if __name__ == '__main__':
    go()
