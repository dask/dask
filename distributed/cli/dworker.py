import logging
from sys import argv, exit
import socket

import click
from distributed import Nanny, Worker, sync
from distributed.utils import get_ip
from distributed.worker import _ncores
from distributed.http import HTTPWorker
from tornado.ioloop import IOLoop
from tornado import gen

logger = logging.getLogger('distributed.dworker')

ip = get_ip()


@click.command()
@click.argument('center', type=str)
@click.option('--port', type=int, default=0,
              help="Serving port, defaults to randomly assigned")
@click.option('--host', type=str, default=None,
              help="Serving host defaults to %s" % ip)
@click.option('--nthreads', type=int, default=0,
              help="Number of threads per process. Defaults to number of cores")
@click.option('--nprocs', type=int, default=1,
              help="Number of worker processes.  Defaults to one.")
@click.option('--no-nanny', is_flag=True)
def go(center, host, port, nthreads, nprocs, no_nanny):
    try:
        center_ip, center_port = center.split(':')
        center_port = int(center_port)
    except IndexError:
        logger.info("Usage:  dworker center_host:center_port")

    if nprocs > 1 and port != 0:
        logger.error("Failed to launch worker.  You cannot use the --port argument when nprocs > 1.")
        exit(1)

    if not nthreads:
        nthreads = _ncores // nprocs

    if nprocs > 1 and port != 0:
        raise ValueError("Can not specify a port when using multiple processes")

    services = {'http': HTTPWorker}

    loop = IOLoop.current()
    t = Worker if no_nanny else Nanny
    nannies = [t(center_ip, center_port, ncores=nthreads, ip=host,
                 services=services)
                for i in range(nprocs)]

    for nanny in nannies:
        loop.add_callback(nanny._start, port)
    try:
        loop.start()
    except KeyboardInterrupt:
        if not no_nanny:
            @gen.coroutine
            def stop():
                results = yield [nanny._kill() for nanny in nannies]
                raise gen.Return(results)

            results = IOLoop().run_sync(stop)

        for r, nanny in zip(results, nannies):
            if r == b'OK':
                logger.info("End nanny at %s:%d", nanny.ip, nanny.port)
            else:
                logger.error("Failed to cleanly kill worker %s:%d",
                             nanny.ip, nanny.port)

if __name__ == '__main__':
    go()
