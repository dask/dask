from __future__ import print_function, division, absolute_import

import logging
from sys import argv, exit
import socket

import click
from distributed import Nanny, Worker, sync
from distributed.utils import get_ip
from distributed.worker import _ncores
from distributed.http import HTTPWorker
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop
from tornado import gen

logger = logging.getLogger('distributed.dworker')


@click.command()
@click.argument('center', type=str)
@click.option('--worker-port', type=int, default=0,
              help="Serving worker port, defaults to randomly assigned")
@click.option('--http-port', type=int, default=0,
              help="Serving http port, defaults to randomly assigned")
@click.option('--nanny-port', type=int, default=0,
              help="Serving nanny port, defaults to randomly assigned")
@click.option('--port', type=int, default=0,
              help="Deprecated, see --nanny-port")
@click.option('--host', type=str, default=None,
              help="Serving host. Defaults to an ip address that can hopefully"
                   " be visible from the center network.")
@click.option('--nthreads', type=int, default=0,
              help="Number of threads per process. Defaults to number of cores")
@click.option('--nprocs', type=int, default=1,
              help="Number of worker processes.  Defaults to one.")
@click.option('--name', type=str, default='', help="Alias")
@click.option('--no-nanny', is_flag=True)
def main(center, host, worker_port, http_port, nanny_port, nthreads, nprocs,
        no_nanny, name, port):
    if port:
        logger.info("--port is deprecated, use --nanny-port instead")
        assert not nanny_port
        nanny_port = port
    try:
        center_host, center_port = center.split(':')
        center_ip = socket.gethostbyname(center_host)
        center_port = int(center_port)
    except IndexError:
        logger.info("Usage:  dworker center_host:center_port")

    if nprocs > 1 and worker_port != 0:
        logger.error("Failed to launch worker.  You cannot use the --port argument when nprocs > 1.")
        exit(1)

    if nprocs > 1 and name:
        logger.error("Failed to launch worker.  You cannot use the --name argument when nprocs > 1.")
        exit(1)

    if not nthreads:
        nthreads = _ncores // nprocs

    services = {('http', http_port): HTTPWorker}

    loop = IOLoop.current()
    t = Worker if no_nanny else Nanny
    if host is not None:
        ip = socket.gethostbyname(host)
    else:
        # lookup the ip address of a local interface on a network that
        # reach the center
        ip = get_ip(center_ip, center_port)
    nannies = [t(center_ip, center_port, ncores=nthreads, ip=ip,
                 services=services, name=name, loop=loop,
                 worker_port=worker_port)
               for i in range(nprocs)]

    for nanny in nannies:
        nanny.start(nanny_port)
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


def go():
    check_python_3()
    main()

if __name__ == '__main__':
    go()
