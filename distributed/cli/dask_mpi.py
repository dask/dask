from functools import partial

import click
from mpi4py import MPI
from tornado.ioloop import IOLoop
from tornado import gen
from warnings import warn

from distributed import Scheduler, Nanny, Worker
from distributed.bokeh.worker import BokehWorker
from distributed.cli.utils import check_python_3, uri_from_host_port
from distributed.utils import get_ip_interface


comm = MPI.COMM_WORLD
rank = comm.Get_rank()
loop = IOLoop()


@click.command()
@click.option(
    "--scheduler-file",
    type=str,
    default="scheduler.json",
    help="Filename to JSON encoded scheduler information. ",
)
@click.option(
    "--interface", type=str, default=None, help="Network interface like 'eth0' or 'ib0'"
)
@click.option("--nthreads", type=int, default=0, help="Number of threads per worker.")
@click.option(
    "--memory-limit",
    default="auto",
    help="Number of bytes before spilling data to disk. "
    "This can be an integer (nbytes) "
    "float (fraction of total memory) "
    "or 'auto'",
)
@click.option(
    "--local-directory", default="", type=str, help="Directory to place worker files"
)
@click.option(
    "--scheduler/--no-scheduler",
    default=True,
    help=(
        "Whether or not to include a scheduler. "
        "Use --no-scheduler to increase an existing dask cluster"
    ),
)
@click.option(
    "--nanny/--no-nanny",
    default=True,
    help="Start workers in nanny process for management",
)
@click.option(
    "--bokeh-port", type=int, default=8787, help="Bokeh port for visual diagnostics"
)
@click.option(
    "--bokeh-worker-port",
    type=int,
    default=8789,
    help="Worker's Bokeh port for visual diagnostics",
)
@click.option("--bokeh-prefix", type=str, default=None, help="Prefix for the bokeh app")
def main(
    scheduler_file,
    interface,
    nthreads,
    local_directory,
    memory_limit,
    scheduler,
    bokeh_port,
    bokeh_prefix,
    nanny,
    bokeh_worker_port,
):
    if interface:
        host = get_ip_interface(interface)
    else:
        host = None

    if rank == 0 and scheduler:
        try:
            from distributed.bokeh.scheduler import BokehScheduler
        except ImportError:
            services = {}
        else:
            services = {
                ("bokeh", bokeh_port): partial(BokehScheduler, prefix=bokeh_prefix)
            }
        scheduler = Scheduler(
            scheduler_file=scheduler_file, loop=loop, services=services
        )
        addr = uri_from_host_port(host, None, 8786)
        scheduler.start(addr)
        try:
            loop.start()
            loop.close()
        finally:
            scheduler.stop()
    else:
        W = Nanny if nanny else Worker
        worker = W(
            scheduler_file=scheduler_file,
            loop=loop,
            name=rank if scheduler else None,
            ncores=nthreads,
            local_dir=local_directory,
            services={("bokeh", bokeh_worker_port): BokehWorker},
            memory_limit=memory_limit,
        )
        addr = uri_from_host_port(host, None, 0)

        @gen.coroutine
        def run():
            yield worker._start(addr)
            while worker.status != "closed":
                yield gen.sleep(0.2)

        try:
            loop.run_sync(run)
            loop.close()
        finally:
            pass

        @gen.coroutine
        def close():
            yield worker._close(timeout=2)

        loop.run_sync(close)


def go():
    check_python_3()
    warn(
        "The dask-mpi command line utility in the `distributed` "
        "package is deprecated.  "
        "Please install the `dask-mpi` package instead. "
        "More information is available at https://mpi.dask.org"
    )
    main()


if __name__ == "__main__":
    go()
