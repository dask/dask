from __future__ import annotations

import logging
import sys
import warnings
from textwrap import dedent

import click

from distributed.deploy.old_ssh import SSHCluster

logger = logging.getLogger("distributed.dask_ssh")


@click.command(
    name="ssh",
    help=dedent(
        """Launch a Dask cluster over SSH. A 'dask scheduler' process will run on the
        first host specified in [HOSTNAMES] or in the hostfile, unless --scheduler is specified
        explicitly. One or more 'dask worker' processes will be run on each host. Use the flag
        --nworkers to adjust how many dask worker process are run on each host and the flag
        --nthreads to adjust how many CPUs are used by each dask worker process."""
    ),
)
@click.option(
    "--scheduler",
    default=None,
    type=str,
    help="Specify scheduler node.  Defaults to first address.",
)
@click.option(
    "--scheduler-port",
    default=8786,
    show_default=True,
    type=int,
    help="Specify scheduler port number.",
)
@click.option(
    "--nthreads",
    default=0,
    type=int,
    help=(
        "Number of threads per worker process. "
        "Defaults to number of cores divided by the number of "
        "processes per host."
    ),
)
@click.option(
    "--nworkers",
    "n_workers",  # This sets the Python argument name
    default=1,
    show_default=True,
    type=int,
    help="Number of worker processes per host.",
)
@click.argument("hostnames", nargs=-1, type=str)
@click.option(
    "--hostfile",
    default=None,
    type=click.Path(exists=True),
    help="Textfile with hostnames/IP addresses",
)
@click.option(
    "--ssh-username",
    default=None,
    type=str,
    help="Username to use when establishing SSH connections.",
)
@click.option(
    "--ssh-port",
    default=22,
    type=int,
    show_default=True,
    help="Port to use for SSH connections.",
)
@click.option(
    "--ssh-private-key",
    default=None,
    type=str,
    help="Private key file to use for SSH connections.",
)
@click.option("--nohost", is_flag=True, help="Do not pass the hostname to the worker.")
@click.option(
    "--log-directory",
    default=None,
    type=click.Path(exists=True),
    help=(
        "Directory to use on all cluster nodes for the output of "
        "dask scheduler and dask worker commands."
    ),
)
@click.option(
    "--local-directory",
    default=None,
    type=click.Path(exists=True),
    help=("Directory to use on all cluster nodes to place workers files."),
)
@click.option(
    "--remote-python", default=None, type=str, help="Path to Python on remote nodes."
)
@click.option(
    "--memory-limit",
    default="auto",
    show_default=True,
    help="Bytes of memory that the worker can use. "
    "This can be an integer (bytes), "
    "float (fraction of total system memory), "
    "string (like 5GB or 5000M), "
    "'auto', or zero for no memory management",
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
    "--remote-dask-worker",
    default="distributed.cli.dask_worker",
    show_default=True,
    type=str,
    help="Worker to run.",
)
@click.pass_context
@click.version_option()
def main(
    ctx,
    scheduler,
    scheduler_port,
    hostnames,
    hostfile,
    nthreads,
    n_workers,
    ssh_username,
    ssh_port,
    ssh_private_key,
    nohost,
    log_directory,
    remote_python,
    memory_limit,
    worker_port,
    nanny_port,
    remote_dask_worker,
    local_directory,
):
    if "dask-ssh" in sys.argv[0]:
        warnings.warn(
            "dask-ssh is deprecated and will be removed in a future release; use `dask ssh` instead",
            FutureWarning,
            stacklevel=1,
        )

    try:
        hostnames = list(hostnames)
        if hostfile:
            with open(hostfile) as f:
                hosts = f.read().split()
            hostnames.extend(hosts)

        if not scheduler:
            scheduler = hostnames[0]

    except IndexError:
        print(ctx.get_help())
        exit(1)

    c = SSHCluster(
        scheduler,
        scheduler_port,
        hostnames,
        nthreads,
        n_workers,
        ssh_username,
        ssh_port,
        ssh_private_key,
        nohost,
        log_directory,
        remote_python,
        memory_limit,
        worker_port,
        nanny_port,
        remote_dask_worker,
        local_directory,
    )

    import distributed

    print("\n---------------------------------------------------------------")
    print(
        "                 Dask.distributed v{version}\n".format(
            version=distributed.__version__
        )
    )
    print(f"Worker nodes: {len(hostnames)}")
    for i, host in enumerate(hostnames):
        print(f"  {i}: {host}")
    print(f"\nscheduler node: {scheduler}:{scheduler_port}")
    print("---------------------------------------------------------------\n\n")

    # Monitor the output of remote processes.  This blocks until the user issues a KeyboardInterrupt.
    c.monitor_remote_processes()

    # Close down the remote processes and exit.
    print("\n[ dask ssh ]: Shutting down remote processes (this may take a moment).")
    c.shutdown()
    print("[ dask ssh ]: Remote processes have been terminated. Exiting.")


if __name__ == "__main__":
    main()  # pragma: no cover
