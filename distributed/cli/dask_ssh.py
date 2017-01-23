from __future__ import print_function, division, absolute_import

from distributed.deploy.ssh import SSHCluster
import click
import os

from distributed.cli.utils import check_python_3


@click.command(help = """Launch a distributed cluster over SSH. A 'dask-scheduler' process will run on the
                         first host specified in [HOSTNAMES] or in the hostfile (unless --scheduler is specified
                         explicitly). One or more 'dask-worker' processes will be run each host in [HOSTNAMES] or
                         in the hostfile. Use command line flags to adjust how many dask-worker process are run on
                         each host (--nprocs) and how many cpus are used by each dask-worker process (--nthreads).""")
@click.option('--scheduler', default=None, type=str,
              help="Specify scheduler node.  Defaults to first address.")
@click.option('--scheduler-port', default=8786, type=int,
              help="Specify scheduler port number.  Defaults to port 8786.")
@click.option('--nthreads', default=0, type=int,
              help="Number of threads per worker process. Defaults to number of cores divided by the number of processes per host.")
@click.option('--nprocs', default=1, type=int,
              help="Number of worker processes per host.  Defaults to one.")
@click.argument('hostnames', nargs=-1, type=str)
@click.option('--hostfile', default=None, type=click.Path(exists=True),
              help="Textfile with hostnames/IP addresses")
@click.option('--ssh-username', default=None, type=str,
              help="Username to use when establishing SSH connections.")
@click.option('--ssh-port', default=22, type=int,
              help="Port to use for SSH connections.")
@click.option('--ssh-private-key', default=None, type=str,
              help="Private key file to use for SSH connections.")
@click.option('--nohost', is_flag=True,
              help="Do not pass the hostname to the worker.")
@click.option('--log-directory', default=None, type=click.Path(exists=True),
              help="Directory to use on all cluster nodes for the output of dask-scheduler and dask-worker commands.")
@click.pass_context
def main(ctx, scheduler, scheduler_port, hostnames, hostfile, nthreads, nprocs,
          ssh_username, ssh_port, ssh_private_key, nohost, log_directory):
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

    c = SSHCluster(scheduler, scheduler_port, hostnames, nthreads, nprocs,
                ssh_username, ssh_port, ssh_private_key, nohost, log_directory)

    import distributed
    print('\n---------------------------------------------------------------')
    print('                 Dask.distributed v{version}\n'.format(version=distributed.__version__))
    print('Worker nodes:'.format(n=len(hostnames)))
    for i, host in enumerate(hostnames):
        print('  {num}: {host}'.format(num = i, host = host))
    print('\nscheduler node: {addr}:{port}'.format(addr=scheduler, port=scheduler_port))
    print('---------------------------------------------------------------\n\n')

    # Monitor the output of remote processes.  This blocks until the user issues a KeyboardInterrupt.
    c.monitor_remote_processes()

    # Close down the remote processes and exit.
    print("\n[ dask-ssh ]: Shutting down remote processes (this may take a moment).")
    c.shutdown()
    print("[ dask-ssh ]: Remote processes have been terminated. Exiting.")


def go():
    check_python_3()
    main()

if __name__ == '__main__':
    start()
