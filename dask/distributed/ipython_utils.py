from .scheduler import Scheduler
from .worker import Worker
from .client import Client


def dask_client_from_ipclient(client):
    """
    Construct a scheduler from an ipython client.

    1. Run a command on the first engine to start a scheduler. This should
       return an address for clients and an address for workers.
    2. Run some start worker command on the rest of the engines, giving them
       the scheduler_to_engines address.
    3. Start a client here with the scheduler to client address.
    """

    view = client[:]
    def start_scheduler():
        from dask.distributed import Scheduler
        scheduler = Scheduler()
        return scheduler.port_to_client, scheduler.port_to_workers

    def start_worker(scheduler_port):
        from dask.distributed import Worker
        worker = Worker(scheduler_port)

    # start sched
    # TODO which view method to use apply sync or apply asyc? Depends on if I
    # want to choose my addresses ahead of time.

    # start workers
    # TODO

    # start client
    dask_client = Client(scheduler.address_to_clients)
    return dask_client
