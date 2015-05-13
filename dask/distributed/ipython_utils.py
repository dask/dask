from .client import Client


def dask_client_from_ipclient(ipclient):
    """
    Construct a scheduler from an ipython client.

    1. Run a command on the first engine to start a scheduler. This should
       return an address for clients and an address for workers.
    2. Run some start worker command on the rest of the engines, giving them
       the scheduler_to_engines address.
    3. Start a client here with the scheduler to client address.
    """

    def start_scheduler():
        from dask.distributed import Scheduler
        scheduler = Scheduler()
        return scheduler.address_to_clients, scheduler.address_to_workers

    def start_worker(scheduler_address):
        from dask.distributed import Worker
        worker = Worker(scheduler_address)

    # start scheduler
    scheduler_target = ipclient[ipclient.ids[0]]
    workers_targets = ipclient[1:]
    to_clients, to_workers = scheduler_target.apply_sync(start_scheduler)

    # start workers
    workers_targets = workers_targets.apply_sync(start_worker, to_workers)

    # start client
    dask_client = Client(to_clients)
    return dask_client
