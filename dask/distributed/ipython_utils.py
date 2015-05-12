from .scheduler import Scheduler
from .worker import Worker
from .client import Client


def dask_client_from_ipclient(client):
    """
    Construct a scheduler from an ipython client.
    """
    zmq_context = client._context
    scheduler = Scheduler(context=zmq_context)
    workers = [Worker(scheduler.address_to_workers) for i in range(len(client))]
    dask_client = Client(scheduler.address_to_clients)
    return dask_client
