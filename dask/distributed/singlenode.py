from .scheduler import Scheduler
from .worker import Worker
from .client import Client
from multiprocessing import Pool

from time import sleep
import psutil

ncpus = psutil.cpu_count()
pool = Pool()

def get(dsk, keys, nworkers=ncpus):
    """ Single node get function using ZeroMQ distributed scheduler

    This creates a small cluster of workers on the local machine, uses it to
    compute a result in parallel, then tears it down.

    See also:
        dask.multiprocessing
        dask.threaded
    """
    s = Scheduler(hostname='localhost')
    for p in pool._pool:
        pool.apply_async(Worker, (s.address_to_workers,), {'block': True})
    c = Client(s.address_to_clients)
    sleep(0.05)

    try:
        result = c.get(dsk, keys)
    finally:
        s.close_workers()
        s.close()

    return result
