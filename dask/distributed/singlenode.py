from .scheduler import Scheduler
from .worker import Worker
from .client import Client
from multiprocessing import Pool

from time import sleep
import psutil

ncpus = psutil.cpu_count()

def get(dsk, keys, nworkers=ncpus):
    """ Single node get function using ZeroMQ distributed scheduler

    This creates a small cluster of workers on the local machine, uses it to
    compute a result in parallel, then tears it down.

    See also:
        dask.multiprocessing
        dask.threaded
    """
    pool = Pool()
    s = Scheduler(hostname='localhost')
    futures = [pool.apply_async(Worker, (s.address_to_workers,), {'block': True})
               for p in pool._pool]
    c = Client(s.address_to_clients)
    while len(s.workers) < len(pool._pool):
        sleep(0.01)

    try:
        result = c.get(dsk, keys)
    finally:
        s.close_workers()
        s.close()

    return result
