from ..async import start_state_from_dask, nested_get, finish_task
from ..core import flatten
from ..compatibility import Queue
from multiprocessing.pool import ThreadPool
from threading import Thread, Lock
import zmq
from zmqompute.core import loads, dumps

context = zmq.Context()


def get_distributed(workers, cache, dsk, result, **kwargs):
    """ Distributed get function

    Parameters
    ----------

    workers: list
        List of zmq uris like tcp://hostname:port
    cache: dict-like
        Temporary storage of results, possibly a pallet.Warehouse
    dsk: dict
        A dask dictionary specifying a workflow
    result: key or list of keys
        Keys corresponding to desired data
    debug_counts: integer or None
        This integer tells how often the scheduler should dump debugging info

    See Also
    --------

    threaded.get
    """
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    results = set(result_flat)

    queue = Queue()

    sockets = dict()
    for worker in workers:
        socket = context.socket(zmq.REQ)
        socket.connect(worker)
        sockets[worker] = socket

    pool = ThreadPool(len(sockets))

    # TODO: don't shove in seed data if already in cache
    state = start_state_from_dask(dsk, cache=cache)

    tick = [0]

    if state['waiting'] and not state['ready']:
        raise ValueError("Found no accessible jobs in dask")

    available_workers = workers[:]

    def interact(socket, message):
        socket.send(dumps(message))
        receipt = loads(socket.recv())
        queue.put(receipt)
        available_workers.append(message['worker'])

    def fire_task(worker):
        """ Fire off a task to the thread pool """
        # Update heartbeat
        tick[0] += 1
        # Choose a good task to compute
        key = state['ready'].pop()
        state['ready-set'].remove(key)
        state['running'].add(key)

        # Submit
        socket = sockets[worker]
        pool.apply_async(interact, args=[socket, ('compute', key, dsk[key])])

    # Seed initial tasks into the thread pool
    while state['ready'] and len(state['running']) < len(workers):
        fire_task(available_workers.pop())

    # Main loop, wait on tasks to finish, insert new ones
    while state['waiting'] or state['ready'] or state['running']:
        message = queue.get()
        if isinstance(message['status'], Exception):
            raise Exception("Exception in remote process\n\n" +
                            message['status'])
        finish_task(dsk, message['key'], state, results, delete=True)
        while state['ready'] and len(state['running']) < len(workers):
            fire_task(available_workers.pop())

    # Final reporting
    while state['running'] or not queue.empty():
        message = queue.get()

    return nested_get(result, state['cache'])
