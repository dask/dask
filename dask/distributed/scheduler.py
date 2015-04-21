from ..async import start_state_from_dask as dag_state_from_dask
from ..async import nested_get, finish_task
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

    dealer = context.socket(zmq.DEALER)

    # TODO: don't shove in seed data if already in cache
    dag_state = dag_state_from_dask(dsk, cache=cache)

    worker_state = {'whereis': defaultdict(set),
                    'has': defaultdict(set)}

    tick = [0]

    if dag_state['waiting'] and not dag_state['ready']:
        raise ValueError("Found no accessible jobs in dask graph")

    available_workers = workers[:]

    def fire_task(worker):
        """ Fire off a task to the thread pool """
        # Update heartbeat
        tick[0] += 1
        # Choose a good task to compute
        key = dag_state['ready'].pop()
        dag_state['ready-set'].remove(key)
        dag_state['running'].add(key)

        task = dsk[key]
        deps = get_dependencies(dsk, key)
        data_locations = {worker_state['whereis'][dep] for dep in deps}

        # Submit
        payload = dict(function='compute',
                       args=(key, dsk[key], data_locations),
                       jobid=('compute', key))

        dealer.send_multipart([worker, '', dumps(payload)])

    # Seed initial tasks into the thread pool
    while dag_state['ready'] and len(dag_state['running']) < len(workers):
        fire_task(available_workers.pop())

    # Main loop, wait on tasks to finish, insert new ones
    while dag_state['waiting'] or dag_state['ready'] or dag_state['running']:
        address, empty, payload = dealer.recv_multipart()
        payload2 = loads(payload)

        if isinstance(payload['status'], Exception):
            raise payload['status']
        if payload['status'] != 'OK':
            raise Exception("Bad status in remote worker:\n\t"
                            + payload['status'])

        if payload['jobid'][0] == 'compute':
            key = payload['result']['key']
            dag_finish_task(dsk, key, dag_state, results, delete=False)
            # TODO: issue delitem calls to workers when freeing data
            worker_finish_task(dsk, address, key, worker_state)
            available_workers.append(address)

        while dag_state['ready'] and len(dag_state['running']) < len(workers):
            fire_task(available_workers.pop())

    return nested_get(result, dag_state['cache'])


def gather(dealer, workers, keys):
    dealer = context.socket(zmq.DEALER)




def worker_finish_task(dask, worker, key, worker_state):
    """
    Manage worker state when task is complete

    >>> worker_state = {'whereis': {'a': {'tcp://alice'}, 'b': {'tcp://bob'}},
    ...                 'has': {'tcp://alice': {'a'}, 'tcp://bob': {'b'}}}
    >>> dsk = {'a': 1, 'b': 2, 'c': (add, 'a', 'b')}

    >>> worker_finish_task(dsk, 'tcp://alice', 'c', worker_state)
    >>> worker_state
    {'whereis': {'a': {'tcp://alice'}, 'b': {'tcp://bob', 'tcp://alice'},
                 'c': {'tcp://alice'}},
     'has': {'tcp://alice': {'a', 'b', 'c'}, 'tcp://bob': {'b'}}}
    """
    deps = get_dependencies(dask, key)

    for k in [key] + list(deps):
        worker_state['whereis'][k].add(worker)
        worker_state['has'][worker].add(k)
