from __future__ import print_function, division, absolute_import

from collections import defaultdict
from math import ceil
from time import time

from toolz import merge, frequencies
from tornado import gen
from tornado.gen import Return
from tornado.concurrent import Future
from tornado.locks import Event
from tornado.queues import Queue
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from dask.async import nested_get, _execute_task
from dask.core import istask, flatten, get_deps
from dask.order import order

from .core import connect, rpc
from .client import RemoteData, keys_to_data, gather_from_center


log = print


@gen.coroutine
def worker(master_queue, worker_queue, ident, dsk, dependencies, stack, ncores):
    try:
        yield [worker_core(master_queue, worker_queue, ident, dsk, dependencies, stack)
                for i in range(ncores)]
    except StreamClosedError:
        master_queue.put_nowait({'op': 'worker-failed',
                                 'worker': ident})
    master_queue.put_nowait({'op': 'worker-finished',
                             'worker': ident})


@gen.coroutine
def worker_core(master_queue, worker_queue, ident, dsk, dependencies, stack):
    worker = rpc(ip=ident[0], port=ident[1])

    while True:
        msg = yield worker_queue.get()
        if msg['op'] == 'close':
            break
        assert msg['op'] == 'compute-task'

        key = stack.pop()
        task = dsk[key]
        if not istask(task):
            response = yield worker.update_data(data={key: task})
            assert response == b'OK', response
        else:
            needed = dependencies[key]
            response = yield worker.compute(function=_execute_task,
                                            args=(task, {}),
                                            needed=needed,
                                            key=key,
                                            kwargs={})
        if response == 'error':
            err = yield worker.get_data(keys=[key])
            master_queue.put_nowait({'op': 'task-erred',
                                     'key': key,
                                     'worker': ident,
                                     'exception': err})
        else:
            master_queue.put_nowait({'op': 'task-finished',
                                     'worker': ident,
                                     'key': key})

    yield worker.close(close=True)
    worker.close_streams()


@gen.coroutine
def delete(master_queue, delete_queue, ip, port):
    """ Delete extraneous intermediates from distributed memory """
    batch = list()
    last = time()
    center = rpc(ip=ip, port=port)

    while True:
        msg = yield delete_queue.get()
        if msg['op'] == 'close':
            break

        batch.append(msg['key'])
        if batch and time() - last > 1:       # One second batching
            last = time()
            yield center.delete_data(keys=batch)
            batch = list()

    if batch:
        yield center.delete_data(keys=batch)

    yield center.close(close=True)
    center.close_streams()          # All done
    master_queue.put_nowait({'op': 'delete-finished'})


def decide_worker(dependencies, stacks, who_has, key):
    """ Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> stacks = {'alice': ['z'], 'bob': []}
    >>> who_has = {'alice': ['a'], 'bob': []}

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, who_has, 'b')
    'alice'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'alice': ['a'], 'bob': []}
    >>> decide_worker(dependencies, stacks, who_has, 'b')
    'bob'
    """
    deps = dependencies[key]
    # TODO: look at args for RemoteData
    workers = frequencies(w for dep in deps
                            for w in who_has[dep])
    worker = min(workers, key=lambda w: len(stacks[w]))
    return worker


@gen.coroutine
def master(master_queue, worker_queues, delete_queue, who_has, has_what,
           workers, stacks, dsk, results):
    dependencies, dependents = get_deps(dsk)
    waiting = {k: v.copy() for k, v in dependencies.items()}
    waiting_data = {k: v.copy() for k, v in dependents.items()}
    leaves = [k for k in dsk if not dependencies[k]]
    keyorder = order(dsk)
    leaves = sorted(leaves, key=keyorder.get)

    finished_results = set()

    """
    Distribute leaves among workers

    We distribute leaf tasks (tasks with no dependencies) among workers
    uniformly.
    """
    k = int(ceil(len(leaves) / len(workers)))
    for i, worker in enumerate(workers):
        keys = leaves[i*k: (i + 1)*k][::-1]
        stacks[worker].extend(keys)
        for key in keys:
            worker_queues[worker].put_nowait({'op': 'compute-task'})

    while True:
        msg = yield master_queue.get()
        if msg['op'] == 'task-finished':
            key = msg['key']
            worker = msg['worker']
            who_has[key].add(worker)
            has_what[worker].add(key)

            for dep in sorted(dependents[key], key=keyorder.get, reverse=True):
                s = waiting[dep]
                s.remove(key)
                if not s:  # new task ready to run
                    del waiting[dep]
                    new_worker = decide_worker(dependencies, stacks, who_has, dep)
                    stacks[new_worker].append(dep)
                    worker_queues[new_worker].put_nowait({'op': 'compute-task'})

            for dep in dependencies[key]:
                assert dep in waiting_data
                s = waiting_data[dep]
                s.remove(key)
                if not s and dep not in results:
                    delete_queue.put_nowait({'op': 'delete-task',
                                             'key': dep})
                    for w in who_has[dep]:
                        has_what[w].remove(dep)
                    del who_has[dep]

            if key in results:
                finished_results.add(key)
                if len(finished_results) == len(results):
                    break

        elif msg['op'] == 'task-erred':
            raise NotImplementedError()
        elif msg['op'] == 'worker-failed':
            raise NotImplementedError()

    delete_queue.put_nowait({'op': 'close'})
    for w, ncores in workers.items():
        for i in range(ncores):
            worker_queues[w].put_nowait({'op': 'close'})

    raise Return(results)


@gen.coroutine
def _get2(ip, port, dsk, result, gather=False):
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    results = set(result_flat)

    loop = IOLoop.current()

    center = rpc(ip=ip, port=port)
    who_has = yield center.who_has()
    has_what = yield center.has_what()
    ncores = yield center.ncores()
    available_cores = ncores

    workers = sorted(ncores)

    dependencies, dependents = get_deps(dsk)

    worker_queues = {worker: Queue() for worker in workers}
    master_queue = Queue()
    delete_queue = Queue()

    stacks = {w: [] for w in workers}

    coroutines = ([master(master_queue, worker_queues, delete_queue,
                          who_has, has_what, ncores,
                          stacks, dsk, results)]
                + [worker(master_queue, worker_queues[w], w, dsk, dependencies,
                          stacks[w], ncores[w])
                   for w in workers]
                + [delete(master_queue, delete_queue, ip, port)])

    yield coroutines

    remote = {key: RemoteData(key, ip, port) for key in results}

    if gather:
        remote = yield gather_from_center((ip, port), remote)

    raise Return(nested_get(result, remote))


@gen.coroutine
def _get(ip, port, dsk, result, gather=False):

    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    results = set(result_flat)

    loop = IOLoop.current()

    center = rpc(ip=ip, port=port)
    who_has = yield center.who_has()
    has_what = yield center.has_what()
    ncores = yield center.ncores()
    available_cores = ncores

    workers = sorted(ncores)

    dependencies, dependents = get_deps(dsk)
    keyorder = order(dsk)
    leaves = [k for k in dsk if not dependencies[k]]
    leaves = sorted(leaves, key=keyorder.get)

    stacks = {w: [] for w in workers}
    idling = defaultdict(list)

    completed = {key: Event() for key in dsk}

    errors = list()

    finished = [False]
    finished_event = Event()
    worker_done = defaultdict(list)
    center_done = Event()
    delete_done = Event()

    """
    Distribute leaves among workers

    We distribute leaf tasks (tasks with no dependencies) among workers
    uniformly.  In the future we should use ordering from dask.order.
    """
    k = int(ceil(len(leaves) / len(workers)))
    for i, worker in enumerate(workers):
        stacks[worker].extend(leaves[i*k: (i + 1)*k][::-1])

    @gen.coroutine
    def add_key_to_stack(key):
        """ Add key to a worker's stack once key becomes available

        We choose which stack to add the key/task based on

        1.  Whether or not that worker has *a* data dependency of this task
        2.  How short that worker's stack is
        """
        deps = dependencies[key]
        yield [completed[dep].wait() for dep in deps]  # wait until dependencies finish

        # TODO: look at args for RemoteData
        workers = frequencies(w for dep in deps
                                for w in who_has[dep])
        worker = min(workers, key=lambda w: len(stacks[w]))
        stacks[worker].append(key)

        if idling.get(worker):
            idling[worker].pop().set()

    for key in dsk:
        if dependencies[key]:
            loop.spawn_callback(add_key_to_stack, key)

    @gen.coroutine
    def delete_intermediates():
        """ Delete extraneous intermediates from distributed memory

        This fires off a coroutine for every intermediate key, waiting on the
        events in ``completed`` for all dependent tasks to finish.
        Once the dependent tasks finish we add the key to an internal list
        ``delete_keys``.  We send this list of keys-to-be-deleted to the center
        for deletion.  We batch communications with the center to once a second
        """
        delete_keys = list()

        @gen.coroutine
        def delete_intermediate(key):
            """ Wait on appropriate events for a single key """
            deps = dependents[key]
            yield [completed[dep].wait() for dep in deps]
            raise Return(key)

        intermediates = [key for key in dsk
                             if key not in results]
        for key in intermediates:
            loop.spawn_callback(delete_intermediate, key)
        wait_iterator = gen.WaitIterator(*[delete_intermediate(key)
                                            for key in intermediates])
        n = len(intermediates)
        k = 0

        @gen.coroutine
        def clear_queue():
            if delete_keys:
                keys = delete_keys[:]   # make a copy
                del delete_keys[:]      # clear out old list
                yield center.delete_data(keys=keys)

        last = time()
        while not wait_iterator.done():
            if errors: break
            key = yield wait_iterator.next()
            delete_keys.append(key)
            if time() - last > 1:       # One second batching
                last = time()
                yield clear_queue()
        yield clear_queue()

        yield center.close(close=True)
        center.close_streams()          # All done
        center_done.set()

    loop.spawn_callback(delete_intermediates)

    @gen.coroutine
    def handle_worker(ident):
        """ Handle all communication with a single worker

        We pull tasks from a list in ``stacks[ident]`` and process each task in
        turn.  If this list goes empty we wait on an event in ``idling[ident]``
        which should be triggered to wake us back up again.

        ident :: (ip, port)
        """
        done_event = Event()
        worker_done[ident].append(done_event)
        stack = stacks[ident]
        worker = rpc(ip=ident[0], port=ident[1])

        while True:
            if not stack:
                event = Event()
                idling[ident].append(event)
                yield event.wait()

            if finished[0]:
                break

            key = stack.pop()
            task = dsk[key]
            if not istask(task):
                response = yield worker.update_data(data={key: task})
                assert response == b'OK', response
            else:
                needed = dependencies[key]
                response = yield worker.compute(function=_execute_task,
                                                args=(task, {}),
                                                needed=needed,
                                                key=key,
                                                kwargs={})
                if response == 'error':
                    finished[0] = True
                    err = yield worker.get_data(keys=[key])
                    errors.append(err[key])
                    for key in results:
                        completed[key].set()
                    break

            completed[key].set()
            who_has[key].add(ident)
            has_what[ident].add(key)

        yield worker.close(close=True)
        worker.close_streams()
        done_event.set()

    for worker in workers:
        for i in range(ncores[worker]):
            loop.spawn_callback(handle_worker, worker)

    yield [completed[key].wait() for key in results]
    finished[0] = True
    for events in idling.values():
        for event in events:
            event.set()

    remote = {key: RemoteData(key, ip, port) for key in results}

    yield [center_done.wait()] + [e.wait() for L in worker_done.values()
                                           for e in L]

    if errors:
        raise errors[0]

    if gather:
        remote = yield gather_from_center((ip, port), remote)

    raise Return(nested_get(result, remote))


def get(ip, port, dsk, keys, gather=False):
    return IOLoop.current().run_sync(lambda: _get(ip, port, dsk, keys, gather))


def hashable(x):
    try:
        hash(x)
        return True
    except TypeError:
        return False
