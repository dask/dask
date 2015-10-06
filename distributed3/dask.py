from __future__ import print_function, division, absolute_import

from collections import defaultdict
from math import ceil
from time import time

from toolz import merge, frequencies, memoize
from tornado import gen
from tornado.gen import Return
from tornado.concurrent import Future
from tornado.locks import Event
from tornado.queues import Queue
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from dask.async import nested_get, _execute_task
from dask.core import istask, flatten, get_deps, reverse_dict
from dask.order import order

from .core import connect, rpc
from .client import RemoteData, keys_to_data, gather_from_center


log = print


@gen.coroutine
def worker(master_queue, worker_queue, ident, dsk, dependencies, stack,
           processing, ncores):
    try:
        yield [worker_core(master_queue, worker_queue, ident, i, dsk,
                           dependencies, stack, processing)
                for i in range(ncores)]
    except StreamClosedError:
        log("Worker failed from closed stream", ident)
        master_queue.put_nowait({'op': 'worker-failed',
                                 'worker': ident})
    master_queue.put_nowait({'op': 'worker-finished',
                             'worker': ident})


@gen.coroutine
def worker_core(master_queue, worker_queue, ident, i, dsk, dependencies, stack,
                processing):
    worker = rpc(ip=ident[0], port=ident[1])
    log("Start worker core", ident, i)

    while True:
        msg = yield worker_queue.get()
        if msg['op'] == 'close':
            break
        assert msg['op'] == 'compute-task'

        if not stack:
            continue
        key = stack.pop()
        processing.add(key)
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
        if response == b'error':
            err = yield worker.get_data(keys=[key])
            master_queue.put_nowait({'op': 'task-erred',
                                     'key': key,
                                     'worker': ident,
                                     'exception': err[key]})
        else:
            master_queue.put_nowait({'op': 'task-finished',
                                     'worker': ident,
                                     'key': key})
        processing.remove(key)

    yield worker.close(close=True)
    worker.close_streams()
    log("Close worker core", ident, i)


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
    log('Delete finished')


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
    if not workers:
        workers = stacks
    worker = min(workers, key=lambda w: len(stacks[w]))
    return worker


@gen.coroutine
def master(master_queue, worker_queues, delete_queue, who_has, has_what,
           workers, stacks, processing, released, dsk, results):
    dependencies, dependents = get_deps(dsk)
    waiting = {k: v.copy() for k, v in dependencies.items()}
    waiting_data = {k: v.copy() for k, v in dependents.items()}
    leaves = [k for k in dsk if not dependencies[k]]
    keyorder = order(dsk)
    leaves = sorted(leaves, key=keyorder.get)

    finished_results = set()

    @gen.coroutine
    def cleanup():
        n = 0
        delete_queue.put_nowait({'op': 'close'}); n += 1
        for w, ncores in workers.items():
            for i in range(ncores):
                worker_queues[w].put_nowait({'op': 'close'}); n += 1

        for i in range(n):
            yield master_queue.get()

    def trigger_task(key):
        if key in waiting:
            del waiting[key]
        new_worker = decide_worker(dependencies, stacks, who_has, key)
        stacks[new_worker].append(key)
        worker_queues[new_worker].put_nowait({'op': 'compute-task'})

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
            log("task finished", key, worker)
            key = msg['key']
            worker = msg['worker']
            who_has[key].add(worker)
            has_what[worker].add(key)

            for dep in sorted(dependents[key], key=keyorder.get, reverse=True):
                s = waiting[dep]
                s.remove(key)
                if not s:  # new task ready to run
                    trigger_task(dep)

            for dep in dependencies[key]:
                assert dep in waiting_data
                s = waiting_data[dep]
                s.remove(key)
                if not s and dep not in results:
                    delete_queue.put_nowait({'op': 'delete-task',
                                             'key': dep})
                    released.add(dep)
                    for w in who_has[dep]:
                        has_what[w].remove(dep)
                    del who_has[dep]

            if key in results:
                finished_results.add(key)
                if len(finished_results) == len(results):
                    break

        elif msg['op'] == 'task-erred':
            yield cleanup()
            raise msg['exception']

        elif msg['op'] == 'worker-failed':
            worker = msg['worker']
            keys = has_what.pop(worker)
            del worker_queues[worker]
            del workers[worker]
            del stacks[worker]
            del processing[worker]
            missing_keys = set()
            for key in keys:
                who_has[key].remove(worker)
                if not who_has[key]:
                    missing_keys.add(key)
            gone_data = {k for k, v in who_has.items() if not v}
            for k in gone_data:
                del who_has[k]

            state = heal(dsk, dependencies, dependents, set(who_has), stacks,
                         processing, released)
            waiting_data = state['waiting_data']
            waiting = state['waiting']
            released = state['released']
            finished_results = state['finished_results']
            trigger_keys = {k for k, v in waiting.items() if not v}
            for key in trigger_keys:
                trigger_task(key)

        for w in workers:  # This is a kludge and should be removed
            while worker_queues[w].qsize() < len(stacks[w]):
                worker_queues[w].put_nowait({'op': 'compute-task'})

    yield cleanup()

    raise Return(results)


def validate_state(dsk, dependencies, dependents, waiting, waiting_data,
        in_memory, stacks, processing, finished_results, released, **kwargs):
    in_stacks = {k for v in stacks.values() for k in v}
    in_processing = {k for v in processing.values() for k in v}
    keys = {key for key in dsk if not dependents[key]}

    @memoize
    def check_key(key):
        """ Validate a single key, recurse downards """
        assert sum([key in waiting,
                    key in in_stacks,
                    key in in_processing,
                    key in in_memory,
                    key in released]) == 1

        if not all(map(check_key, dependencies[key])):# Recursive case
            assert False

        if key in in_memory:
            assert not any(key in waiting.get(dep, ())
                           for dep in dependents[key])
            assert not waiting.get(key)

        if key in in_stacks or key in in_processing:
            assert all(dep in in_memory for dep in dependencies[key])
            assert not waiting.get(key)

        if key in finished_results:
            assert key in in_memory
            assert key in keys

        if key in keys and key in in_memory:
            assert key in finished_results

        return True

    assert all(map(check_key, keys))


def heal(dsk, dependencies, dependents, in_memory, stacks, processing,
        released, **kwargs):
    """ Make a runtime state consistent

    Sometimes we lose intermediate values.  In these cases it can be tricky to
    rewind our runtime state to a point where it can again proceed to
    completion.  This function edits runtime state in place to make it
    consistent.  It outputs a full state dict.
    """
    keys = {key for key in dsk if not dependents[key]}

    rev_stacks = reverse_dict(stacks)
    rev_processing = reverse_dict(processing)

    waiting_data = defaultdict(set) # deepcopy(dependents)
    waiting = defaultdict(set) # deepcopy(dependencies)
    finished_results = set()

    released = set(dsk)

    @memoize
    def make_accessible(key):
        released.remove(key)

        if key in in_memory:
            if key in keys:
                finished_results.add(key)
            return

        # Remove in-flight tasks
        if (key in rev_stacks and
            any(dep not in in_memory for dep in dependencies[key])):
            for worker in rev_stacks[key]:
                stacks[worker].remove(key)
        if (key in rev_processing and
            any(dep not in in_memory for dep in dependencies[key])):
            for worker in rev_processing[key]:
                processing[worker].remove(key)

        # Recurse
        for dep in dependencies[key]:
            make_accessible(dep)

        waiting[key] = {dep for dep in dependencies[key]
                            if dep not in in_memory}

        for dep in dependencies[key]:
            waiting_data[dep].add(key)

        if key in finished_results:
            finished_results.remove(key)

    for key in keys:
        make_accessible(key)

    for seq in list(stacks.values()) + list(processing.values()):
        for key in seq:
            assert not waiting[key]
            del waiting[key]

    output = {'dsk': dsk, 'keys': keys,
             'dependencies': dependencies, 'dependents': dependents,
             'waiting': waiting, 'waiting_data': waiting_data,
             'in_memory': in_memory, 'processing': processing, 'stacks': stacks,
             'finished_results': finished_results, 'released': released}
    validate_state(**output)
    return output



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
    processing = {w: set() for w in workers}
    released = set()

    coroutines = ([master(master_queue, worker_queues, delete_queue,
                          who_has, has_what, ncores,
                          stacks, processing, released, dsk, results)]
                + [worker(master_queue, worker_queues[w], w, dsk, dependencies,
                          stacks[w], processing[w], ncores[w])
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
                if response == b'error':
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
