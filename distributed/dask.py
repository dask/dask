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

def log(*args):
    return


@gen.coroutine
def worker(master_queue, worker_queue, ident, dsk, dependencies, stack,
           processing, ncores):
    """ Manage a single distributed worker node

    This coroutine manages one remote worker.  It spins up several
    ``worker_core`` coroutines, one for each core.  It reports a closed
    connection to master if one occurs.
    """
    try:
        yield [worker_core(master_queue, worker_queue, ident, i, dsk,
                           dependencies, stack, processing)
                for i in range(ncores)]
    except StreamClosedError:
        log("Worker failed from closed stream", ident)
        master_queue.put_nowait({'op': 'worker-failed',
                                 'worker': ident})


@gen.coroutine
def worker_core(master_queue, worker_queue, ident, i, dsk, dependencies, stack,
                processing):
    """ Manage one core on one distributed worker node

    This coroutine listens on worker_queue for the following operations

    Incoming Messages
    -----------------

    - compute-task:  call worker.compute(...) on remote node, report when done
    - close: close connection to worker node, report `worker-finished` to master

    Outgoing Messages
    -----------------

    - task-finished:  sent to master once a task completes
    - task-erred: sent to master when a task errs
    - worker-finished: sent to master in response to a close command

    Shared State
    ------------

    - stack::list should be populated by the master coroutine with tasks to
      run.  Every task added to task should be accompanied with one
      'compute-task' operation added to the worker_queue.  Stack is shared with
      other workers.
    - processing::set keeps track of what tasks are currently being run on the
      worker.  One processing set is shared by all worker-cores on the same
      worker.
    """
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
    master_queue.put_nowait({'op': 'worker-finished',
                             'worker': ident})
    log("Close worker core", ident, i)


@gen.coroutine
def delete(master_queue, delete_queue, ip, port):
    """ Delete extraneous intermediates from distributed memory

    This coroutine manages a connection to the center in order to send keys
    that should be removed from distributed memory.  We batch several keys that
    come in over the ``delete_queue`` into a list.  Roughly once a second it
    sends this list of keys over to the center which then handles deleting
    these keys from workers' memory.

    worker \                             /-> worker node
    worker -> master -> delete -> center --> worker node
    worker /                             \-> worker node

    Incoming Messages
    -----------------

    - delete-task: holds a key to be deleted
    - close: close this coroutine
    """
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
    >>> who_has = {'a': {'alice'}}

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, who_has, 'b')
    'alice'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice', 'bob'}}
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
    """ The master coroutine for dask scheduling

    This coroutine manages interactions with all worker cores and with the
    delete coroutine through shared state and queues.

    Parameters
    ----------
    master_queue: tornado.queues.Queue
    worker_queues: dict {worker: tornado.queues.Queue}
    delete_queue: tornado.queues.Queue
    who_has: dict {key: set}
        Mapping key to {set of worker-identities}
    has_what: dict {worker: set}
        Mapping worker-identity to {set of keys}
    workers: dict {worker: int}
        Mapping worker-identity to number-of-cores
    stacks: dict {worker: list}
        One dask.async style stack per worker node
    processing: dict {worker: set}
        One set of tasks currently in process on each worker
    released: set
        A set of all keys that have been computed, used, and released
    dsk, results: normal inputs to get

    Queues
    ------

    -   worker_queues: One queue per worker node.
        Each queue is listened to by several worker_core coroutiens.
    -   delete_queue: One queue listened to by ``delete`` which connects to the
        center to delete unnecessary intermediate data

    Shared State with Workers
    -------------------------

    -   stacks: a dictionary of lists of ready-to-run tasks, one for each worker
        e.g. {('192.168.0.1', 8788): ['key1', 'key2'],
              ('192.168.0.2', 8788): ['key3', 'key4']}
        Master populates stacks, worker_core depletes them
    -   processing: a dictionary of sets of running tasks, one for each worker.
        Worker_core coroutines manage processing mostly on their own.
    """
    dependencies, dependents = get_deps(dsk)
    state = heal(dependencies, dependents, set(who_has), stacks, processing)
    waiting = state['waiting']
    waiting_data = state['waiting_data']
    finished_results = state['finished_results']
    released.update(state['released'])

    @gen.coroutine
    def cleanup():
        """ Clean up queues and coroutines, prepare to stop """
        n = 0
        delete_queue.put_nowait({'op': 'close'}); n += 1
        for w, ncores in workers.items():
            for i in range(ncores):
                worker_queues[w].put_nowait({'op': 'close'}); n += 1

        for i in range(n):
            yield master_queue.get()

    def trigger_task(key):
        """ Send task to an appropriate worker """
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
    leaves = [k for k, deps in waiting.items() if not deps]
    keyorder = order(dsk)
    leaves = sorted(leaves, key=keyorder.get)

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

            state = heal(dependencies, dependents, set(who_has), stacks,
                         processing)
            waiting_data = state['waiting_data']
            waiting = state['waiting']
            released = state['released']
            finished_results = state['finished_results']
            trigger_keys = {k for k, v in waiting.items() if not v}
            for key in trigger_keys:
                trigger_task(key)
            for key in set(who_has).intersection(released):
                delete_queue.put_nowait({'op': 'delete-task', 'key': key})

        for w in workers:  # This is a kludge and should be removed
            while worker_queues[w].qsize() < len(stacks[w]):
                worker_queues[w].put_nowait({'op': 'compute-task'})

    yield cleanup()

    raise Return(results)


def validate_state(dependencies, dependents, waiting, waiting_data,
        in_memory, stacks, processing, finished_results, released, **kwargs):
    """ Validate a current runtime state

    See also:
        heal - fix a current runtime state
    """
    in_stacks = {k for v in stacks.values() for k in v}
    in_processing = {k for v in processing.values() for k in v}
    keys = {key for key in dependents if not dependents[key]}

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


def heal(dependencies, dependents, in_memory, stacks, processing, **kwargs):
    """ Make a runtime state consistent

    Sometimes we lose intermediate values.  In these cases it can be tricky to
    rewind our runtime state to a point where it can again proceed to
    completion.  This function edits runtime state in place to make it
    consistent.  It outputs a full state dict.

    This function gets run whenever something bad happens, like a worker
    failure.  It should be idempotent.
    """
    outputs = {key for key in dependents if not dependents[key]}

    rev_stacks = {k: v for k, v in reverse_dict(stacks).items() if v}
    rev_processing = {k: v for k, v in reverse_dict(processing).items() if v}

    waiting_data = defaultdict(set) # deepcopy(dependents)
    waiting = defaultdict(set) # deepcopy(dependencies)
    finished_results = set()

    def remove_from_stacks(key):
        if key in rev_stacks:
            for worker in rev_stacks.pop(key):
                stacks[worker].remove(key)

    def remove_from_processing(key):
        if key in rev_processing:
            for worker in rev_processing.pop(key):
                processing[worker].remove(key)

    new_released = set(dependents)

    accessible = set()

    @memoize
    def make_accessible(key):
        new_released.remove(key)
        if key in in_memory:
            return

        for dep in dependencies[key]:
            make_accessible(dep)  # recurse

        waiting[key] = {dep for dep in dependencies[key]
                            if dep not in in_memory}

    for key in outputs:
        make_accessible(key)

    waiting_data = {key: {dep for dep in dependents[key]
                              if dep not in new_released
                              and dep not in in_memory}
                    for key in dependents
                    if key not in new_released}

    def unrunnable(key):
        return (key in new_released
             or key in in_memory
             or waiting.get(key)
             or not all(dep in in_memory for dep in dependencies[key]))

    for key in list(filter(unrunnable, rev_stacks)):
        remove_from_stacks(key)

    for key in list(filter(unrunnable, rev_processing)):
        remove_from_processing(key)

    for key in list(rev_stacks) + list(rev_processing):
        if key in waiting:
            assert not waiting[key]
            del waiting[key]

    finished_results = {key for key in outputs if key in in_memory}

    output = {'keys': outputs,
             'dependencies': dependencies, 'dependents': dependents,
             'waiting': waiting, 'waiting_data': waiting_data,
             'in_memory': in_memory, 'processing': processing, 'stacks': stacks,
             'finished_results': finished_results, 'released': new_released}
    validate_state(**output)
    return output



@gen.coroutine
def _get2(ip, port, dsk, result, gather=False):
    """ Distributed dask scheduler

    This uses a distributed network of Center and Worker nodes.

    Parameters
    ----------
    ip/port:
        address of center
    dsk/result:
        normal graph/keys inputs to dask.get
    gather: bool
        Collect distributed results from cluster.  If False then return
        RemoteData objects.
    """
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    results = set(result_flat)

    loop = IOLoop.current()

    center = rpc(ip=ip, port=port)
    who_has, has_what, ncores = yield [center.who_has(),
                                       center.has_what(),
                                       center.ncores()]
    workers = sorted(ncores)

    dependencies, dependents = get_deps(dsk)

    worker_queues = {worker: Queue() for worker in workers}
    master_queue = Queue()
    delete_queue = Queue()

    stacks = {w: list() for w in workers}
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


def get(ip, port, dsk, keys, gather=True, _get=_get2):
    return IOLoop.current().run_sync(lambda: _get(ip, port, dsk, keys, gather))


def hashable(x):
    try:
        hash(x)
        return True
    except TypeError:
        return False
