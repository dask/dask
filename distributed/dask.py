from __future__ import print_function, division, absolute_import

from collections import defaultdict
from math import ceil
from time import time

from toolz import merge, frequencies, memoize, concat
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
from .client import RemoteData, _gather, unpack_remotedata
from .utils import All


log = print


def log(*args):
    return


@gen.coroutine
def worker(scheduler_queue, worker_queue, ident, ncores):
    """ Manage a single distributed worker node

    This coroutine manages one remote worker.  It spins up several
    ``worker_core`` coroutines, one for each core.  It reports a closed
    connection to scheduler if one occurs.
    """
    try:
        yield All([worker_core(scheduler_queue, worker_queue, ident, i)
                for i in range(ncores)])
    except StreamClosedError:
        log("Worker failed from closed stream", ident)
        scheduler_queue.put_nowait({'op': 'worker-failed',
                                    'worker': ident})


@gen.coroutine
def worker_core(scheduler_queue, worker_queue, ident, i):
    """ Manage one core on one distributed worker node

    This coroutine listens on worker_queue for the following operations

    Incoming Messages
    -----------------

    - compute-task:  call worker.compute(...) on remote node, report when done
    - close: close connection to worker node, report `worker-finished` to
      scheduler

    Outgoing Messages
    -----------------

    - task-finished:  sent to scheduler once a task completes
    - task-erred: sent to scheduler when a task errs
    - worker-finished: sent to scheduler in response to a close command
    """
    worker = rpc(ip=ident[0], port=ident[1])
    log("Start worker core", ident, i)

    while True:
        msg = yield worker_queue.get()
        if msg['op'] == 'close':
            break
        if msg['op'] == 'compute-task':
            key = msg['key']
            needed = msg['needed']
            task = msg['task']
            if not istask(task):
                response = yield worker.update_data(data={key: task})
                assert response == b'OK', response
            else:
                response = yield worker.compute(function=_execute_task,
                                                args=(task, {}),
                                                needed=needed,
                                                key=key,
                                                kwargs={})
            if response == b'error':
                err = yield worker.get_data(keys=[key])
                scheduler_queue.put_nowait({'op': 'task-erred',
                                            'key': key,
                                            'worker': ident,
                                            'exception': err[key]})
            else:
                scheduler_queue.put_nowait({'op': 'task-finished',
                                            'worker': ident,
                                            'key': key})

    yield worker.close(close=True)
    worker.close_streams()
    scheduler_queue.put_nowait({'op': 'worker-finished',
                                'worker': ident})
    log("Close worker core", ident, i)


@gen.coroutine
def delete(scheduler_queue, delete_queue, ip, port):
    """ Delete extraneous intermediates from distributed memory

    This coroutine manages a connection to the center in order to send keys
    that should be removed from distributed memory.  We batch several keys that
    come in over the ``delete_queue`` into a list.  Roughly once a second it
    sends this list of keys over to the center which then handles deleting
    these keys from workers' memory.

    worker \                                /-> worker node
    worker -> scheduler -> delete -> center --> worker node
    worker /                                \-> worker node

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
    scheduler_queue.put_nowait({'op': 'delete-finished'})
    log('Delete finished')


@gen.coroutine
def interact(interact_queue, scheduler_queue, who_has, dsk, result):
    """ Interact with outside world

    For a normal get function this coroutine is almost non-essential.
    It just starts and stops the scheduler coroutine.
    """
    if isinstance(result, list):
        result_flat = set(flatten(result))
    else:
        result_flat = set([result])
    out_keys = set(result_flat)

    scheduler_queue.put_nowait({'op': 'update-graph',
                                'dsk': dsk,
                                'keys': out_keys})

    finished_results = {k for k in out_keys if k in who_has}

    while finished_results != out_keys:
        msg = yield interact_queue.get()
        if msg['op'] == 'task-finished':
            if msg['key'] in out_keys:
                finished_results.add(msg['key'])
        if msg['op'] == 'lost-data':
            if msg['key'] in finished_results:
                finished_results.remove(msg['key'])
        if msg['op'] == 'task-erred':
            scheduler_queue.put_nowait({'op': 'close'})
            raise msg['exception']
    scheduler_queue.put_nowait({'op': 'close'})

    raise Return(out_keys)


@gen.coroutine
def scheduler(scheduler_queue, interact_queue, worker_queues, delete_queue,
              who_has, has_what, ncores, dsk=None):
    """ The scheduler coroutine for dask scheduling

    This coroutine manages interactions with all worker cores and with the
    delete coroutine through queues.

    Parameters
    ----------
    scheduler_queue: tornado.queues.Queue
    interact_queue: tornado.queues.Queue
    worker_queues: dict {worker: tornado.queues.Queue}
    delete_queue: tornado.queues.Queue
    who_has: dict {key: set}
        Mapping key to {set of worker-identities}
    has_what: dict {worker: set}
        Mapping worker-identity to {set of keys}
    ncores: dict {worker: int}
        Mapping worker-identity to number-of-cores

    Queues
    ------

    -   interact_queue: get graphs from outside, report tasks done
    -   worker_queues: One queue per worker node.
        Each queue is listened to by several worker_core coroutiens.
    -   delete_queue: One queue listened to by ``delete`` which connects to the
        center to delete unnecessary intermediate data
    """
    stacks = {worker: list() for worker in ncores}
    processing = {worker: set() for worker in ncores}
    held_data = set()
    if dsk is None:
        dsk = dict()
    keyorder = dict()
    generation = 0

    @gen.coroutine
    def cleanup():
        """ Clean up queues and coroutines, prepare to stop """
        n = 0
        delete_queue.put_nowait({'op': 'close'}); n += 1
        for w, nc in ncores.items():
            for i in range(nc):
                worker_queues[w].put_nowait({'op': 'close'}); n += 1

        for i in range(n):
            yield scheduler_queue.get()

    def mark_ready_to_run(key):
        """ Send task to an appropriate worker, trigger worker if idle """
        if key in waiting:
            del waiting[key]
        new_worker = decide_worker(dependencies, stacks, who_has, key)
        stacks[new_worker].append(key)
        ensure_occupied(new_worker)

    def ensure_occupied(worker):
        """ If worker is free, spin up a task on that worker """
        while stacks[worker] and ncores[worker] > len(processing[worker]):
            key = stacks[worker].pop()
            processing[worker].add(key)
            worker_queues[worker].put_nowait({'op': 'compute-task',
                                              'key': key,
                                              'task': dsk[key],
                                              'needed': dependencies[key]})

    def seed_ready_tasks():
        """ Distribute leaves among workers """
        new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has, stacks,
                                       [k for k, deps in waiting.items() if not deps])
        for worker, stack in new_stacks.items():
            if stack:
                ensure_occupied(worker)


    def release_key(key):
        if key not in held_data and not waiting_data.get(key):
            delete_queue.put_nowait({'op': 'delete-task',
                                     'key': key})
            for w in who_has[key]:
                has_what[w].remove(key)
            del who_has[key]
            if key in waiting_data:
                del waiting_data[key]

    while True:
        msg = yield scheduler_queue.get()
        if msg['op'] == 'close':
            break
        elif msg['op'] == 'update-graph':
            new_dsk = msg['dsk']
            dsk.update(new_dsk)
            dependencies, dependents = get_deps(dsk)  # TODO: https://github.com/blaze/dask/issues/781

            _, _, _, new_held_data = insert_remote_deps(
                    dsk, dependencies, dependents, copy=False,
                    keys=list(new_dsk))

            held_data.update(msg['keys'])
            held_data.update(new_held_data)

            # TODO: replace heal with agglomerate function
            state = heal(dependencies, dependents, set(who_has), stacks, processing)
            waiting = state['waiting']
            waiting_data = state['waiting_data']
            finished_results = state['finished_results']

            new_keyorder = order(new_dsk)
            for key in new_keyorder:
                if key not in keyorder:
                    # TODO: add test for this
                    keyorder[key] = (generation, new_keyorder[key]) # prefer old
            generation += 1  # older graph generations take precedence

            seed_ready_tasks()

        elif msg['op'] == 'task-finished':
            key = msg['key']
            worker = msg['worker']
            log("task finished", key, worker)
            who_has[key].add(worker)
            has_what[worker].add(key)
            processing[worker].remove(key)
            interact_queue.put_nowait(msg)

            for dep in sorted(dependents[key], key=keyorder.get, reverse=True):
                s = waiting[dep]
                s.remove(key)
                if not s:  # new task ready to run
                    mark_ready_to_run(dep)

            for dep in dependencies[key]:
                s = waiting_data[dep]
                s.remove(key)
                if not s and dep:
                    release_key(dep)

            ensure_occupied(worker)

        elif msg['op'] == 'task-erred':
            processing[msg['worker']].remove(msg['key'])
            interact_queue.put_nowait(msg)

        elif msg['op'] == 'worker-failed':
            worker = msg['worker']
            keys = has_what.pop(worker)
            del worker_queues[worker]
            del ncores[worker]
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
            add_keys = {k for k, v in waiting.items() if not v}
            for key in held_data & released:
                interact_queue.put_nowait({'op': 'lost-key', 'key': key})
            for key in add_keys:
                mark_ready_to_run(key)
            for key in set(who_has) & released - held_data:
                delete_queue.put_nowait({'op': 'delete-task', 'key': key})

        elif msg['op'] == 'release-held-data':
            if msg['key'] in held_data:
                held_data.remove(msg['key'])
                release_key(msg['key'])

        else:
            log("Bad message", msg)

    log('Finished scheduling')
    yield cleanup()


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
                           for dep in dependents.get(key, ()))
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
        if key in new_released:
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
def _get(ip, port, dsk, result, gather=False):
    center = rpc(ip=ip, port=port)
    who_has, has_what, ncores = yield [center.who_has(),
                                       center.has_what(),
                                       center.ncores()]
    workers = sorted(ncores)

    worker_queues = {worker: Queue() for worker in workers}
    scheduler_queue = Queue()
    delete_queue = Queue()
    interact_queue = Queue()

    coroutines = ([interact(interact_queue, scheduler_queue, who_has, dsk, result),
                   scheduler(scheduler_queue, interact_queue, worker_queues, delete_queue,
                             who_has, has_what, ncores),
                   delete(scheduler_queue, delete_queue, ip, port)]
                + [worker(scheduler_queue, worker_queues[w], w, ncores[w])
                   for w in workers])

    results = yield All(coroutines)
    out_keys = results[0]

    if gather:
        out_data = yield _gather(center, out_keys)
        d = dict(zip(out_keys, out_data))
    else:
        d = {key: RemoteData(key, ip, port) for key in out_keys}

    raise Return(nested_get(result, d))


def get(ip, port, dsk, keys, gather=True, _get=_get):
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

    Examples
    --------
    >>> inc = lambda x: x + 1
    >>> dsk = {'x': 1, 'y': (inc, 'x')}
    >>> get('127.0.0.1', 8787, dsk, 'y')  # doctest: +SKIP

    Use with dask collections by partialing in the center ip/port

    >>> from functools import partial
    >>> myget = partial(get, '127.0.0.1', 8787)
    >>> import dask.array as da  # doctest: +SKIP
    >>> x = da.ones((1000, 1000), chunks=(100, 100))  # doctest: +SKIP
    >>> x.sum().compute(get=myget)  # doctest: +SKIP
    1000000
    """
    return IOLoop.current().run_sync(lambda: _get(ip, port, dsk, keys, gather))


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
    workers = frequencies(w for dep in deps
                            for w in who_has[dep])
    if not workers:
        workers = stacks
    worker = min(workers, key=lambda w: len(stacks[w]))
    return worker


def insert_remote_deps(dsk, dependencies, dependents, copy=True, keys=None):
    """ Find RemoteData objects, replace with keys and insert dependencies

    Examples
    --------

    >>> from operator import add
    >>> x = RemoteData('x', '127.0.0.1', 8787)
    >>> dsk = {'y': (add, x, 10)}
    >>> dependencies, dependents = get_deps(dsk)
    >>> dsk, dependencies, depdendents, held_data = insert_remote_deps(dsk, dependencies, dependents)
    >>> dsk
    {'y': (<built-in function add>, 'x', 10)}
    >>> dependencies  # doctest: +SKIP
    {'x': set(), 'y': {'x'}}
    >>> dependents  # doctest: +SKIP
    {'x': {'y'}, 'y': set()}
    >>> held_data
    {'x'}
    """
    if keys is None:
        keys = list(dsk)
    if copy:
        dsk = dsk.copy()
        dependencies = dependencies.copy()
        dependents = dependents.copy()
    held_data = set()

    for key in keys:
        value = dsk[key]
        vv, keys = unpack_remotedata(value)
        if keys:
            dependencies[key] |= keys
            dsk[key] = vv
            for k in keys:
                held_data.add(k)
                dependencies[k] = set()
                if not k in dependents:
                    dependents[k] = set()
                dependents[k].add(key)

    return dsk, dependencies, dependents, held_data


def assign_many_tasks(dependencies, waiting, keyorder, who_has, stacks, keys):
    """ Assign many new ready tasks to workers

    Often at the beginning of computation we have to assign many new leaves to
    several workers.  This initial seeding of work can have dramatic effects on
    total runtime.

    This takes some typical state variables (dependencies, waiting, keyorder,
    who_has, stacks) as well as a list of keys to assign to stacks.

    This mutates waiting and stacks in place and returns a dictionary,
    new_stacks, that serves as a diff between the old and new stacks.  These
    new tasks have yet to be put on worker queues.
    """
    leaves = list()  # ready tasks without data dependencies
    ready = list()   # ready tasks with data dependencies
    new_stacks = defaultdict(list)

    for k in keys:
        assert not waiting.pop(k)
        if not dependencies[k]:
            leaves.append(k)
        else:
            ready.append(k)

    leaves = sorted(leaves, key=keyorder.get)

    k = int(ceil(len(leaves) / len(stacks)))
    for i, worker in enumerate(stacks):
        keys = leaves[i*k: (i + 1)*k][::-1]
        new_stacks[worker].extend(keys)
        stacks[worker].extend(keys)

    for key in ready:
        worker = decide_worker(dependencies, stacks, who_has, key)
        new_stacks[worker].append(key)
        stacks[worker].append(key)

    return new_stacks
