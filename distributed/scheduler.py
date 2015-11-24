from __future__ import print_function, division, absolute_import

from collections import defaultdict
from functools import partial
import logging
from math import ceil
from time import time

from toolz import frequencies, memoize, concat, first, identity
from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from dask.core import istask, get_deps, reverse_dict, get_dependencies
from dask.order import order

from .core import rpc
from .client import unpack_remotedata
from .utils import All, ignoring


logger = logging.getLogger(__name__)


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
    except (IOError, OSError):
        logger.info("Worker failed from closed stream: %s", ident)
        scheduler_queue.put_nowait({'op': 'worker-failed',
                                    'worker': ident})


@gen.coroutine
def worker_core(scheduler_queue, worker_queue, ident, i):
    """ Manage one core on one distributed worker node

    This coroutine listens on worker_queue for the following operations

    **Incoming Messages**:

    - compute-task:  call worker.compute(...) on remote node, report when done
    - close: close connection to worker node, report `worker-finished` to
      scheduler

    **Outgoing Messages**:

    - task-finished:  sent to scheduler once a task completes
    - task-erred: sent to scheduler when a task errs
    - worker-finished: sent to scheduler in response to a close command
    """
    worker = rpc(ip=ident[0], port=ident[1])
    logger.debug("Start worker core %s, %d", ident, i)

    while True:
        msg = yield worker_queue.get()
        if msg['op'] == 'close':
            break
        if msg['op'] == 'compute-task':
            key = msg['key']
            needed = msg['needed']
            task = msg['task']
            if not istask(task):
                response, content = yield worker.update_data(data={key: task})
                assert response == b'OK', response
                nbytes = content['nbytes'][key]
            else:
                response, content = yield worker.compute(function=execute_task,
                                                         args=(task,),
                                                         needed=needed,
                                                         key=key,
                                                         kwargs={})
                if response == b'OK':
                    nbytes = content['nbytes']
            if response == b'error':
                error, traceback = content
                scheduler_queue.put_nowait({'op': 'task-erred',
                                            'key': key,
                                            'worker': ident,
                                            'exception': error,
                                            'traceback': traceback})

            elif response == b'missing-data':
                scheduler_queue.put_nowait({'op': 'task-missing-data',
                                            'key': key,
                                            'worker': ident,
                                            'missing': content.args})

            else:
                scheduler_queue.put_nowait({'op': 'task-finished',
                                            'workers': [ident],
                                            'key': key,
                                            'nbytes': nbytes})

    yield worker.close(close=True)
    worker.close_streams()
    if msg.get('report', True):
        scheduler_queue.put_nowait({'op': 'worker-finished',
                                    'worker': ident})
    logger.debug("Close worker core, %s, %d", ident, i)


@gen.coroutine
def delete(scheduler_queue, delete_queue, ip, port, batch_time=1):
    """ Delete extraneous intermediates from distributed memory

    This coroutine manages a connection to the center in order to send keys
    that should be removed from distributed memory.  We batch several keys that
    come in over the ``delete_queue`` into a list.  Roughly once a second we
    send this list of keys over to the center which then handles deleting
    these keys from workers' memory.

    worker \                                /-> worker node
    worker -> scheduler -> delete -> center --> worker node
    worker /                                \-> worker node

    **Incoming Messages**

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

        # TODO: trigger coroutine to go off in a second if no activity
        batch.append(msg['key'])
        if batch and time() - last > batch_time:  # One second batching
            logger.debug("Ask center to delete %d keys", len(batch))
            last = time()
            yield center.delete_data(keys=batch)
            batch = list()

    if batch:
        yield center.delete_data(keys=batch)

    yield center.close(close=True)
    center.close_streams()          # All done
    scheduler_queue.put_nowait({'op': 'delete-finished'})
    logger.debug('Delete finished')


@gen.coroutine
def scheduler(scheduler_queue, report_queue, worker_queues, delete_queue,
              who_has, has_what, ncores, dsk=None, dependencies=None,
              dependents=None, restrictions=None, held_data=None, nbytes=None,
              waiting=None, waiting_data=None, stacks=None, processing=None):
    """ The scheduler coroutine for dask scheduling

    This coroutine manages interactions with all worker cores and with the
    delete coroutine through queues.

    Parameters
    ----------
    scheduler_queue: tornado.queues.Queue
        Get information from outside
    report_queue: tornado.queues.Queue
        Report information to outside
    worker_queues: dict {worker: tornado.queues.Queue}
        One queue per worker node.
        Each queue is listened to by several worker_core coroutines.
    delete_queue: tornado.queues.Queue
        One queue listened to by ``delete`` which connects to the
        center to delete unnecessary intermediate data
    who_has: dict {key: set}
        Mapping key to {set of worker-identities}
    has_what: dict {worker: set}
        Mapping worker-identity to {set of keys}
    ncores: dict {worker: int}
        Mapping worker-identity to number-of-cores
    """
    held_data = set() if held_data is None else held_data
    dsk = dict() if dsk is None else dsk
    dependencies = dict() if dependencies is None else dependencies
    dependents = dict() if dependents is None else dependents
    nbytes = dict() if nbytes is None else nbytes
    restrictions = dict() if restrictions is None else restrictions
    waiting = dict() if waiting is None else waiting
    waiting_data = dict() if waiting_data is None else waiting_data
    stacks = dict() if stacks is None else stacks
    processing = dict() if processing is None else processing

    stacks.clear()
    stacks.update({worker: list() for worker in ncores})
    processing.clear()
    processing.update({worker: set() for worker in ncores})

    assert (not dsk) == (not dependencies), (dsk, dependencies)

    in_play = set(who_has)  # keys in memory, stacks, processing, or waiting
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
        """ Send task to an appropriate worker, trigger worker """
        if key in waiting:
            assert not waiting[key]
            del waiting[key]

        new_worker = decide_worker(dependencies, stacks, who_has, restrictions,
                                   nbytes, key)
        stacks[new_worker].append(key)
        ensure_occupied(new_worker)

    def mark_key_in_memory(key, workers=None):
        if workers is None:
            workers = who_has[key]
        for worker in workers:
            who_has[key].add(worker)
            has_what[worker].add(key)
            with ignoring(KeyError):
                processing[worker].remove(key)

        for dep in sorted(dependents.get(key, []), key=keyorder.get, reverse=True):
            if dep in waiting:
                s = waiting[dep]
                with ignoring(KeyError):
                    s.remove(key)
                if not s:  # new task ready to run
                    mark_ready_to_run(dep)

        for dep in dependencies.get(key, []):
            if dep in waiting_data:
                s = waiting_data[dep]
                with ignoring(KeyError):
                    s.remove(key)
                if not s and dep:
                    release_key(dep)

        report_queue.put_nowait({'op': 'key-in-memory',
                                 'key': key,
                                 'workers': workers})

    def ensure_occupied(worker):
        """ Spin up tasks on worker while it has tasks and free cores """
        logger.debug('Ensure worker is occupied: %s', worker)
        while stacks[worker] and ncores[worker] > len(processing[worker]):
            key = stacks[worker].pop()
            processing[worker].add(key)
            logger.debug("Send job to worker: %s, %s, %s", worker, key, dsk[key])
            worker_queues[worker].put_nowait({'op': 'compute-task',
                                              'key': key,
                                              'task': dsk[key],
                                              'needed': dependencies[key]})

    def seed_ready_tasks(keys=dsk):
        """ Distribute leaves among workers

        Takes an iterable of keys to consider for execution
        """
        new_stacks = assign_many_tasks(dependencies, waiting, keyorder, who_has,
                                       stacks, restrictions, nbytes,
                                       [k for k in keys if k in waiting and
                                                        not waiting[k]])
        logger.debug("Seed ready tasks: %s", new_stacks)
        for worker, stack in new_stacks.items():
            if stack:
                ensure_occupied(worker)

    def release_key(key):
        """ Release key from distributed memory if its ready """
        if key not in held_data and not waiting_data.get(key):
            delete_queue.put_nowait({'op': 'delete-task',
                                     'key': key})
            for w in who_has[key]:
                has_what[w].remove(key)
            del who_has[key]
            if key in waiting_data:
                del waiting_data[key]
            if key in in_play:
                in_play.remove(key)

    def update_data(extra_who_has, extra_nbytes):
        for key, workers in extra_who_has.items():
            mark_key_in_memory(key, workers)

        nbytes.update(extra_nbytes)

        held_data.update(extra_who_has)
        in_play.update(extra_who_has)

    exceptions = dict()
    tracebacks = dict()
    exceptions_blame = dict()
    def mark_failed(key, failing_key=None):
        """ When a task fails mark it and all dependent task as failed """
        if key in exceptions_blame:
            return
        exceptions_blame[key] = failing_key
        report_queue.put_nowait({'op': 'task-erred',
                                 'key': key,
                                 'exception': exceptions[failing_key],
                                 'traceback': tracebacks[failing_key]})
        if key in waiting:
            del waiting[key]
        if key in waiting_data:
            del waiting_data[key]
        in_play.remove(key)
        for dep in dependents[key]:
            mark_failed(dep, failing_key)

    def log_state(msg=''):
        logger.debug("Runtime State: %s", msg)
        logger.debug('\n\nwaiting: %s\n\nstacks: %s\n\nprocessing: %s\n\n'
                'in_play: %s\n\n', waiting, stacks, processing, in_play)

    def mark_worker_missing(worker):
        if worker not in processing:
            return
        keys = has_what.pop(worker)
        for i in range(ncores[worker]):  # send close message, in case not dead
            worker_queues[worker].put_nowait({'op': 'close', 'report': False})
        del worker_queues[worker]
        del ncores[worker]
        del stacks[worker]
        del processing[worker]
        if not stacks:
            logger.critical("Lost all workers")
        missing_keys = set()
        for key in keys:
            who_has[key].remove(worker)
            if not who_has[key]:
                missing_keys.add(key)
        gone_data = {k for k, v in who_has.items() if not v}
        in_play.difference_update(missing_keys)
        for k in gone_data:
            del who_has[k]

    def heal_state():
        """ Recover from catastrophic change """
        state = heal(dependencies, dependents, set(who_has), stacks,
                     processing, waiting, waiting_data)
        released = state['released']
        in_play.clear(); in_play.update(state['in_play'])
        add_keys = {k for k, v in waiting.items() if not v}
        for key in held_data & released:
            report_queue.put_nowait({'op': 'lost-key', 'key': key})
        if stacks:
            for key in add_keys:
                mark_ready_to_run(key)
        for key in set(who_has) & released - held_data:
            delete_queue.put_nowait({'op': 'delete-task', 'key': key})

    my_heal_missing_data = partial(heal_missing_data,
            dsk, dependencies, dependents, held_data, who_has, in_play,
            waiting, waiting_data)

    heal_state()

    while True:
        msg = yield scheduler_queue.get()
        if msg['op'] == 'close':
            break
        elif msg['op'] == 'update-graph':
            update_state(dsk, dependencies, dependents, held_data,
                         who_has, in_play,
                         waiting, waiting_data, msg['dsk'], msg['keys'])

            cover_aliases(dsk, msg['dsk'])

            restrictions.update(msg.get('restrictions', {}))

            new_keyorder = order(msg['dsk'])  # TODO: define order wrt old graph
            for key in new_keyorder:
                if key not in keyorder:
                    # TODO: add test for this
                    keyorder[key] = (generation, new_keyorder[key]) # prefer old
            if len(msg['dsk']) > 1:
                generation += 1  # older graph generations take precedence

            for key in msg['dsk']:
                for dep in dependencies[key]:
                    if dep in exceptions_blame:
                        mark_failed(key, exceptions_blame[dep])

            seed_ready_tasks(msg['dsk'])
            for key in msg['keys']:
                if who_has[key]:
                    mark_key_in_memory(key)

        elif msg['op'] == 'update-data':
            update_data(msg['who-has'], msg['nbytes'])

        elif msg['op'] == 'task-finished':
            key, worker = msg['key'], msg['workers'][0]
            if key in processing[worker]:
                nbytes[key] = msg['nbytes']
                mark_key_in_memory(key, [worker])
                ensure_occupied(worker)

        elif msg['op'] == 'task-erred':
            key, worker = msg['key'], msg['worker']
            if key in processing[worker]:
                processing[worker].remove(key)
                exceptions[key] = msg['exception']
                tracebacks[key] = msg['traceback']
                mark_failed(key, key)
                ensure_occupied(worker)

        elif msg['op'] in ('missing-data', 'task-missing-data'):
            missing = set(msg['missing'])
            logger.debug("Recovering missing data: %s", missing)
            for k in missing:
                with ignoring(KeyError):
                    workers = who_has.pop(k)
                    for worker in workers:
                        has_what[worker].remove(k)
            my_heal_missing_data(missing)

            if msg['op'] == 'task-missing-data':
                key = msg['key']
                with ignoring(KeyError):
                    processing[msg['worker']].remove(key)
                waiting[key] = missing
                logger.info('task missing data, %s, %s', key, waiting)
                with ignoring(KeyError):
                    processing[msg['worker']].remove(msg['key'])

                ensure_occupied(msg['worker'])

            seed_ready_tasks()

        elif msg['op'] == 'worker-failed':
            worker = msg['worker']
            mark_worker_missing(worker)
            if msg.get('heal', True):
                heal_state()

        elif msg['op'] == 'release-held-data':
            if msg['key'] in held_data:
                logger.debug("Release key: %s", msg['key'])
                held_data.remove(msg['key'])
                release_key(msg['key'])

        else:
            logger.warn("Bad message: %s", msg)

    logger.debug('Finished scheduling')
    yield cleanup()


def validate_state(dependencies, dependents, waiting, waiting_data,
        in_memory, stacks, processing, finished_results, released, in_play,
        **kwargs):
    """ Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.

    See Also
    --------
    distributed.scheduler.heal: fix a broken runtime state
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

        assert (key in released) != (key in in_play)

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


def keys_outside_frontier(dsk, dependencies, keys, frontier):
    """ All keys required by terminal keys within graph up to frontier

    Given:

    1. A graph
    2. A set of desired keys
    3. A frontier/set of already-computed (or already-about-to-be-computed) keys

    Find all keys necessary to compute the desired set that are outside of the
    frontier.

    Parameters
    ----------
    dsk: dict
    keys: iterable of keys
    frontier: set of keys

    Examples
    --------
    >>> f = lambda:1
    >>> dsk = {'x': 1, 'a': 2, 'y': (f, 'x'), 'b': (f, 'a'), 'z': (f, 'b', 'y')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> keys = {'z', 'b'}
    >>> frontier = {'y', 'a'}
    >>> list(sorted(keys_outside_frontier(dsk, dependencies, keys, frontier)))
    ['b', 'z']
    """
    assert isinstance(keys, set)
    assert isinstance(frontier, set)
    stack = list(keys - frontier)
    result = set()
    while stack:
        x = stack.pop()
        if x in result or x in frontier:
            continue
        result.add(x)
        stack.extend(dependencies[x])

    return result


def update_state(dsk, dependencies, dependents, held_data,
                 who_has, in_play,
                 waiting, waiting_data, new_dsk, new_keys):
    """ Update state given new dask graph and output keys

    This should operate in linear time relative to the size of edges of the
    added graph.  It assumes that the current runtime state is valid.
    """
    dsk.update(new_dsk)
    if not isinstance(new_keys, set):
        new_keys = set(new_keys)

    for key in new_dsk:  # add dependencies/dependents
        if key in dependencies:
            continue

        deps = get_dependencies(dsk, key)
        dependencies[key] = deps

        for dep in deps:
            if dep not in dependents:
                dependents[dep] = set()
            dependents[dep].add(key)

        if key not in dependents:
            dependents[key] = set()

    for key, value in new_dsk.items():  # add in remotedata
        vv, s = unpack_remotedata(value)
        if s:
            # TODO: check against in-memory, maybe add to in_play
            dsk[key] = vv
            dependencies[key] |= s
            for dep in s:
                if not dep in dependencies:
                    held_data.add(dep)
                    dependencies[dep] = set()
                if dep not in dependents:
                    dependents[dep] = set()
                dependents[dep].add(key)

    exterior = keys_outside_frontier(dsk, dependencies, new_keys, in_play)
    in_play |= exterior
    for key in exterior:
        deps = dependencies[key]
        waiting[key] = {d for d in deps if not (d in who_has and who_has[d])}
        for dep in deps:
            if dep not in waiting_data:
                waiting_data[dep] = set()
            waiting_data[dep].add(key)

        if key not in waiting_data:
            waiting_data[key] = set()

    held_data |= new_keys

    return {'dsk': dsk,
            'dependencies': dependencies,
            'dependents': dependents,
            'held_data': held_data,
            'waiting': waiting,
            'waiting_data': waiting_data}


def heal(dependencies, dependents, in_memory, stacks, processing, waiting,
        waiting_data, **kwargs):
    """ Make a possibly broken runtime state consistent

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

    waiting_data.clear()
    waiting.clear()
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

    waiting_data.update({key: {dep for dep in dependents[key]
                              if dep not in new_released
                              and dep not in in_memory}
                    for key in dependents
                    if key not in new_released})

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

    in_play = (in_memory
            | set(waiting)
            | (set.union(*processing.values()) if processing else set())
            | set(concat(stacks.values())))

    output = {'keys': outputs,
             'dependencies': dependencies, 'dependents': dependents,
             'waiting': waiting, 'waiting_data': waiting_data,
             'in_memory': in_memory, 'processing': processing, 'stacks': stacks,
             'finished_results': finished_results, 'released': new_released,
             'in_play': in_play}
    validate_state(**output)
    return output


def decide_worker(dependencies, stacks, who_has, restrictions, nbytes, key):
    """ Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> stacks = {('alice', 8000): ['z'], ('bob', 8000): []}
    >>> who_has = {'a': {('alice', 8000)}}
    >>> nbytes = {'a': 100}
    >>> restrictions = {}

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'b')
    ('alice', 8000)

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {('alice', 8000), ('bob', 8000)}}
    >>> decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'b')
    ('bob', 8000)

    Optionally provide restrictions of where jobs are allowed to occur

    >>> restrictions = {'b': {'alice', 'charile'}}
    >>> decide_worker(dependencies, stacks, who_has, restrictions, nbytes, 'b')
    ('alice', 8000)

    If the task requires data communication, then we choose to minimize the
    number of bytes sent between workers. This takes precedence over worker
    occupancy.

    >>> dependencies = {'c': {'a', 'b'}}
    >>> who_has = {'a': {('alice', 8000)}, 'b': {('bob', 8000)}}
    >>> nbytes = {'a': 1, 'b': 1000}
    >>> stacks = {('alice', 8000): [], ('bob', 8000): []}

    >>> decide_worker(dependencies, stacks, who_has, {}, nbytes, 'c')
    ('bob', 8000)
    """
    deps = dependencies[key]
    workers = frequencies(w for dep in deps
                            for w in who_has[dep])
    if not workers:
        workers = stacks
    if key in restrictions:
        r = restrictions[key]
        workers = {w for w in workers if w[0] in r}  # TODO: nonlinear
        if not workers:
            workers = {w for w in stacks if w[0] in r}
            if not workers:
                raise ValueError("Task has no valid workers", key, r)
    if not workers or not stacks:
        raise ValueError("No workers found")

    commbytes = {w: sum(nbytes[k] for k in dependencies[key]
                                   if w not in who_has[k])
                 for w in workers}

    minbytes = min(commbytes.values())

    workers = {w for w, nb in commbytes.items() if nb == minbytes}
    worker = min(workers, key=lambda w: len(stacks[w]))
    return worker


_round_robin = [0]


def assign_many_tasks(dependencies, waiting, keyorder, who_has, stacks,
        restrictions, nbytes, keys):
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
        if not dependencies[k] and k not in restrictions:
            leaves.append(k)
        else:
            ready.append(k)

    if not stacks:
        raise ValueError("No workers found")

    leaves = sorted(leaves, key=keyorder.get)

    workers = list(stacks)

    k = _round_robin[0] % len(workers)
    workers = workers[k:] + workers[:k]
    _round_robin[0] += 1

    k = int(ceil(len(leaves) / len(workers)))
    for i, worker in enumerate(workers):
        keys = leaves[i*k: (i + 1)*k][::-1]
        new_stacks[worker].extend(keys)
        stacks[worker].extend(keys)

    for key in ready:
        worker = decide_worker(dependencies, stacks, who_has, restrictions,
                nbytes, key)
        new_stacks[worker].append(key)
        stacks[worker].append(key)

    return new_stacks


def heal_missing_data(dsk, dependencies, dependents, held_data,
                      in_memory, in_play,
                      waiting, waiting_data, missing):
    """ Return to healthy state after discovering missing data

    When we identify that we're missing certain keys we rewind runtime state to
    evaluate those keys.
    """
    logger.debug("Healing missing: %s", missing)
    for key in missing:
        if key in in_play:
            in_play.remove(key)

    def ensure_key(key):
        if key in in_play:
            return
        for dep in dependencies[key]:
            ensure_key(dep)
            waiting_data[dep].add(key)
        waiting[key] = {dep for dep in dependencies[key] if dep not in in_memory}
        logger.debug("Added key to waiting: %s", key)
        waiting_data[key] = {dep for dep in dependents[key] if dep in in_play
                                                    and dep not in in_memory}
        in_play.add(key)

    for key in missing:
        ensure_key(key)

    assert set(missing).issubset(in_play)


def cover_aliases(dsk, new_keys):
    """ Replace aliases with calls to identity

    Warning: operates in place

    >>> dsk = {'x': 1, 'y': 'x'}
    >>> cover_aliases(dsk, ['y'])  # doctest: +SKIP
    {'x': 1, 'y': (<function identity ...>, 'x')}
    """
    for key in new_keys:
        try:
            if dsk[key] in dsk:
                dsk[key] = (identity, dsk[key])
        except TypeError:
            pass

    return dsk


def execute_task(task):
    """ Evaluate a nested task """
    if istask(task):
        func, args = task[0], task[1:]
        return func(*map(execute_task, args))
    else:
        return task
