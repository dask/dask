from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from functools import partial
import logging
from math import ceil
from time import time
import uuid

from toolz import frequencies, memoize, concat, first, identity
from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError, IOStream

from dask.core import istask, get_deps, reverse_dict, get_dependencies
from dask.order import order

from .core import (rpc, coerce_to_rpc, connect, read, write, MAX_BUFFER_SIZE,
        Server)
from .client import unpack_remotedata, scatter_to_workers, _gather
from .utils import All, ignoring, clear_queue


logger = logging.getLogger(__name__)


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


class Scheduler(Server):
    def __init__(self, center, delete_batch_time=1, loop=None,
            resource_interval=1, resource_log_size=1000,
            max_buffer_size=MAX_BUFFER_SIZE, **kwargs):
        self.scheduler_queues = [Queue()]
        self.report_queues = []
        self.delete_queue = Queue()
        self.streams = []
        self.status = None
        self.coroutines = []

        self.center = coerce_to_rpc(center)

        self.dask = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.generation = 0
        self.has_what = defaultdict(set)
        self.held_data = set()
        self.in_play = set()
        self.keyorder = dict()
        self.nbytes = dict()
        self.ncores = dict()
        self.nannies = dict()
        self.processing = dict()
        self.restrictions = dict()
        self.stacks = dict()
        self.waiting = dict()
        self.waiting_data = dict()
        self.who_has = defaultdict(set)

        self.exceptions = dict()
        self.tracebacks = dict()
        self.exceptions_blame = dict()
        self.resource_logs = dict()

        self.loop = loop or IOLoop.current()
        self.io_loop = self.loop

        self.delete_batch_time = delete_batch_time
        self.resource_interval = resource_interval
        self.resource_log_size = resource_log_size

        self.plugins = []

        self.compute_handlers = {'update-graph': self.update_graph,
                                 'update-data': self.update_data,
                                 'missing-data': self.mark_missing_data,
                                 'task-missing-data': self.mark_missing_data,
                                 'worker-failed': self.mark_worker_missing,
                                 'release-held-data': self.release_held_data,
                                 'restart': self._restart}

        self.handlers = {'start-control': self.control_stream,
                         'scatter': self.scatter,
                         'gather': self.gather}

        super(Scheduler, self).__init__(handlers=self.handlers, max_buffer_size=max_buffer_size, **kwargs)

    @property
    def port(self):
        return first(self._sockets.values()).getsockname()[1]

    def identity(self, stream):
        return {'type': type(self).__name__, 'id': self.id,
                'center': (self.center.ip, self.center.port),
                'workers': list(self.ncores)}

    def put(self, msg):
        return self.scheduler_queues[0].put_nowait(msg)

    @property
    def report_queue(self):
        return self.report_queues[0]

    @gen.coroutine
    def _sync_center(self):
        self.ncores, self.has_what, self.who_has, self.nannies = yield [
                self.center.ncores(),
                self.center.has_what(),
                self.center.who_has(),
                self.center.nannies()]

        self._nanny_coroutines = []
        for (ip, wport), nport in self.nannies.items():
            if not nport:
                continue
            if (ip, nport) not in self.resource_logs:
                self.resource_logs[(ip, nport)] = deque(maxlen=self.resource_log_size)

            self._nanny_coroutines.append(self._nanny_listen(ip, nport))

    def start(self, start_queues=True):
        collections = [self.dask, self.dependencies, self.dependents,
                self.waiting, self.waiting_data, self.in_play, self.keyorder,
                self.nbytes, self.processing, self.restrictions]
        for collection in collections:
            collection.clear()

        self.processing = {addr: set() for addr in self.ncores}
        self.stacks = {addr: list() for addr in self.ncores}

        self.worker_queues = {addr: Queue() for addr in self.ncores}

        with ignoring(AttributeError):
            self._delete_coroutine.cancel()
        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        self._delete_coroutine = self.delete()
        self._worker_coroutines = [self.worker(w) for w in self.ncores]

        self.heal_state()

        self.status = 'running'

        if start_queues:
            self.handle_queues(self.scheduler_queues[0], None)

        for cor in self.coroutines:
            if cor.done():
                raise cor.exception()

        return self._finished()

    @gen.coroutine
    def _finished(self):
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    @gen.coroutine
    def _close(self):
        yield self.cleanup()
        yield self._finished()
        yield self.center.close(close=True)
        self.center.close_streams()

    @gen.coroutine
    def cleanup(self):
        """ Clean up queues and coroutines, prepare to stop """
        if self.status == 'closing':
            raise gen.Return()

        self.status = 'closing'
        logger.debug("Cleaning up coroutines")
        n = 0
        self.delete_queue.put_nowait({'op': 'close'}); n += 1
        for w, nc in self.ncores.items():
            for i in range(nc):
                self.worker_queues[w].put_nowait({'op': 'close'}); n += 1

        for s in self.scheduler_queues[1:]:
            s.put_nowait({'op': 'close-stream'})

        for i in range(n):
            msg = yield self.scheduler_queues[0].get()

        for q in self.report_queues:
            q.put_nowait({'op': 'close'})

    def mark_ready_to_run(self, key):
        """ Send task to an appropriate worker, trigger worker """
        logger.debug("Mark %s ready to run", key)
        if key in self.waiting:
            assert not self.waiting[key]
            del self.waiting[key]

        new_worker = decide_worker(self.dependencies, self.stacks,
                self.who_has, self.restrictions, self.nbytes, key)

        self.stacks[new_worker].append(key)
        self.ensure_occupied(new_worker)

    def mark_key_in_memory(self, key, workers=None):
        """ Mark that key now lives in particular workers """
        logger.debug("Mark %s in memory", key)
        if workers is None:
            workers = self.who_has[key]
        for worker in workers:
            self.who_has[key].add(worker)
            self.has_what[worker].add(key)
            with ignoring(KeyError):
                self.processing[worker].remove(key)

        for dep in sorted(self.dependents.get(key, []), key=self.keyorder.get,
                          reverse=True):
            if dep in self.waiting:
                s = self.waiting[dep]
                with ignoring(KeyError):
                    s.remove(key)
                if not s:  # new task ready to run
                    self.mark_ready_to_run(dep)

        for dep in self.dependencies.get(key, []):
            if dep in self.waiting_data:
                s = self.waiting_data[dep]
                with ignoring(KeyError):
                    s.remove(key)
                if not s and dep:
                    self.release_key(dep)

        self.report({'op': 'key-in-memory',
                     'key': key,
                     'workers': workers})

    def ensure_occupied(self, worker):
        """ Spin up tasks on worker while it has tasks and free cores """
        logger.debug('Ensure worker is occupied: %s', worker)
        while (self.stacks[worker] and
               self.ncores[worker] > len(self.processing[worker])):
            key = self.stacks[worker].pop()
            self.processing[worker].add(key)
            logger.debug("Send job to worker: %s, %s, %s", worker, key, self.dask[key])
            self.worker_queues[worker].put_nowait(
                    {'op': 'compute-task',
                     'key': key,
                     'task': self.dask[key],
                     'needed': self.dependencies[key]})

    def seed_ready_tasks(self, keys=None):
        """ Distribute leaves among workers

        Takes an iterable of keys to consider for execution
        """
        if keys is None:
            keys = self.dask
        new_stacks = assign_many_tasks(
                self.dependencies, self.waiting, self.keyorder, self.who_has,
                self.stacks, self.restrictions, self.nbytes,
                [k for k in keys if k in self.waiting and not self.waiting[k]])
        logger.debug("Seed ready tasks: %s", new_stacks)
        for worker, stack in new_stacks.items():
            if stack:
                self.ensure_occupied(worker)

    def release_key(self, key):
        """ Release key from distributed memory if its ready """
        logger.debug("Release key %s", key)
        if key not in self.held_data and not self.waiting_data.get(key):
            self.delete_queue.put_nowait({'op': 'delete-task',
                                          'key': key})
            for w in self.who_has[key]:
                self.has_what[w].remove(key)
            del self.who_has[key]
            if key in self.waiting_data:
                del self.waiting_data[key]
            if key in self.in_play:
                self.in_play.remove(key)

    def update_data(self, who_has=None, nbytes=None):
        logger.debug("Update data %s", who_has)
        for key, workers in who_has.items():
            self.mark_key_in_memory(key, workers)

        self.nbytes.update(nbytes)

        self.held_data.update(who_has)
        self.in_play.update(who_has)

    def mark_task_erred(self, key, worker, exception, traceback):
        """ Mark that a task has erred on a particular worker """
        if key in self.processing[worker]:
            self.processing[worker].remove(key)
            self.exceptions[key] = exception
            self.tracebacks[key] = traceback
            self.mark_failed(key, key)
            self.ensure_occupied(worker)
            for plugin in self.plugins[:]:
                try:
                    plugin.task_erred(self, key, worker, exception)
                except Exception as e:
                    logger.exception(e)

    def mark_failed(self, key, failing_key=None):
        """ When a task fails mark it and all dependent task as failed """
        logger.debug("Mark key as failed %s", key)
        if key in self.exceptions_blame:
            return
        self.exceptions_blame[key] = failing_key
        self.report({'op': 'task-erred',
                     'key': key,
                     'exception': self.exceptions[failing_key],
                     'traceback': self.tracebacks[failing_key]})
        if key in self.waiting:
            del self.waiting[key]
        if key in self.waiting_data:
            del self.waiting_data[key]
        self.in_play.remove(key)
        for dep in self.dependents[key]:
            self.mark_failed(dep, failing_key)

    def mark_task_finished(self, key, worker, nbytes):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Mark task as finished %s, %s", key, worker)
        if key in self.processing[worker]:
            self.nbytes[key] = nbytes
            self.mark_key_in_memory(key, [worker])
            self.ensure_occupied(worker)
            for plugin in self.plugins[:]:
                try:
                    plugin.task_finished(self, key, worker, nbytes)
                except Exception as e:
                    logger.exception(e)
        else:
            logger.debug("Key not found in processing, %s, %s, %s",
                         key, worker, self.processing[worker])

    def mark_missing_data(self, missing=None, key=None, worker=None):
        missing = set(missing)
        logger.debug("Recovering missing data: %s", missing)
        for k in missing:
            with ignoring(KeyError):
                workers = self.who_has.pop(k)
                for worker in workers:
                    self.has_what[worker].remove(k)
        self.my_heal_missing_data(missing)

        if key and worker:
            with ignoring(KeyError):
                self.processing[worker].remove(key)
            self.waiting[key] = missing
            logger.info('task missing data, %s, %s', key, self.waiting)
            self.ensure_occupied(worker)

        self.seed_ready_tasks()

    def log_state(self, msg=''):
        logger.debug("Runtime State: %s", msg)
        logger.debug('\n\nwaiting: %s\n\nstacks: %s\n\nprocessing: %s\n\n'
                'in_play: %s\n\n', self.waiting, self.stacks, self.processing,
                self.in_play)

    def mark_worker_missing(self, worker=None, heal=True):
        """ Mark that a worker no longer seems responsive """
        logger.debug("Mark worker as missing %s", worker)
        if worker not in self.processing:
            return
        keys = self.has_what.pop(worker)
        for i in range(self.ncores[worker]):  # send close message, in case not dead
            self.worker_queues[worker].put_nowait({'op': 'close', 'report': False})
        del self.worker_queues[worker]
        del self.ncores[worker]
        del self.stacks[worker]
        del self.processing[worker]
        if not self.stacks:
            logger.critical("Lost all workers")
        missing_keys = set()
        for key in keys:
            self.who_has[key].remove(worker)
            if not self.who_has[key]:
                missing_keys.add(key)
        gone_data = {k for k, v in self.who_has.items() if not v}
        self.in_play.difference_update(missing_keys)
        for k in gone_data:
            del self.who_has[k]

        if heal:
            self.heal_state()

    def update_graph(self, dsk=None, keys=None, restrictions={}):
        update_state(self.dask, self.dependencies, self.dependents,
                self.held_data, self.who_has, self.in_play,
                self.waiting, self.waiting_data, dsk, keys)

        cover_aliases(self.dask, dsk)

        self.restrictions.update(restrictions)

        new_keyorder = order(dsk)  # TODO: define order wrt old graph
        for key in new_keyorder:
            if key not in self.keyorder:
                # TODO: add test for this
                self.keyorder[key] = (self.generation, new_keyorder[key]) # prefer old
        if len(dsk) > 1:
            self.generation += 1  # older graph generations take precedence

        for key in dsk:
            for dep in self.dependencies[key]:
                if dep in self.exceptions_blame:
                    self.mark_failed(key, self.exceptions_blame[dep])

        self.seed_ready_tasks(dsk)
        for key in keys:
            if self.who_has[key]:
                self.mark_key_in_memory(key)

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(self, dsk, keys, restrictions)
            except Exception as e:
                logger.exception(e)

    def release_held_data(self, key=None):
        if key in self.held_data:
            logger.debug("Release key: %s", key)
            self.held_data.remove(key)
            self.release_key(key)

    def heal_state(self):
        """ Recover from catastrophic change """
        logger.debug("Heal state")
        self.log_state("Before Heal")
        state = heal(self.dependencies, self.dependents, set(self.who_has),
                self.stacks, self.processing, self.waiting, self.waiting_data)
        released = state['released']
        self.in_play.clear(); self.in_play.update(state['in_play'])
        add_keys = {k for k, v in self.waiting.items() if not v}
        for key in self.held_data & released:
            self.report({'op': 'lost-key', 'key': key})
        if self.stacks:
            for key in add_keys:
                self.mark_ready_to_run(key)
        for key in set(self.who_has) & released - self.held_data:
            self.delete_queue.put_nowait({'op': 'delete-task', 'key': key})
        self.in_play.update(self.who_has)
        self.log_state("After Heal")

    def my_heal_missing_data(self, missing):
        logger.debug("Heal from missing data")
        return heal_missing_data(self.dask, self.dependencies, self.dependents,
                self.held_data, self.who_has, self.in_play, self.waiting,
                self.waiting_data, missing)

    def report(self, msg):
        for q in self.report_queues:
            q.put_nowait(msg)
        for s in self.streams:
            try:
                write(s, msg)
            except StreamClosedError:
                logger.critical("Tried writing to closed stream: %s", msg)

    def add_plugin(self, plugin):
        self.plugins.append(plugin)

    def handle_queues(self, scheduler_queue, report_queue):
        self.scheduler_queues.append(scheduler_queue)
        if report_queue:
            self.report_queues.append(report_queue)
        future = self.handle_messages(scheduler_queue, report_queue)
        self.coroutines.append(future)
        return future

    @gen.coroutine
    def control_stream(self, stream, address=None):
        ident = str(uuid.uuid1())
        logger.info("Connection to %s, %s", type(self).__name__, ident)
        self.streams.append(stream)
        try:
            yield self.handle_messages(stream, stream)
        finally:
            if not stream.closed():
                yield write(stream, {'op': 'stream-closed'})
                stream.close()
            self.streams.remove(stream)
            logger.info("Close connection to %s, %s", type(self).__name__,
                    ident)

    @gen.coroutine
    def handle_messages(self, in_queue, report):
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
        if isinstance(in_queue, Queue):
            next_message = in_queue.get
        elif isinstance(in_queue, IOStream):
            next_message = lambda: read(in_queue)
        else:
            raise NotImplementedError()

        if isinstance(report, Queue):
            put = report.put_nowait
        elif isinstance(report, IOStream):
            put = lambda msg: write(report, msg)
        else:
            put = lambda msg: None
        put({'op': 'stream-start'})

        while True:
            msg = yield next_message()  # in_queue.get()
            logger.debug("scheduler receives message %s", msg)
            op = msg.pop('op')

            if op == 'close-stream':
                break
            elif op == 'close':
               self._close()
               break
            elif op in self.compute_handlers:
                result = self.compute_handlers[op](**msg)
                if isinstance(result, gen.Future):
                    yield result
            else:
                logger.warn("Bad message: op=%s, %s", op, msg)

            if op == 'close':
                break

        logger.debug('Finished scheduling coroutine')

    @gen.coroutine
    def worker(self, ident):
        """ Manage a single distributed worker node

        This coroutine manages one remote worker.  It spins up several
        ``worker_core`` coroutines, one for each core.  It reports a closed
        connection to scheduler if one occurs.
        """
        try:
            yield All([self.worker_core(ident, i)
                    for i in range(self.ncores[ident])])
        except (IOError, OSError):
            logger.info("Worker failed from closed stream: %s", ident)
            self.put({'op': 'worker-failed',
                      'worker': ident})

    @gen.coroutine
    def worker_core(self, ident, i):
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
            msg = yield self.worker_queues[ident].get()
            if msg['op'] == 'close':
                logger.debug("Worker core receives close message %s, %s",
                        ident, msg)
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
                logger.debug("Compute response from worker %s, %s, %s, %s",
                             ident, key, response, content)
                if response == b'error':
                    error, traceback = content
                    self.mark_task_erred(key, ident, error, traceback)

                elif response == b'missing-data':
                    self.mark_missing_data(content.args, key=key, worker=ident)

                else:
                    self.mark_task_finished(key, ident, nbytes)

        yield worker.close(close=True)
        worker.close_streams()
        if msg.get('report', True):
            self.put({'op': 'worker-finished',
                      'worker': ident})
        logger.debug("Close worker core, %s, %d", ident, i)


    @gen.coroutine
    def delete(self):
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

        while True:
            msg = yield self.delete_queue.get()
            if msg['op'] == 'close':
                break

            # TODO: trigger coroutine to go off in a second if no activity
            batch.append(msg['key'])
            if batch and time() - last > self.delete_batch_time:  # One second batching
                logger.debug("Ask center to delete %d keys", len(batch))
                last = time()
                yield self.center.delete_data(keys=batch)
                batch = list()

        if batch:
            yield self.center.delete_data(keys=batch)

        self.put({'op': 'delete-finished'})
        logger.debug('Delete finished')

    @gen.coroutine
    def _nanny_listen(self, ip, port):
        stream = yield connect(ip=ip, port=port)
        yield write(stream, {'op': 'monitor_resources',
                             'interval': self.resource_interval})
        while not stream.closed():
            msg = yield read(stream)
            self.resource_logs[(ip, port)].append(msg)

    @gen.coroutine
    def scatter(self, stream=None, data=None, workers=None):
        if not self.ncores:
            raise ValueError("No workers yet found.  "
                             "Try syncing with center.\n"
                             "  e.sync_center()")
        ncores = workers if workers is not None else self.ncores
        remotes, who_has, nbytes = yield scatter_to_workers(
                                            self.center, ncores, data)
        self.update_data(who_has=who_has, nbytes=nbytes)

        raise gen.Return(remotes)

    @gen.coroutine
    def gather(self, stream=None, keys=None):
        keys = list(keys)

        try:
            data = yield _gather(self.center, keys)
            data = dict(zip(keys, data))
            result = (b'OK', data)
        except KeyError as e:
            logger.debug("Couldn't gather keys %s", e)
            result = (b'error', e)

        raise gen.Return(result)

    @gen.coroutine
    def _restart(self):
        logger.debug("Send shutdown signal to workers")

        for q in self.scheduler_queues + self.report_queues:
            clear_queue(q)

        for addr in self.nannies:
            self.mark_worker_missing(worker=addr, heal=False)

        logger.debug("Send kill signal to nannies")
        nannies = [rpc(ip=ip, port=n_port)
                   for (ip, w_port), n_port in self.nannies.items()]
        yield All([nanny.kill() for nanny in nannies])

        while self.ncores:
            yield gen.sleep(0.01)

        # All quiet

        yield All([nanny.instantiate(close=True) for nanny in nannies])
        yield self._sync_center()
        self.start()

        self.report({'op': 'restart'})
        for plugin in self.plugins[:]:
            try:
                plugin.restart(self)
            except Exception as e:
                logger.exception(e)

scheduler = 0
