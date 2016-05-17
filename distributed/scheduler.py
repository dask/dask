from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from datetime import datetime, timedelta
import logging
import math
import random
import socket
from time import time
from timeit import default_timer

try:
    from cytoolz import frequencies, topk
except ImportError:
    from toolz import frequencies, topk
from toolz import memoize, valmap, first, second, keymap, unique
from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError, IOStream

from dask.compatibility import PY3, unicode
from dask.core import reverse_dict
from dask.order import order

from .batched import BatchedSend
from .client import (scatter_to_workers, gather_from_workers)
from .core import (rpc, connect, read, write, MAX_BUFFER_SIZE,
        Server, send_recv, coerce_to_address, error_message)
from .utils import (All, ignoring, clear_queue, get_ip, ignore_exceptions,
        ensure_ip, log_errors, key_split, mean, divide_n_among_bins)


logger = logging.getLogger(__name__)


class Scheduler(Server):
    """ Dynamic distributed task scheduler

    The scheduler tracks the current state of workers, data, and computations.
    The scheduler listens for events and responds by controlling workers
    appropriately.  It continuously tries to use the workers to execute an ever
    growing dask graph.

    All events are handled quickly, in linear time with respect to their input
    (which is often of constant size) and generally within a millisecond.  To
    accomplish this the scheduler tracks a lot of state.  Every operation
    maintains the consistency of this state.

    The scheduler communicates with the outside world either by adding pairs of
    in/out queues or by responding to a new IOStream (the Scheduler can operate
    as a typical distributed ``Server``).  It maintains a consistent and valid
    view of the world even when listening to several clients at once.

    A Scheduler is typically started either with the ``dscheduler``
    executable::

        $ dscheduler 127.0.0.1:8787  # address of center

    Or as part of when an Executor starts up and connects to a Center::

        >>> e = Executor('127.0.0.1:8787')  # doctest: +SKIP
        >>> e.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler except through Plugins.
    See http://distributed.readthedocs.io/en/latest/plugins.html

    **State**

    * **tasks:** ``{key: task}``:
        Dictionary mapping key to task, either dask task, or serialized dict
        like: ``{'function': b'xxx', 'args': b'xxx'}`` or ``{'task': b'xxx'}``
    * **dependencies:** ``{key: {key}}``:
        Dictionary showing which keys depend on which others
    * **dependents:** ``{key: {key}}``:
        Dictionary showing which keys are dependent on which others
    * **waiting:** ``{key: {key}}``:
        Dictionary like dependencies but excludes keys already computed
    * **waiting_data:** ``{key: {key}}``:
        Dictionary like dependents but excludes keys already computed
    * **ready:** ``deque(key)``
        Keys that are ready to run, but not yet assigned to a worker
    * **ncores:** ``{worker: int}``:
        Number of cores owned by each worker
    * **idle:** ``{worker}``:
        Set of workers that are not fully utilized
    * **services:** ``{str: port}``:
        Other services running on this scheduler, like HTTP
    * **worker_info:** ``{worker: {str: data}}``:
        Information about each worker
    * **host_info:** ``{hostname: dict}``:
        Information about each worker host
    * **who_has:** ``{key: {worker}}``:
        Where each key lives.  The current state of distributed memory.
    * **has_what:** ``{worker: {key}}``:
        What worker has what keys.  The transpose of who_has.
    * **who_wants:** ``{key: {client}}``:
        Which clients want each key.  The active targets of computation.
    * **wants_what:** ``{client: {key}}``:
        What keys are wanted by each client..  The transpose of who_wants.
    * **nbytes:** ``{key: int}``:
        Number of bytes for a key as reported by workers holding that key.
    * **processing:** ``{worker: {key: cost}}``:
        Set of keys currently in execution on each worker and their expected
        duration
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    * **occupancy:** ``{worker: time}``
        Expected runtime for all tasks currently processing on a worker
    * **stacks:** ``{worker: [keys]}``:
        List of keys waiting to be sent to each worker
    * **released:** ``{keys}``
        Set of keys that are known, but released from memory
    * **unrunnable:** ``{key}``
        Keys that we are unable to run
    * **retrictions:** ``{key: {hostnames}}``:
        A set of hostnames per key of where that key can be run.  Usually this
        is empty unless a key has been specifically restricted to only run on
        certain hosts.  These restrictions don't include a worker port.  Any
        worker on that hostname is deemed valid.
    * **loose_retrictions:** ``{key}``:
        Set of keys for which we are allow to violate restrictions (see above)
        if not valid workers are present.
    * **keyorder:** ``{key: tuple}``:
        A score per key that determines its priority
    * **scheduler_queues:** ``[Queues]``:
        A list of Tornado Queues from which we accept stimuli
    * **report_queues:** ``[Queues]``:
        A list of Tornado Queues on which we report results
    * **streams:** ``[IOStreams]``:
        A list of Tornado IOStreams from which we both accept stimuli and
        report results
    * **coroutines:** ``[Futures]``:
        A list of active futures that control operation
    *  **exceptions:** ``{key: Exception}``:
        A dict mapping keys to remote exceptions
    *  **tracebacks:** ``{key: list}``:
        A dict mapping keys to remote tracebacks stored as a list of strings
    *  **exceptions_blame:** ``{key: key}``:
        A dict mapping a key to another key on which it depends that has failed
    *  **deleted_keys:** ``{key: {workers}}``
        Locations of workers that have keys that should be deleted
    *  **loop:** ``IOLoop``:
        The running Torando IOLoop
    """
    default_port = 8786

    def __init__(self, center=None, loop=None,
            resource_interval=1, resource_log_size=1000,
            max_buffer_size=MAX_BUFFER_SIZE, delete_interval=500,
            ip=None, services=None, heartbeat_interval=500, **kwargs):
        self.scheduler_queues = [Queue()]
        self.report_queues = []
        self.worker_streams = dict()
        self.streams = dict()
        self.status = None
        self.coroutines = []
        self.ip = ip or get_ip()
        self.delete_interval = delete_interval
        self.heartbeat_interval = heartbeat_interval
        self._worker_coroutines = []

        self.tasks = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.generation = 0
        self.has_what = defaultdict(set)
        self.released = set()
        self.keyorder = dict()
        self.nbytes = dict()
        self.ncores = dict()
        self.worker_info = defaultdict(dict)
        self.host_info = dict()
        self.aliases = dict()
        self.processing = dict()
        self.occupancy = dict()
        self.task_duration = {prefix: 0.00001 for prefix in fast_task_prefixes}
        self.restrictions = dict()
        self.loose_restrictions = set()
        self.stacks = dict()
        self.waiting = dict()
        self.waiting_data = dict()
        self.ready = deque()
        self.unrunnable = set()
        self.idle = set()
        self.maybe_ready = set()
        self.who_has = defaultdict(set)
        self.deleted_keys = defaultdict(set)
        self.who_wants = defaultdict(set)
        self.wants_what = defaultdict(set)

        self.exceptions = dict()
        self.tracebacks = dict()
        self.exceptions_blame = dict()

        self.loop = loop or IOLoop.current()
        self.io_loop = self.loop

        self.resource_interval = resource_interval
        self.resource_log_size = resource_log_size

        self.plugins = []

        self.compute_handlers = {'update-graph': self.update_graph,
                                 'update-data': self.update_data,
                                 'missing-data': self.mark_missing_data,
                                 'client-releases-keys': self.client_releases_keys,
                                 'restart': self.restart}

        self.handlers = {'register-client': self.add_client,
                         'scatter': self.scatter,
                         'register': self.add_worker,
                         'unregister': self.remove_worker,
                         'gather': self.gather,
                         'cancel': self.cancel,
                         'feed': self.feed,
                         'terminate': self.close,
                         'broadcast': self.broadcast,
                         'ncores': self.get_ncores,
                         'has_what': self.get_has_what,
                         'who_has': self.get_who_has,
                         'nbytes': self.get_nbytes,
                         'add_keys': self.add_keys,
                         'rebalance': self.rebalance,
                         'replicate': self.replicate}

        self.services = {}
        for k, v in (services or {}).items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            self.services[k] = v(self)
            self.services[k].listen(port)

        super(Scheduler, self).__init__(handlers=self.handlers,
                max_buffer_size=max_buffer_size, **kwargs)

    def __del__(self):
        self.close_streams()

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

    @property
    def address_tuple(self):
        return (self.ip, self.port)

    def add_keys(self, stream=None, address=None, keys=()):
        address = coerce_to_address(address)
        self.has_what[address].update(keys)
        for key in keys:
            self.who_has[key].add(address)
        return 'OK'

    def identity(self, stream):
        """ Basic information about ourselves and our cluster """
        d = {'type': type(self).__name__,
             'id': str(self.id),
             'workers': list(self.ncores),
             'services': {key: v.port for (key, v) in self.services.items()},
             'workers': dict(self.worker_info)}
        return d

    def put(self, msg):
        """ Place a message into the scheduler's queue """
        return self.scheduler_queues[0].put_nowait(msg)

    def start(self, port=8786, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        collections = [self.tasks, self.dependencies, self.dependents,
                self.waiting, self.waiting_data, self.released, self.keyorder,
                self.nbytes, self.restrictions, self.loose_restrictions,
                self.ready, self.who_wants, self.wants_what]
        for collection in collections:
            collection.clear()

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        self._delete_periodic_callback = \
                PeriodicCallback(callback=self.clear_data_from_workers,
                                 callback_time=self.delete_interval,
                                 io_loop=self.loop)
        self._delete_periodic_callback.start()

        if start_queues:
            self.handle_queues(self.scheduler_queues[0], None)

        for cor in self.coroutines:
            if cor.done():
                exc = cor.exception()
                if exc:
                    raise exc

        if self.status != 'running':
            self.listen(port)

            self.status = 'running'
            logger.info("Scheduler at: %20s:%s", self.ip, self.port)
            for k, v in self.services.items():
                logger.info("%9s at: %20s:%s", k, self.ip, v.port)

        return self.finished()

    @gen.coroutine
    def finished(self):
        """ Wait until all coroutines have ceased """
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    def close_streams(self):
        for r in self._rpcs.values():
            r.close_streams()
        for stream in self.streams.values():
            stream.stream.close()

    @gen.coroutine
    def close(self, stream=None):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        yield self.cleanup()
        yield self.finished()
        self.close_streams()
        self.status = 'closed'
        self.stop()

    @gen.coroutine
    def cleanup(self):
        """ Clean up queues and coroutines, prepare to stop """
        if self.status == 'closing':
            raise gen.Return()

        self.status = 'closing'
        logger.debug("Cleaning up coroutines")

        for w, bstream in self.worker_streams.items():
            with ignoring(AttributeError):
                yield bstream.close(ignore_closed=True)

        for s in self.scheduler_queues[1:]:
            s.put_nowait({'op': 'close-stream'})

        for q in self.report_queues:
            q.put_nowait({'op': 'close'})

    def mark_ready_to_run(self, key):
        """
        Mark a task as ready to run.

        If the task should be assigned to a worker then make that determination
        and assign appropriately.  Otherwise place task in the ready queue.

        Trigger appropriate workers if idle.

        See Also
        --------
        decide_worker
        Scheduler.ensure_occupied
        """
        logger.debug("Mark %s ready to run", key)
        if key in self.exceptions_blame:
            logger.debug("Don't mark ready, task has already failed")
            return

        if key in self.waiting:
            assert not self.waiting[key]
            del self.waiting[key]

        if self.dependencies.get(key, None) or key in self.restrictions:
            new_worker = decide_worker(self.dependencies, self.stacks,
                    self.processing, self.who_has, self.has_what,
                    self.restrictions, self.loose_restrictions, self.nbytes,
                    key)
            if not new_worker:
                self.unrunnable.add(key)
            else:
                self.stacks[new_worker].append(key)
                self.maybe_ready.add(new_worker)
                # self.ensure_occupied(new_worker)
        else:
            self.ready.appendleft(key)
            # self.ensure_idle_ready()

    def ensure_idle_ready(self):
        """ Run ready tasks on idle workers

        **Work stealing policy**

        If some workers are idle but not others, if there are no globally ready
        tasks, and if there are tasks in worker stacks, then we start to pull
        preferred tasks from overburdened workers and deploy them back into the
        global pool in the following manner.

        We determine the number of tasks to reclaim as the number of all tasks
        in all stacks times the fraction of idle workers to all workers.
        We sort the stacks by size and walk through them, reclaiming half of
        each stack until we have enough task to fill the global pool.
        We are careful not to reclaim tasks that are restricted to run on
        certain workers.
        """
        for worker in self.maybe_ready:
            self.ensure_occupied_stacks(worker)
        self.maybe_ready.clear()

        if self.idle and self.ready:
            if len(self.ready) < len(self.idle):
                def keyfunc(w):
                    return (-len(self.stacks[w]) - len(self.processing[w]),
                            -len(self.has_what.get(w, ())))
                for worker in topk(len(self.ready), self.idle, key=keyfunc):
                    self.ensure_occupied_ready_count(worker, count=1)
            else:
                # Fill up empty cores
                workers = list(self.idle)
                free_cores = [self.ncores[w] - len(self.processing[w])
                              for w in workers]

                workers2 = []  # Clean out workers that *are* actually full
                free_cores2 = []
                for w, fs in zip(workers, free_cores):
                    if fs > 0:
                        workers2.append(w)
                        free_cores2.append(fs)

                if workers2:
                    n = min(sum(free_cores2), len(self.ready))
                    counts = divide_n_among_bins(n, free_cores2)
                    for worker, count in zip(workers2, counts):
                        self.ensure_occupied_ready_count(worker, count=count)

                # Fill up unsaturated cores by time
                workers = list(self.idle)
                latency = 5e-3
                free_time = [latency * self.ncores[w] - self.occupancy[w]
                              for w in workers]
                workers2 = []  # Clean out workers that *are* actually full
                free_time2 = []
                for w, fs in zip(workers, free_time):
                    if fs > 0:
                        workers2.append(w)
                        free_time2.append(fs)
                total_free_time = sum(free_time2)
                if workers2 and total_free_time > 0:
                    tasks = []
                    while self.ready and total_free_time > 0:
                        task = self.ready.pop()
                        total_free_time -= self.task_duration.get(key_split(task), 1)
                        tasks.append(task)

                    self.ready.extend(tasks[::-1])

                    counts = divide_n_among_bins(len(tasks), free_time2)
                    for worker, count in zip(workers2, counts):
                        self.ensure_occupied_ready_count(worker, count=count)

        # Work stealing
        if 0 < len(self.idle) < len(self.ncores) and not self.ready:
            n = sum(map(len, self.stacks)) * len(self.idle) / len(self.ncores)

            if not n:
                return
            stacks = sorted([(w, self.stacks[w]) for w in self.ncores
                                                  if w not in self.idle],
                            key=lambda kv: len(kv[1]), reverse=True)

            for w, stack in stacks:
                k = min(len(stack) // 2, len(stack) - self.ncores[w])
                if k <= 0:
                    continue
                tasks = stack[:k]
                good = [t for t in tasks if t not in self.restrictions]
                bad = [t for t in tasks if t in self.restrictions]
                del self.stacks[w][:k]
                self.stacks[w][:0] = bad
                self.ready.extend(good)

                n -= len(good)

                if n <= 0:
                    break

    def mark_key_in_memory(self, key, workers=None, type=None):
        """ Mark that a key now lives in distributed memory """
        logger.debug("Mark %s in memory", key)
        if workers is None:
            workers = self.who_has[key]
        for worker in workers:
            self.who_has[key].add(worker)
            self.has_what[worker].add(key)
            try:
                self.occupancy[worker] -= self.processing[worker].pop(key)
            except KeyError:
                pass

        deps = self.dependents.get(key, [])
        if len(deps) > 1:
            deps = sorted(deps, key=self.keyorder.get, reverse=True)

        for dep in deps:
            if dep in self.waiting:
                s = self.waiting[dep]
                try:
                    s.remove(key)
                except KeyError:
                    pass
                if not s:  # new task ready to run
                    self.mark_ready_to_run(dep)

        self.release_live_dependencies(key)

        msg = {'op': 'key-in-memory',
               'key': key,
               'workers': list(workers)}
        if type is not None:
            msg['type'] = type
        self.report(msg)

        for plugin in self.plugins:
            try:
                plugin.mark_key_in_memory(self, key, workers=workers, type=type)
            except Exception as e:
                logger.exception(e)
        # self.validate(allow_overlap=True, allow_bad_stacks=True)

    def release_live_dependencies(self, key):
        """ We no longer need to keep data in memory to compute this

        This occurs after we've computed it or after we've forgotten it
        """
        for dep in self.dependencies.get(key, []):
            if dep in self.waiting_data:
                s = self.waiting_data[dep]
                if key in s:
                    s.remove(key)
                if not s and dep and dep not in self.who_wants:
                    self.delete_data(keys=[dep])

    def ensure_occupied(self, worker):
        self.ensure_occupied_stacks(worker)
        self.ensure_occupied_ready(worker)

    def ensure_occupied_stacks(self, worker):
        """ Send tasks to worker while it has tasks and free cores

        These tasks may come from the worker's own stacks or from the global
        ready deque.

        We update the idle workers set appropriately.
        """
        stack = self.stacks[worker]
        latency = 5e-3

        while (stack and
               (self.ncores[worker] > len(self.processing[worker]) or
                self.occupancy[worker] < latency * self.ncores[worker])):
            key = stack.pop()
            if key not in self.tasks:
                continue
            if self.who_has.get(key):
                continue
            duration = self.task_duration.get(key_split(key), latency*100)
            self.processing[worker][key] = duration
            self.occupancy[worker] += duration
            logger.debug("Send job to worker: %s, %s", worker, key)
            try:
                self.send_task_to_worker(worker, key)
            except StreamClosedError:
                self.remove_worker(worker)
                return

        self._check_idle(worker)

    def ensure_occupied_ready_count(self, worker, count):
        latency = 5e-3
        for i in range(count):
            try:
                key = self.ready.pop()
            except KeyError:
                break
            if key not in self.tasks:
                continue
            if self.who_has.get(key):
                continue
            duration = self.task_duration.get(key_split(key), latency*100)
            self.processing[worker][key] = duration
            self.occupancy[worker] += duration
            logger.debug("Send job to worker: %s, %s", worker, key)
            try:
                self.send_task_to_worker(worker, key)
            except StreamClosedError:
                self.remove_worker(worker)
                return

        self._check_idle(worker)

    def ensure_occupied_ready(self, worker):
        latency = 5e-3

        while (self.ready and
               (self.ncores[worker] > len(self.processing[worker]) or
                self.occupancy[worker] < latency * self.ncores[worker])):
            key = self.ready.pop()
            if key not in self.tasks:
                continue
            if self.who_has.get(key):
                continue
            duration = self.task_duration.get(key_split(key), latency*100)
            self.processing[worker][key] = duration
            self.occupancy[worker] += duration
            logger.debug("Send job to worker: %s, %s", worker, key)
            try:
                self.send_task_to_worker(worker, key)
            except StreamClosedError:
                self.remove_worker(worker)
                return

        self._check_idle(worker)

    def _check_idle(self, worker, latency=5e-3):
        if (len(self.processing[worker]) < self.ncores[worker] or
            self.occupancy[worker] < latency * self.ncores[worker]):
            self.idle.add(worker)
        elif worker in self.idle:
            self.idle.remove(worker)

    def update_data(self, who_has=None, nbytes=None, client=None):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        who_has = {k: [self.coerce_address(vv) for vv in v]
                   for k, v in who_has.items()}
        logger.debug("Update data %s", who_has)
        for key, workers in who_has.items():
            self.mark_key_in_memory(key, workers)

        self.nbytes.update(nbytes)

        for key in who_has:
            if key not in self.dependents:
                self.dependents[key] = set()
            if key not in self.dependencies:
                self.dependencies[key] = set()

        if client:
            self.client_wants_keys(keys=list(who_has), client=client)

    def mark_task_erred(self, key=None, worker=None,
                        exception=None, traceback=None, **kwargs):
        """ Mark that a task has erred on a particular worker

        See Also
        --------
        Scheduler.mark_failed
        """
        if worker in self.processing and key in self.processing[worker]:
            self.occupancy[worker] -= self.processing[worker].pop(key)
            self.exceptions[key] = exception
            self.tracebacks[key] = traceback
            self.mark_failed(key, key)
            self.maybe_ready.add(worker)
            # self.ensure_occupied(worker)
            for plugin in self.plugins[:]:
                try:
                    plugin.task_erred(self, key=key, worker=worker,
                            exception=exception, traceback=traceback, **kwargs)
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

        self.release_live_dependencies(key)

        self.released.add(key)
        for dep in self.dependents[key]:
            self.mark_failed(dep, failing_key)

    def mark_task_finished(self, key=None, worker=None, nbytes=None, type=None,
            compute_start=None, compute_stop=None, transfer_start=None,
            transfer_stop=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Mark task as finished %s, %s", key, worker)
        if worker in self.processing and key in self.processing[worker]:
            self.nbytes[key] = nbytes
            self.mark_key_in_memory(key, [worker], type=type)
            self.maybe_ready.add(worker)

            # Update average task duration for worker
            info = self.worker_info[worker]
            ks = key_split(key)
            gap = (transfer_start or compute_start) - info.get('last-task', 0)
            old_duration = self.task_duration.get(ks, 0)
            new_duration = compute_stop - compute_start
            if (not old_duration or
                gap > max(10e-3, info.get('latency', 0), old_duration)):
                avg_duration = new_duration
            else:
                avg_duration = (0.5 * old_duration
                              + 0.5 * new_duration)

            self.task_duration[ks] = avg_duration
            info['last-task'] = compute_stop
            for plugin in self.plugins[:]:
                try:
                    plugin.task_finished(self, key=key, worker=worker,
                            nbytes=nbytes, type=type,
                            compute_start=compute_start,
                            compute_stop=compute_stop,
                            transfer_start=transfer_start,
                            transfer_stop=transfer_stop,
                            **kwargs)


                except Exception as e:
                    logger.exception(e)
        else:
            logger.debug("Key not found in processing, %s, %s, %s",
                         key, worker, self.processing[worker])
            self.maybe_ready.add(worker)

    def recover_missing(self, key):
        """ Recover a recently lost piece of data

        This assumes that we've already removed this key from who_has/has_what.
        """
        if key in self.released:
            return
        if key in self.who_has:
            return
        if key not in self.tasks:
            logger.warn("Lost irrecoverable data %s", key)
            return

        self.released.add(key)
        self.ensure_in_play(key)

        for dep in self.dependents[key]:
            if dep in self.released:
                continue
            if dep in self.who_has:
                continue
            if dep in self.waiting:
                self.waiting[dep].add(key)
            else:
                self.waiting[dep] = {key}

    def mark_missing_data(self, keys=None, key=None, worker=None, **kwargs):
        """ Mark that certain keys have gone missing.  Recover.

        See Also
        --------
        recover_missing
        """
        missing = keys
        if key and worker:
            try:
                self.occupancy[worker] -= self.processing[worker].pop(key)
            except KeyError:
                logger.info("Tried to remove %s from %s, but it wasn't there",
                             key, worker)

        missing = set(missing)
        logger.debug("Recovering missing data: %s", missing)
        for k in missing:
            with ignoring(KeyError):
                workers = self.who_has.pop(k)
                for worker in workers:
                    self.has_what[worker].remove(k)
            self.recover_missing(k)

        if worker:
            self.ensure_occupied(worker)

        # self.validate(allow_overlap=True, allow_bad_stacks=True)

    def log_state(self, msg=''):
        """ Log current full state of the scheduler """
        logger.debug("Runtime State: %s", msg)
        logger.debug('\n\nwaiting: %s\n\nstacks: %s\n\nprocessing: %s\n\n',
                     self.waiting, self.stacks, self.processing)

    def _restart_heartbeat(self, host):
        """ Restart heartbeat periodic callback from among active ports """
        d = self.host_info[host]

        if 'heartbeat-port' in d and d['heartbeat-port'] in d['ports']:
            return

        if 'heartbeat' in d:
            d['heartbeat'].stop()

        port = first(d['ports'])

        pc = PeriodicCallback(callback=lambda: self.heartbeat(host),
                              callback_time=self.heartbeat_interval,
                              io_loop=self.loop)
        self.loop.add_callback(pc.start)
        d['heartbeat'] = pc
        d['heartbeat-port'] = port

    def remove_worker(self, stream=None, address=None):
        """ Mark that a worker no longer seems responsive

        See Also
        --------
        Scheduler.recover_missing
        """
        with log_errors():
            address = self.coerce_address(address)
            logger.debug("Remove worker %s", address)
            if address not in self.processing:
                return
            with ignoring(AttributeError):
                self.worker_streams[address].stream.close()

            host, port = address.split(':')

            self.host_info[host]['cores'] -= self.ncores[address]
            self.host_info[host]['ports'].remove(port)

            if not self.host_info[host]['ports']:
                del self.host_info[host]
            else:
                self._restart_heartbeat(host)

            del self.worker_streams[address]
            del self.ncores[address]
            del self.aliases[self.worker_info[address]['name']]
            del self.worker_info[address]
            if address in self.idle:
                self.idle.remove(address)

            in_flight = set(self.stacks.pop(address))
            in_flight |= set(self.processing.pop(address))
            del self.occupancy[address]
            in_flight = {k for k in in_flight if k in self.tasks}
            missing = set()

            for key in self.has_what.pop(address):
                s = self.who_has[key]
                s.remove(address)
                if not s:
                    self.who_has.pop(key)
                    self.report({'op': 'lost-data', 'key': key})
                    missing.add(key)
                    for plugin in self.plugins:
                        try:
                            plugin.lost_data(self, key=key)
                        except Exception as e:
                            logger.exception(e)


            for key in missing:
                self.recover_missing(key)
            for key in in_flight:
                self.released.add(key)
                self.ensure_in_play(key)

            if not self.stacks:
                logger.critical("Lost all workers")

            self.ensure_idle_ready()

            return 'OK'

    def add_worker(self, stream=None, address=None, keys=(), ncores=None,
                   name=None, coerce_address=True, **info):
        with log_errors():
            if coerce_address:
                address = self.coerce_address(address)
            name = name or address
            if name in self.aliases:
                return 'name taken, %s' % name

            if coerce_address:
                host, port = self.coerce_address(address).split(':')
                if host not in self.host_info:
                    self.host_info[host] = {'ports': set(), 'cores': 0}

                self.host_info[host]['ports'].add(port)
                self.host_info[host]['cores'] += ncores
                self.loop.add_callback(self.heartbeat, host)
                self._restart_heartbeat(host)

            self.ncores[address] = ncores

            self.aliases[name] = address

            info['name'] = name
            self.worker_info[address] = info

            if address not in self.processing:
                self.has_what[address] = set()
                self.processing[address] = dict()
                self.occupancy[address] = 0
                self.stacks[address] = []

            for key in keys:
                self.mark_key_in_memory(key, [address])

            self.worker_streams[address] = BatchedSend(interval=2, loop=self.loop)
            self._worker_coroutines.append(self.worker_stream(address))

            if self.ncores[address] > len(self.processing[address]):
                self.idle.add(address)

            self.ensure_occupied(address)

            logger.info("Register %s", str(address))
            return 'OK'

    def ensure_in_play(self, key):
        """ Ensure that a key is on track to enter memory in the future

        This will only act on keys currently in self.released.
        """
        stack = [key]
        visited = set()
        stack2 = [key]
        while stack:
            k = stack.pop()
            if k not in self.released or k in visited:
                continue
            visited.add(k)
            stack.extend(self.dependencies.get(k, []))
            stack2.extend(self.dependencies.get(k, []))

        visited.clear()
        while stack2:
            k = stack2.pop()
            if k not in self.released or k in visited:
                continue
            visited.add(k)

            for dep in self.dependencies[k]:
                try:
                    self.waiting_data[dep].add(k)
                except KeyError:
                    self.waiting_data[dep] = {k}

            waiting = {dep for dep in self.dependencies[k]
                        if not self.who_has.get(dep)}
            self.released.remove(k)

            if waiting:
                self.waiting[k] = waiting
            else:
                self.mark_ready_to_run(k)

            if k not in self.waiting_data:
                self.waiting_data[k] = set()

    def update_graph(self, client=None, tasks=None, keys=None,
                     dependencies=None, restrictions=None,
                     loose_restrictions=None):
        """ Add new computations to the internal dask graph

        This happens whenever the Executor calls submit, map, get, or compute.
        """
        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]
            if k in self.tasks:
                del tasks[k]

        original_keys = keys
        keys = set(keys)
        for k in keys:
            self.who_wants[k].add(client)
            self.wants_what[client].add(k)

        n = 0
        while len(tasks) != n:  # walk thorough new tasks, cancel any bad deps
            n = len(tasks)
            for k, deps in list(dependencies.items()):
                if any(dep not in self.dependencies and dep not in tasks
                        for dep in deps):  # bad key
                    logger.info('User asked for computation on lost data, %s', k)
                    del tasks[k]
                    del dependencies[k]
                    if k in keys:
                        keys.remove(k)
                    self.report({'op': 'cancelled-key', 'key': k})
                    self.client_releases_keys(keys=[k], client=client)

        stack = list(keys)
        touched = set()
        while stack:
            k = stack.pop()
            if k in self.tasks:
                continue
            touched.add(k)
            if k not in self.tasks and k in tasks:
                self.tasks[k] = tasks[k]
                self.dependencies[k] = set(dependencies.get(k, ()))
                self.released.add(k)
                for dep in self.dependencies[k]:
                    if dep not in self.dependents:
                        self.dependents[dep] = set()
                    self.dependents[dep].add(k)
                if k not in self.dependents:
                    self.dependents[k] = set()

            stack.extend(self.dependencies[k])

        new_keyorder = order(tasks)  # TODO: define order wrt old graph
        self.generation += 1  # older graph generations take precedence
        for key in set(new_keyorder) & touched:
            if key not in self.keyorder:
                self.keyorder[key] = (self.generation, new_keyorder[key]) # prefer old

        if restrictions:
            restrictions = {k: set(map(self.coerce_address, v))
                            for k, v in restrictions.items()}
            self.restrictions.update(restrictions)

            if loose_restrictions:
                self.loose_restrictions |= set(loose_restrictions)

        for key in unique(original_keys):
            if key in keys:
                self.ensure_in_play(key)

        for key in touched | keys:
            for dep in self.dependencies[key]:
                if dep in self.exceptions_blame:
                    self.mark_failed(key, self.exceptions_blame[dep])

        for key in keys:
            if self.who_has.get(key):
                self.mark_key_in_memory(key)

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(self, client=client, tasks=tasks,
                        keys=keys, restrictions=restrictions or {},
                        dependencies=dependencies,
                        loose_restrictions=loose_restrictions)
            except Exception as e:
                logger.exception(e)

        self.ensure_idle_ready()
        # self.validate()

    def client_releases_keys(self, keys=None, client=None):
        for k in list(keys):
            try:
                self.wants_what[client].remove(k)
            except KeyError:
                pass
            try:
                self.who_wants[k].remove(client)
            except KeyError:
                pass
            if not self.who_wants[k]:
                del self.who_wants[k]
                self.release_held_data([k])
                logger.debug("Delete who_wants[%s]", k)

    def client_wants_keys(self, keys=None, client=None):
        for k in keys:
            self.who_wants[k].add(client)
            self.wants_what[client].add(k)

    def release_held_data(self, keys=None):
        """ Mark that a key is no longer externally required to be in memory """
        keys = set(keys)
        if keys:
            logger.debug("Release keys: %s", keys)
            keys2 = {k for k in keys if not self.waiting_data.get(k)}
            if keys2:
                self.delete_data(keys=keys2)  # async

        changed = True
        keys2 = keys.copy()
        while changed:
            changed = False
            for key in list(keys2):
                if key in self.tasks and not self.dependents.get(key):
                    self.forget(key)
                    keys2.remove(key)
                    changed = True

    def forget(self, key):
        """ Forget a key if no one cares about it

        This removes all knowledge of how to produce a key from the scheduler.
        This is almost exclusively called by release_held_data
        """
        stack = [key]
        while stack:
            key = stack.pop()

            if key not in self.tasks and key not in self.who_has:
                return
            assert not self.dependents[key] and key not in self.who_wants

            if key in self.tasks:
                del self.tasks[key]
                self.release_live_dependencies(key)
                del self.dependents[key]
                for dep in self.dependencies[key]:
                    s = self.dependents[dep]
                    s.remove(key)
                    if not s and dep not in self.who_wants:
                        assert dep is not key
                        stack.append(dep)
                del self.dependencies[key]
                if key in self.restrictions:
                    del self.restrictions[key]
                if key in self.loose_restrictions:
                    self.loose_restrictions.remove(key)
                del self.keyorder[key]
                if key in self.exceptions:
                    del self.exceptions[key]
                if key in self.exceptions_blame:
                    del self.exceptions_blame[key]
                if key in self.released:
                    self.released.remove(key)
                if key in self.waiting:
                    del self.waiting[key]
                if key in self.waiting_data:
                    del self.waiting_data[key]

            if key in self.who_has:
                self.delete_data(keys=[key])

            if key in self.nbytes:
                del self.nbytes[key]

            for plugin in self.plugins:
                try:
                    plugin.forget(self, key)
                except Exception as e:
                    logger.exception(e)

    def cancel_key(self, key, client, retries=5):
        if key not in self.who_wants:  # no key yet, lets try again in 500ms
            if retries:
                self.loop.add_future(gen.sleep(0.2),
                        lambda _: self.cancel_key(key, client, retries - 1))
            return
        if self.who_wants[key] == {client}:  # no one else wants this key
            for dep in list(self.dependents[key]):
                self.cancel_key(dep, client)
        logger.debug("Scheduler cancels key %s", key)
        self.report({'op': 'cancelled-key', 'key': key})
        self.client_releases_keys(keys=[key], client=client)

    def cancel(self, stream, keys=None, client=None):
        """ Stop execution on a list of keys """
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        for key in keys:
            self.cancel_key(key, client)

    def report(self, msg):
        """ Publish updates to all listening Queues and Streams """
        for q in self.report_queues:
            q.put_nowait(msg)
        if 'key' in msg:
            streams = {self.streams[c]
                       for c in self.who_wants.get(msg['key'], ())
                       if c in self.streams}
        else:
            streams = self.streams.values()
        for s in streams:
            try:
                s.send(msg)
                logger.debug("Scheduler sends message to client %s", msg)
            except StreamClosedError:
                logger.critical("Tried writing to closed stream: %s", msg)

    def add_plugin(self, plugin):
        """ Add external plugin to scheduler

        See http://http://distributed.readthedocs.io/en/latest/plugins.html
        """
        self.plugins.append(plugin)

    def remove_plugin(self, plugin):
        self.plugins.remove(plugin)

    def handle_queues(self, scheduler_queue, report_queue):
        """ Register new control and report queues to the Scheduler """
        self.scheduler_queues.append(scheduler_queue)
        if report_queue:
            self.report_queues.append(report_queue)
        future = self.handle_messages(scheduler_queue, report_queue)
        self.coroutines.append(future)
        return future

    @gen.coroutine
    def add_client(self, stream, client=None):
        """ Listen to messages from an IOStream """
        logger.info("Connection to %s, %s", type(self).__name__, client)
        bstream = BatchedSend(interval=2, loop=self.loop)
        bstream.start(stream)
        self.streams[client] = bstream

        try:
            yield self.handle_messages(stream, bstream, client=client)
        finally:
            if not stream.closed():
                bstream.send({'op': 'stream-closed'})
                yield bstream.close(ignore_closed=True)
            del self.streams[client]
            logger.info("Close connection to %s, %s", type(self).__name__,
                        client)

    def remove_client(self, client=None):
        logger.info("Remove client %s", client)
        self.client_releases_keys(self.wants_what.get(client, ()), client)
        with ignoring(KeyError):
            del self.wants_what[client]

    @gen.coroutine
    def handle_messages(self, in_queue, report, client=None):
        """ Master coroutine.  Handles inbound messages.

        This runs once per Queue or Stream.
        """
        with log_errors():
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
            elif isinstance(report, BatchedSend):
                put = report.send
            else:
                put = lambda msg: None
            put({'op': 'stream-start'})

            breakout = False

            while True:
                try:
                    msgs = yield next_message()  # in_queue.get()
                except (StreamClosedError, AssertionError, GeneratorExit):
                    break
                except Exception as e:
                    logger.exception(e)
                    put(error_message(e, status='scheduler-error'))
                    continue

                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    logger.debug("scheduler receives message %s", msg)
                    try:
                        op = msg.pop('op')
                    except Exception as e:
                        logger.exception(e)
                        put(error_message(e, status='scheduler-error'))

                    if op == 'close-stream':
                        breakout = True
                        break
                    elif op == 'close':
                        breakout = True
                        self.close()
                        break
                    elif op in self.compute_handlers:
                        try:
                            result = self.compute_handlers[op](**msg)
                            if isinstance(result, gen.Future):
                                yield result
                        except Exception as e:
                            logger.exception(e)
                            raise
                    else:
                        logger.warn("Bad message: op=%s, %s", op, msg, exc_info=True)

                    if op == 'close':
                        breakout = True
                        break
                if breakout:
                    break

            self.remove_client(client=client)
            logger.debug('Finished scheduling coroutine')

    def send_task_to_worker(self, ident, key):
        msg = {'op': 'compute-task',
               'key': key}

        deps = self.dependencies[key]
        if deps:
            msg['who_has'] = {dep: tuple(self.who_has[dep]) for dep in deps}

        task = self.tasks[key]
        if type(task) is dict:
            msg.update(task)
        else:
            msg['task'] = task

        self.worker_streams[ident].send(msg)

    def correct_time_delay(self, worker, msg):
        """ Apply offset time delay in message times

        Operates in place
        """
        if 'time-delay' in self.host_info[worker]:
            delay = self.host_info[worker]['time-delay']
            for key in ['transfer_start', 'transfer_stop', 'time',
                        'compute_start', 'compute_stop']:
                if key in msg:
                    msg[key] += delay

    @gen.coroutine
    def worker_stream(self, ident):
        yield gen.sleep(0)
        ip, port = coerce_to_address(ident, out=tuple)
        stream = yield connect(ip, port)
        yield write(stream, {'op': 'compute-stream'})
        self.worker_streams[ident].start(stream)
        logger.info("Starting worker compute stream, %s", ident)

        try:
            while True:
                msgs = yield read(stream)
                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    logger.debug("Compute response from worker %s, %s",
                                 ident, msg)

                    if msg == 'OK':  # from close
                        break

                    self.correct_time_delay(ip, msg)

                    if msg['status'] == 'OK':
                        self.mark_task_finished(worker=ident, **msg)
                    elif msg['status'] == 'error':
                        self.mark_task_erred(worker=ident, **msg)
                    elif msg['status'] == 'missing-data':
                        self.mark_missing_data(worker=ident, **msg)
                    else:
                        logger.warn("Unknown message type, %s, %s", msg['status'],
                                msg)

                self.ensure_idle_ready()
        except (StreamClosedError, IOError, OSError):
            logger.info("Worker failed from closed stream: %s", ident)
        finally:
            stream.close()
            self.remove_worker(address=ident)

    @gen.coroutine
    def clear_data_from_workers(self):
        """ This is intended to be run periodically,

        The ``self._delete_periodic_callback`` attribute holds a PeriodicCallback
        that runs this every ``self.delete_interval`` milliseconds``.
        """
        if self.deleted_keys:
            d = self.deleted_keys.copy()
            self.deleted_keys.clear()

            coroutines = [self.rpc(addr=worker).delete_data(
                                   keys=list(keys - self.has_what.get(worker,
                                                                      set())),
                                   report=False)
                          for worker, keys in d.items()]
            for worker, keys in d.items():
                logger.debug("Remove %d keys from worker %s", len(keys), worker)
            yield ignore_exceptions(coroutines, socket.error, StreamClosedError)

        raise Return('OK')

    def delete_data(self, stream=None, keys=None):
        def trigger_plugins(key):
            for plugin in self.plugins:
                try:
                    plugin.delete(self, key)
                except Exception as e:
                    logger.exception(e)
            self.released.add(key)

        for key in keys:
            if key in self.who_has:
                for worker in self.who_has.pop(key):
                    self.has_what[worker].remove(key)
                    self.deleted_keys[worker].add(key)
                trigger_plugins(key)
            elif key in self.ready:  # O(n), though infrequent
                self.ready.remove(key)
                trigger_plugins(key)

            if key in self.waiting_data:
                del self.waiting_data[key]


    @gen.coroutine
    def heartbeat(self, host):
        port = self.host_info[host]['heartbeat-port']
        start_time = time()
        start = default_timer()

        try:
            d = yield gen.with_timeout(timedelta(seconds=1),
                                       self.rpc(ip=host, port=port).health(),
                                       io_loop=self.loop)
        except gen.TimeoutError:
            logger.warn("Heartbeat failed for %s", host)
        except Exception as e:
            logger.exception(e)

        end = default_timer()
        end_time = time()
        last_seen = datetime.now()

        d['latency'] = end - start
        d['last-seen'] = last_seen

        delay = (end_time + start_time) / 2 - d['time']
        try:
            avg_delay = self.host_info[host]['time-delay']
            avg_delay = (0.90 * avg_delay + 0.10 * delay)
        except KeyError:
            avg_delay = delay

        d['time-delay'] = delay

        self.host_info[host].update(d)

    @gen.coroutine
    def scatter(self, stream=None, data=None, workers=None, client=None,
            broadcast=False):
        """ Send data out to workers """
        if not self.ncores:
            raise ValueError("No workers yet found.")
        if workers is not None:
            workers = [self.coerce_address(w) for w in workers]
        ncores = workers if workers is not None else self.ncores
        keys, who_has, nbytes = yield scatter_to_workers(ncores, data,
                                                         report=False,
                                                         serialize=False)

        self.update_data(who_has=who_has, nbytes=nbytes)
        self.client_wants_keys(keys=keys, client=client)

        if broadcast:
            if broadcast == True:
                n = len(ncores)
            else:
                n = broadcast
            yield self.replicate(keys=keys, workers=workers, n=n)

        raise gen.Return(keys)

    @gen.coroutine
    def gather(self, stream=None, keys=None):
        """ Collect data in from workers """
        keys = list(keys)
        who_has = {key: self.who_has[key] for key in keys}

        try:
            data = yield gather_from_workers(who_has, deserialize=False,
                    rpc=self.rpc, close=False)
            result = {'status': 'OK', 'data': data}
        except KeyError as e:
            logger.debug("Couldn't gather keys %s", e)
            result = {'status': 'error', 'keys': e.args}

        raise gen.Return(result)

    @gen.coroutine
    def restart(self):
        """ Restart all workers.  Reset local state """
        logger.debug("Send shutdown signal to workers")

        for q in self.scheduler_queues + self.report_queues:
            clear_queue(q)

        nannies = {addr: d['services']['nanny']
                   for addr, d in self.worker_info.items()}

        for addr in nannies:
            self.remove_worker(address=addr)

        for client, keys in self.wants_what.items():
            self.client_releases_keys(keys=keys, client=client)

        logger.debug("Send kill signal to nannies: %s", nannies)
        nannies = [rpc(ip=worker_address.split(':')[0], port=n_port)
                   for worker_address, n_port in nannies.items()]
        yield All([nanny.kill() for nanny in nannies])
        logger.debug("Received done signal from nannies")

        while self.ncores:
            yield gen.sleep(0.01)

        logger.debug("Workers all removed.  Sending startup signal")

        # All quiet
        resps = yield All([nanny.instantiate(close=True) for nanny in nannies])
        assert all(resp == 'OK' for resp in resps)

        self.start()

        logger.debug("All workers reporting in")

        self.report({'op': 'restart'})
        for plugin in self.plugins[:]:
            try:
                plugin.restart(self)
            except Exception as e:
                logger.exception(e)

    def validate(self, allow_overlap=False, allow_bad_stacks=True):
        validate_state(self.dependencies, self.dependents, self.waiting,
                self.waiting_data, self.ready, self.who_has, self.stacks,
                self.processing, None, self.released, self.who_wants,
                self.wants_what, tasks=self.tasks,
                allow_overlap=allow_overlap, allow_bad_stacks=allow_bad_stacks)
        if not (set(self.ncores) == \
                set(self.has_what) == \
                set(self.stacks) == \
                set(self.processing) == \
                set(self.worker_info) == \
                set(self.worker_streams)):
            raise ValueError("Workers not the same in all collections")

    @gen.coroutine
    def feed(self, stream, function=None, setup=None, teardown=None, interval=1, **kwargs):
        import pickle
        if function:
            function = pickle.loads(function)
        if setup:
            setup = pickle.loads(setup)
        if teardown:
            teardown = pickle.loads(teardown)
        state = setup(self) if setup else None
        if isinstance(state, gen.Future):
            state = yield state
        try:
            while True:
                if state is None:
                    response = function(self)
                else:
                    response = function(self, state)
                yield write(stream, response)
                yield gen.sleep(interval)
        except (OSError, IOError, StreamClosedError):
            if teardown:
                teardown(self, state)

    def get_who_has(self, stream=None, keys=None):
        if keys is not None:
            return {k: list(self.who_has.get(k, [])) for k in keys}
        else:
            return valmap(list, self.who_has)

    def get_has_what(self, stream=None, keys=None):
        if keys is not None:
            keys = map(self.coerce_address, keys)
            return {k: list(self.has_what[k]) for k in keys}
        else:
            return valmap(list, self.has_what)

    def get_ncores(self, stream=None, addresses=None):
        if addresses is not None:
            addresses = map(self.coerce_address, addresses)
            return {k: self.ncores.get(k, None) for k in addresses}
        else:
            return self.ncores

    def get_nbytes(self, stream=None, keys=None, summary=True):
        with log_errors():
            if keys is not None:
                result = {k: self.nbytes[k] for k in keys}
            else:
                result = self.nbytes

            if summary:
                out = defaultdict(lambda: 0)
                for k, v in result.items():
                    out[key_split(k)] += v
                result = out

            return result

    @gen.coroutine
    def broadcast(self, stream=None, msg=None, workers=None):
        """ Broadcast message to workers, return all results """
        if workers is None:
            workers = list(self.ncores)
        results = yield All([send_recv(arg=address, close=True, **msg)
                             for address in workers])
        raise Return(dict(zip(workers, results)))

    def coerce_address(self, addr):
        """ Coerce possible input addresses to canonical form

        Handles lists, strings, bytes, tuples, or aliases
        """
        if isinstance(addr, list):
            addr = tuple(addr)
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, bytes):
            addr = addr.decode()
        if addr in self.aliases:
            addr = self.aliases[addr]
        if isinstance(addr, unicode):
            if ':' in addr:
                addr = tuple(addr.rsplit(':', 1))
            else:
                addr = ensure_ip(addr)
        if isinstance(addr, tuple):
            ip, port = addr
            if PY3 and isinstance(ip, bytes):
                ip = ip.decode()
            ip = ensure_ip(ip)
            port = int(port)
            addr = '%s:%d' % (ip, port)
        return addr

    @gen.coroutine
    def rebalance(self, stream=None, keys=None, workers=None):
        with log_errors():
            keys = set(keys or self.who_has)
            workers = set(workers or self.ncores)

            if not keys.issubset(self.who_has):
                raise Return({'status': 'missing-data',
                              'keys': list(keys - set(self.who_has))})

            workers_by_key = {k: self.who_has[k] & workers for k in keys}
            keys_by_worker = {w: set() for w in workers}
            for k, v in workers_by_key.items():
                for vv in v:
                    keys_by_worker[vv].add(k)

            worker_bytes = {w: sum(self.nbytes[k] for k in v)
                            for w, v in keys_by_worker.items()}
            avg = sum(worker_bytes.values()) / len(worker_bytes)

            sorted_workers = list(map(first, sorted(worker_bytes.items(),
                                              key=second, reverse=True)))

            recipients = iter(reversed(sorted_workers))
            recipient = next(recipients)
            msgs = []  # (sender, recipient, key)
            for sender in sorted_workers[:len(workers) // 2]:
                sender_keys = {k: self.nbytes[k] for k in keys_by_worker[sender]}
                sender_keys = iter(sorted(sender_keys.items(),
                                          key=second, reverse=True))

                try:
                    while worker_bytes[sender] > avg:
                        while worker_bytes[recipient] < avg and worker_bytes[sender] > avg:
                            k, nb = next(sender_keys)
                            if k not in keys_by_worker[recipient]:
                                keys_by_worker[recipient].add(k)
                                # keys_by_worker[sender].remove(k)
                                msgs.append((sender, recipient, k))
                                worker_bytes[sender] -= nb
                                worker_bytes[recipient] += nb
                        if worker_bytes[sender] > avg:
                            recipient = next(recipients)
                except StopIteration:
                    break

            to_recipients = defaultdict(lambda: defaultdict(list))
            to_senders = defaultdict(list)
            for sender, recipient, key in msgs:
                to_recipients[recipient][key].append(sender)
                to_senders[sender].append(key)

            result = yield {r: self.rpc(addr=r).gather(who_has=v)
                            for r, v in to_recipients.items()}

            if not all(r['status'] == 'OK' for r in result.values()):
                raise Return({'status': 'missing-data',
                              'keys': sum([r['keys'] for r in result
                                                     if 'keys' in r], [])})

            for sender, recipient, key in msgs:
                self.who_has[key].add(recipient)
                self.has_what[recipient].add(key)

            result = yield {r: self.rpc(addr=r).delete_data(keys=v, report=False)
                            for r, v in to_senders.items()}

            for sender, recipient, key in msgs:
                self.who_has[key].remove(sender)
                self.has_what[sender].remove(key)

            raise Return({'status': 'OK'})

    def workers_list(self, workers):
        """ List of qualifying workers

        Takes a list of worker addresses or hostnames.
        Returns a list of all worker addresses that match
        """
        if workers is None:
            return list(self.ncores)

        out = set()
        for w in workers:
            if ':' in w:
                out.add(w)
            else:
                out.update({ww for ww in self.ncores if w in ww}) # TODO: quadratic
        return list(out)


    @gen.coroutine
    def replicate(self, stream=None, keys=None, n=None, workers=None, branching_factor=2):
        """ Replicate data throughout cluster

        This performs a tree copy of the data throughout the network
        individually on each piece of data.

        Parameters
        ----------
        keys: Iterable
            list of keys to replicate
        n: int
            Number of replications we expect to see within the cluster
        branching_factor: int, optional
            The number of workers that can copy data in each generation

        See also
        --------
        Scheduler.rebalance
        """
        with log_errors():
            workers = set(self.workers_list(workers))
            if n is None:
                n = len(workers)
            n = min(n, len(workers))
            keys = set(keys)

            if n == 0:
                raise ValueError("Can not use replicate to delete data")

            if not keys.issubset(self.who_has):
                raise Return({'status': 'missing-data',
                              'keys': list(keys - set(self.who_has))})

            # Delete extraneous data
            del_keys = {k: random.sample(self.who_has[k] & workers,
                                         len(self.who_has[k] & workers) - n)
                        for k in keys
                        if len(self.who_has[k] & workers) > n}
            del_workers = {k: v for k, v in reverse_dict(del_keys).items() if v}
            yield [self.rpc(addr=worker).delete_data(keys=list(keys),
                                                     report=False)
                    for worker, keys in del_workers.items()]

            for worker, keys in del_workers.items():
                self.has_what[worker] -= keys
                for key in keys:
                    self.who_has[key].remove(worker)

            keys = {k for k in keys if len(self.who_has[k] & workers) < n}
            # Copy not-yet-filled data
            while keys:
                gathers = defaultdict(dict)
                for k in list(keys):
                    missing = workers - self.who_has[k]
                    count = min(max(n - len(self.who_has[k] & workers), 0),
                                branching_factor * len(self.who_has[k]))
                    if not count:
                        keys.remove(k)
                    else:
                        sample = random.sample(missing, count)
                        for w in sample:
                            gathers[w][k] = list(self.who_has[k])

                results = yield {w: self.rpc(addr=w).gather(who_has=who_has)
                                    for w, who_has in gathers.items()}
                for w, v in results.items():
                    if v['status'] == 'OK':
                        self.add_keys(address=w, keys=list(gathers[w]))


def decide_worker(dependencies, stacks, processing, who_has, has_what, restrictions,
                  loose_restrictions, nbytes, key):
    """ Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> stacks = {'alice:8000': ['z'], 'bob:8000': []}
    >>> processing = {'alice:8000': set(), 'bob:8000': set()}
    >>> who_has = {'a': {'alice:8000'}}
    >>> has_what = {'alice:8000': {'a'}}
    >>> nbytes = {'a': 100}
    >>> restrictions = {}
    >>> loose_restrictions = set()

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, 'b')
    'alice:8000'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice:8000', 'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'a'}}
    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, 'b')
    'bob:8000'

    Optionally provide restrictions of where jobs are allowed to occur

    >>> restrictions = {'b': {'alice', 'charlie'}}
    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, 'b')
    'alice:8000'

    If the task requires data communication, then we choose to minimize the
    number of bytes sent between workers. This takes precedence over worker
    occupancy.

    >>> dependencies = {'c': {'a', 'b'}}
    >>> who_has = {'a': {'alice:8000'}, 'b': {'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'b'}}
    >>> nbytes = {'a': 1, 'b': 1000}
    >>> stacks = {'alice:8000': [], 'bob:8000': []}

    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               {}, set(), nbytes, 'c')
    'bob:8000'
    """
    deps = dependencies[key]
    workers = frequencies([w for dep in deps
                             for w in who_has[dep]])
    if not workers:
        workers = stacks
    if key in restrictions:
        r = restrictions[key]
        workers = {w for w in workers if w in r or w.split(':')[0] in r}  # TODO: nonlinear
        if not workers:
            workers = {w for w in stacks if w in r or w.split(':')[0] in r}
            if not workers:
                if key in loose_restrictions:
                    return decide_worker(dependencies, stacks, processing,
                            who_has, has_what, {}, set(), nbytes, key)
                else:
                    return None
    if not workers or not stacks:
        return None

    if len(workers) == 1:
        return first(workers)

    commbytes = {w: sum([nbytes[k] for k in dependencies[key]
                                   if w not in who_has[k]])
                 for w in workers}

    minbytes = min(commbytes.values())

    workers = {w for w, nb in commbytes.items() if nb == minbytes}
    def objective(w):
        return (len(stacks[w]) + len(processing[w]),
                len(has_what.get(w, ())))
    worker = min(workers, key=objective)
    return worker


def validate_state(dependencies, dependents, waiting, waiting_data, ready,
        who_has, stacks, processing, finished_results, released,
        who_wants, wants_what, tasks=None, allow_overlap=False, allow_bad_stacks=False,
        **kwargs):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.
    """
    in_stacks = {k for v in stacks.values() for k in v}
    in_processing = {k for v in processing.values() for k in v}
    keys = {key for key in dependents if not dependents[key]}
    ready_set = set(ready)

    assert set(waiting).issubset(dependencies), "waiting not subset of deps"
    assert set(waiting_data).issubset(dependents), "waiting_data not subset"
    if tasks is not None:
        assert ready_set.issubset(tasks), "All ready tasks are tasks"
        assert set(dependents).issubset(set(tasks) | set(who_has)), "all dependents tasks"
        assert set(dependencies).issubset(set(tasks) | set(who_has)), "all dependencies tasks"

    for k, v in waiting.items():
        assert v
        assert v.issubset(dependencies[k]), "waiting set not dependencies"
        for vv in v:
            assert vv not in who_has, "waiting dependency in memory"
            assert vv not in released, "dependency released"

    for k, v in waiting_data.items():
        for vv in v:
            if vv in released:
                raise ValueError('dependent not in play', vv)
            assert (vv in ready_set or
                    vv in waiting or
                    vv in in_stacks or
                    vv in in_processing), 'dependent not in play2'

    @memoize
    def check_key(key):
        """ Validate a single key, recurse downards """
        vals = ([key in waiting,
                 key in ready,
                 key in in_stacks,
                 key in in_processing,
                 not not who_has.get(key),
                 key in released])
        if ((allow_overlap and sum(vals) < 1) or
            (not allow_overlap and sum(vals) != 1)):
            raise ValueError("Key exists in wrong number of places", key, vals)

        if not all(map(check_key, dependencies[key])):  # Recursive case
            raise ValueError("Failed to check dependencies")

        if who_has.get(key):
            assert not any(key in waiting.get(dep, ())
                           for dep in dependents.get(key, ()))
            assert not waiting.get(key)

        if not allow_bad_stacks and (key in in_stacks or key in in_processing):
            if not all(who_has.get(dep) for dep in dependencies[key]):
                raise ValueError("Key in stacks/processing without all deps",
                                 key)
            assert not waiting.get(key)
            assert key not in ready

        if finished_results is not None:
            if key in finished_results:
                assert who_has.get(key)
                assert key in keys

            if key in keys and who_has.get(key):
                assert key in finished_results

        for key, s in who_wants.items():
            assert s, "empty who_wants"
            for client in s:
                assert key in wants_what[client]

        if key in waiting:
            assert waiting[key], 'waiting empty'

        if key in ready:
            assert key not in waiting

        return True

    assert all(map(check_key, keys))


_round_robin = [0]


fast_task_prefixes = {'sum', 'max', 'min', 'len', 'rechunk-split'}
