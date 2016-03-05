from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
from datetime import datetime
from functools import partial
import logging
from math import ceil
import socket
from time import time
import uuid

from toolz import frequencies, memoize, concat, identity, valmap, keymap
from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError, IOStream

from dask.compatibility import PY3, unicode
from dask.core import get_deps, reverse_dict, istask
from dask.order import order

from .core import (rpc, coerce_to_rpc, connect, read, write, MAX_BUFFER_SIZE,
        Server, send_recv)
from .client import (scatter_to_workers,
        gather_from_workers, broadcast_to_workers)
from .utils import (All, ignoring, clear_queue, _deps, get_ip,
        ignore_exceptions, ensure_ip, get_traceback, truncate_exception,
        tokey, log_errors)


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
    See http://distributed.readthedocs.org/en/latest/plugins.html

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
    * **processing:** ``{worker: {keys}}``:
        Set of keys currently in execution on each worker
    * **stacks:** ``{worker: [keys]}``:
        List of keys waiting to be sent to each worker
    * **retrictions:** ``{key: {hostnames}}``:
        A set of hostnames per key of where that key can be run.  Usually this
        is empty unless a key has been specifically restricted to only run on
        certain hosts.  These restrictions don't include a worker port.  Any
        worker on that hostname is deemed valid.
    * **loose_retrictions:** ``{key}``:
        Set of keys for which we are allow to violate restrictions (see above)
        if not valid workers are present.
    * **in_play:** ``{key}``:
        All keys in one of who_has, waiting, stacks, processing.  This is any
        key that will eventually be in memory.
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
            ip=None, services=None, **kwargs):
        self.scheduler_queues = [Queue()]
        self.report_queues = []
        self.streams = dict()
        self.status = None
        self.coroutines = []
        self.ip = ip or get_ip()
        self.delete_interval = delete_interval

        self.tasks = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.generation = 0
        self.has_what = defaultdict(set)
        self.in_play = set()
        self.keyorder = dict()
        self.nbytes = dict()
        self.ncores = dict()
        self.worker_info = defaultdict(dict)
        self.aliases = dict()
        self.processing = dict()
        self.restrictions = dict()
        self.loose_restrictions = set()
        self.stacks = dict()
        self.waiting = dict()
        self.waiting_data = dict()
        self.ready = deque()
        self.idle = set()
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

        self._rpcs = dict()

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
                         'who_has': self.get_who_has}

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

    def rpc(self, arg=None, ip=None, port=None, addr=None):
        """ Cached rpc objects """
        key = arg, ip, port, addr
        if key not in self._rpcs:
            self._rpcs[key] = rpc(arg=arg, ip=ip, port=port, addr=addr)
        return self._rpcs[key]

    @property
    def address(self):
        return '%s:%d' % (self.ip, self.port)

    @property
    def address_tuple(self):
        return (self.ip, self.port)

    def identity(self, stream):
        """ Basic information about ourselves and our cluster """
        d = {'type': type(self).__name__,
             'id': str(self.id),
             'workers': list(self.ncores),
             'services': {key: v.port for (key, v) in self.services.items()}}
        return d

    def put(self, msg):
        """ Place a message into the scheduler's queue """
        return self.scheduler_queues[0].put_nowait(msg)

    def start(self, port=8786, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        collections = [self.tasks, self.dependencies, self.dependents,
                self.waiting, self.waiting_data, self.in_play, self.keyorder,
                self.nbytes, self.processing, self.restrictions,
                self.loose_restrictions, self.ready, self.who_wants,
                self.wants_what]
        for collection in collections:
            collection.clear()

        self.processing = {addr: set() for addr in self.ncores}
        self.stacks = {addr: list() for addr in self.ncores}

        self.worker_queues = {addr: Queue() for addr in self.ncores}

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        self._worker_coroutines = [self.worker(w) for w in self.ncores]
        self._delete_periodic_callback = \
                PeriodicCallback(callback=self.clear_data_from_workers,
                                 callback_time=self.delete_interval,
                                 io_loop=self.loop)
        self._delete_periodic_callback.start()

        self.heal_state()

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
            logger.info("Start Scheduler at: %20s:%s", self.ip, self.port)
            for k, v in self.services.items():
                logger.info("  %13s at: %20s:%s", k, self.ip, v.port)

        return self.finished()

    @gen.coroutine
    def finished(self):
        """ Wait until all coroutines have ceased """
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    @gen.coroutine
    def close(self, stream=None):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        yield self.cleanup()
        yield self.finished()
        self.status = 'closed'
        self.stop()

    @gen.coroutine
    def cleanup(self):
        """ Clean up queues and coroutines, prepare to stop """
        if self.status == 'closing':
            raise gen.Return()

        self.status = 'closing'
        logger.debug("Cleaning up coroutines")
        n = 0
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
                    self.who_has, self.restrictions, self.loose_restrictions,
                    self.nbytes, key)
            if not new_worker:
                raise ValueError("No valid workers found")
            else:
                self.stacks[new_worker].append(key)
                self.ensure_occupied(new_worker)
        else:
            self.ready.appendleft(key)
            self.ensure_idle_ready()

    def ensure_idle_ready(self):
        """ Run ready tasks on idle workers """
        while self.idle and self.ready and self.ncores:
            worker = min(self.idle, key=lambda w: len(self.has_what[w]))
            self.ensure_occupied(worker)

    def mark_key_in_memory(self, key, workers=None, type=None):
        """ Mark that a key now lives in distributed memory """
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

        self.release_live_dependencies(key)

        msg = {'op': 'key-in-memory',
               'key': key,
               'workers': list(workers)}
        if type:
            msg['type'] = type
        self.report(msg)

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
        """ Send tasks to worker while it has tasks and free cores

        These tasks may come from the worker's own stacks or from the global
        ready deque.

        We update the idle workers set appropriately.
        """
        logger.debug('Ensure worker is occupied: %s', worker)
        while (self.stacks[worker] and
               self.ncores[worker] > len(self.processing[worker])):
            key = self.stacks[worker].pop()
            if key not in self.tasks:
                continue
            if self.who_has.get(key):
                continue
            self.processing[worker].add(key)
            logger.debug("Send job to worker: %s, %s", worker, key)
            self.worker_queues[worker].put_nowait(
                    {'op': 'compute-task',
                     'key': key,
                     'task': self.tasks[key],
                     'who_has': {dep: self.who_has[dep] for dep in
                                 self.dependencies[key]}})

        while (self.ready and
               self.ncores[worker] > len(self.processing[worker])):
            key = self.ready.pop()
            if key not in self.tasks:
                continue
            if self.who_has.get(key):
                continue
            self.processing[worker].add(key)
            logger.debug("Send job to worker: %s, %s", worker, key)
            self.worker_queues[worker].put_nowait(
                    {'op': 'compute-task',
                     'key': key,
                     'task': self.tasks[key],
                     'who_has': {dep: self.who_has[dep]
                                  for dep in self.dependencies[key]}})

        if self.ncores[worker] > len(self.processing[worker]):
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

        if client:
            self.client_wants_keys(keys=list(who_has), client=client)

        self.in_play.update(who_has)

    def mark_task_erred(self, key, worker, exception, traceback):
        """ Mark that a task has erred on a particular worker

        See Also
        --------
        Scheduler.mark_failed
        """
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

        self.release_live_dependencies(key)

        self.in_play.remove(key)
        for dep in self.dependents[key]:
            self.mark_failed(dep, failing_key)

    def mark_task_finished(self, key, worker, nbytes, type=None):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Mark task as finished %s, %s", key, worker)
        if key in self.processing[worker]:
            self.nbytes[key] = nbytes
            self.mark_key_in_memory(key, [worker], type=type)
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
        """ Mark that certain keys have gone missing.  Recover.

        See Also
        --------
        heal_missing_data
        """
        if key and worker:
            try:
                self.processing[worker].remove(key)
            except KeyError:
                logger.debug("Tried to remove %s from %s, but it wasn't there",
                             key, worker)

        missing = set(missing)
        logger.debug("Recovering missing data: %s", missing)
        for k in missing:
            with ignoring(KeyError):
                workers = self.who_has.pop(k)
                for worker in workers:
                    self.has_what[worker].remove(k)
        self.my_heal_missing_data(missing)

        if key and worker:
            self.waiting[key] = missing
            logger.debug('task missing data, %s, %s', key, self.waiting)
            self.ensure_occupied(worker)

        # self.validate(allow_overlap=True, allow_bad_stacks=True)

    def log_state(self, msg=''):
        """ Log current full state of the scheduler """
        logger.debug("Runtime State: %s", msg)
        logger.debug('\n\nwaiting: %s\n\nstacks: %s\n\nprocessing: %s\n\n'
                'in_play: %s\n\n', self.waiting, self.stacks, self.processing,
                self.in_play)

    def remove_worker(self, stream=None, address=None, heal=True):
        """ Mark that a worker no longer seems responsive

        See Also
        --------
        Scheduler.heal_state
        """
        address = self.coerce_address(address)
        logger.debug("Remove worker %s", address)
        if address not in self.processing:
            return
        keys = self.has_what.pop(address)
        for i in range(self.ncores[address]):  # send close message, in case not dead
            self.worker_queues[address].put_nowait({'op': 'close', 'report': False})
        del self.worker_queues[address]
        del self.ncores[address]
        del self.stacks[address]
        del self.processing[address]
        del self.aliases[self.worker_info[address]['name']]
        del self.worker_info[address]
        if address in self.idle:
            self.idle.remove(address)
        if not self.stacks:
            logger.critical("Lost all workers")
        missing_keys = set()
        for key in keys:
            self.who_has[key].remove(address)
            if not self.who_has[key]:
                missing_keys.add(key)
        gone_data = {k for k, v in self.who_has.items() if not v}
        self.in_play.difference_update(missing_keys)
        for k in gone_data:
            del self.who_has[k]

        if heal:
            self.heal_state()

        return 'OK'

    def add_worker(self, stream=None, address=None, keys=(), ncores=None,
                   name=None, coerce_address=True, **info):
        if coerce_address:
            address = self.coerce_address(address)
        name = name or address
        if name in self.aliases:
            return 'name taken, %s' % name

        self.ncores[address] = ncores

        self.aliases[name] = address

        info['name'] = name
        self.worker_info[address] = info

        if address not in self.processing:
            self.has_what[address] = set()
            self.processing[address] = set()
            self.stacks[address] = []
            self.worker_queues[address] = Queue()

        for key in keys:
            self.mark_key_in_memory(key, [address])

        self._worker_coroutines.append(self.worker(address))

        if self.ncores[address] > len(self.processing[address]):
            self.idle.add(address)

        self.ensure_occupied(address)

        logger.info("Register %s", str(address))
        return 'OK'

    def update_graph(self, client=None, tasks=None, keys=None,
                     dependencies=None, restrictions=None,
                     loose_restrictions=None):
        """ Add new computations to the internal dask graph

        This happens whenever the Executor calls submit, map, get, or compute.
        """
        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]

        if not isinstance(keys, set):
            keys = set(keys)

        for key, task in tasks.items():
            if key not in self.tasks:
                self.tasks[key] = task

        for key in tasks:  # add dependencies/dependents
            if key in self.dependencies:
                continue

            task = self.tasks[key]
            self.dependencies[key] = set(dependencies.get(key, ()))

            for dep in self.dependencies[key]:
                if dep not in self.dependents:
                    self.dependents[dep] = set()
                self.dependents[dep].add(key)

            if key not in self.dependents:
                self.dependents[key] = set()

        for k in keys:
            self.who_wants[k].add(client)
            self.wants_what[client].add(k)

        ready_to_run = set()

        exterior = keys_outside_frontier(self.dependencies, keys, self.in_play)
        self.in_play |= exterior
        for key in exterior:
            deps = self.dependencies[key]
            wait_keys = {d for d in deps if not self.who_has.get(d)}
            if wait_keys:
                self.waiting[key] = wait_keys
            else:
                ready_to_run.add(key)
            for dep in deps:
                if dep not in self.waiting_data:
                    self.waiting_data[dep] = set()
                self.waiting_data[dep].add(key)

            if key not in self.waiting_data:
                self.waiting_data[key] = set()

        if restrictions:
            restrictions = {k: set(map(self.coerce_address, v))
                            for k, v in restrictions.items()}
            self.restrictions.update(restrictions)

            if loose_restrictions:
                self.loose_restrictions |= set(loose_restrictions)

        new_keyorder = order(tasks)  # TODO: define order wrt old graph
        for key in new_keyorder:
            if key not in self.keyorder:
                # TODO: add test for this
                self.keyorder[key] = (self.generation, new_keyorder[key]) # prefer old
        if len(tasks) > 1:
            self.generation += 1  # older graph generations take precedence

        for key in tasks:
            for dep in self.dependencies[key]:
                if dep in self.exceptions_blame:
                    self.mark_failed(key, self.exceptions_blame[dep])

        for key in keys:
            if self.who_has.get(key):
                self.mark_key_in_memory(key)

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(self, tasks, keys, restrictions or {})
            except Exception as e:
                logger.exception(e)

        for key in ready_to_run:
            self.mark_ready_to_run(key)

        self.ensure_idle_ready()

    def client_releases_keys(self, keys=None, client=None):
        for k in list(keys):
            with ignoring(KeyError):
                self.wants_what[client].remove(k)
            with ignoring(KeyError):
                self.who_wants[k].remove(client)
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
        assert not self.dependents[key] and key not in self.who_wants
        if key in self.tasks:
            del self.tasks[key]
            self.release_live_dependencies(key)
            del self.dependents[key]
            for dep in self.dependencies[key]:
                s = self.dependents[dep]
                s.remove(key)
                if not s and dep not in self.who_wants:
                    self.forget(dep)
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

            if key in self.waiting:
                del self.waiting[key]
            if key in self.waiting_data:
                del self.waiting_data[key]

        if key in self.who_has:
            self.delete_data(keys=[key])

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
        logger.info("Client %s requests to cancel keys %s", client, keys)
        for key in keys:
            self.cancel_key(key, client)

    def heal_state(self):
        """ Recover from catastrophic change """
        logger.debug("Heal state")
        self.log_state("Before Heal")
        state = heal(self.dependencies, self.dependents, self.who_has,
                self.stacks, self.processing, self.waiting, self.waiting_data,
                self.ready)
        released = state['released']
        self.in_play.clear(); self.in_play.update(state['in_play'])

        for key in set(self.who_wants) & released:
            self.report({'op': 'lost-key', 'key': key})

        for key in state['ready']:
            self.mark_ready_to_run(key)

        self.delete_data(keys=set(self.who_has) & released - set(self.who_wants))
        self.in_play.update(self.who_has)

        for worker in self.ncores:
            self.ensure_occupied(worker)

        self.log_state("After Heal")

    def my_heal_missing_data(self, missing):
        """ Recover from lost data """
        logger.debug("Heal from missing data")
        ready_to_run = heal_missing_data(self.tasks, self.dependencies,
                self.dependents, self.who_has, self.in_play, self.waiting,
                self.waiting_data, missing)
        for key in ready_to_run:
            self.mark_ready_to_run(key)

    def report(self, msg):
        """ Publish updates to all listening Queues and Streams """
        for q in self.report_queues:
            q.put_nowait(msg)
        if 'key' in msg:
            streams = {self.streams.get(c, ())
                       for c in self.who_wants.get(msg['key'], ())}
        else:
            streams = self.streams.values()
        for s in streams:
            try:
                self._last_message = write(s, msg), msg  # asynchrnous
                logger.debug("Scheduler sends message to client %s", msg)
            except StreamClosedError:
                logger.critical("Tried writing to closed stream: %s", msg)

    def add_plugin(self, plugin):
        """ Add external plugin to scheduler

        See http://http://distributed.readthedocs.org/en/latest/plugins.html
        """
        self.plugins.append(plugin)

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
        self.streams[client] = stream
        try:
            yield self.handle_messages(stream, stream, client=client)
        finally:
            if not stream.closed():
                yield write(stream, {'op': 'stream-closed'})
                stream.close()
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
            try:
                msg = yield next_message()  # in_queue.get()
            except (StreamClosedError, AssertionError):
                break
            except Exception as e:
                from .core import dumps
                put({'op': 'scheduler-error',
                     'exception': dumps(truncate_exception(e)),
                     'traceback': dumps(get_traceback())})
                logger.exception(e)
                continue
            logger.debug("scheduler receives message %s", msg)
            try:
                op = msg.pop('op')
            except Exception as e:
                logger.exception(e)

            if op == 'close-stream':
                break
            elif op == 'close':
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
                break

        self.remove_client(client=client)
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
            self.remove_worker(address=ident)

    @gen.coroutine
    def worker_core(self, ident, i):
        """ Manage one core on one distributed worker node

        This coroutine listens on worker_queue for the following operations

        **Incoming Messages**:

        - compute-task:  call worker.compute(...) on remote node, report when done
        - close: close connection to worker node, report `worker-finished` to
          scheduler

        See Also
        --------
        Scheduler.mark_task_finished
        Scheduler.mark_task_erred
        Scheduler.mark_missing_data
        distributed.worker.Worker.compute
        """
        worker = rpc(addr=ident)
        logger.debug("Start worker core %s, %d", ident, i)

        with log_errors():
            while True:
                msg = yield self.worker_queues[ident].get()
                if msg['op'] == 'close':
                    logger.debug("Worker core receives close message %s, %s",
                            ident, msg)
                    break
                if msg['op'] == 'compute-task':
                    key = msg['key']
                    who_has = valmap(list, msg['who_has'])
                    task = msg['task']
                    if istask(task):
                        task = {'task': task}

                    response = yield worker.compute(who_has=who_has,
                                                    key=key,
                                                    report=False,
                                                    **task)
                    if response['status'] == 'OK':
                        nbytes = response['nbytes']
                    logger.debug("Compute response from worker %s, %s, %s",
                                 ident, key, response)
                    if response['status'] == 'error':
                        self.mark_task_erred(key, ident, response['exception'],
                                                         response['traceback'])

                    elif response['status'] == 'missing-data':
                        self.mark_missing_data(response['keys'],
                                               key=key, worker=ident)

                    else:
                        self.mark_task_finished(key, ident, nbytes,
                                                type=response.get('type'))

        yield worker.close(close=True)
        worker.close_streams()
        if msg.get('report', True):
            self.put({'op': 'worker-finished',
                      'worker': ident})
        logger.debug("Close worker core, %s, %d", ident, i)

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
                                   keys=list(keys - self.has_what[worker]),
                                   report=False)
                          for worker, keys in d.items()]
            for worker, keys in d.items():
                logger.debug("Remove %d keys from worker %s", len(keys), worker)
            yield ignore_exceptions(coroutines, socket.error, StreamClosedError)

        raise Return('OK')

    def delete_data(self, stream=None, keys=None):
        for key in keys:
            for worker in self.who_has[key]:
                self.has_what[worker].remove(key)
                self.deleted_keys[worker].add(key)
            del self.who_has[key]
            if key in self.waiting_data:
                del self.waiting_data[key]
            if key in self.in_play:
                self.in_play.remove(key)

    @gen.coroutine
    def scatter(self, stream=None, data=None, workers=None, client=None,
            broadcast=False):
        """ Send data out to workers """
        if not self.ncores:
            raise ValueError("No workers yet found.")
        if not broadcast:
            if workers:
                workers = [self.coerce_address(w) for w in workers]
            ncores = workers if workers is not None else self.ncores
            keys, who_has, nbytes = yield scatter_to_workers(ncores, data,
                                                             report=False,
                                                             serialize=False)
        else:
            workers2 = workers if workers is not None else list(self.ncores)
            keys, nbytes = yield broadcast_to_workers(workers2, data,
                                                      report=False,
                                                      serialize=False)
            who_has = {k: set(workers2) for k in keys}

        self.update_data(who_has=who_has, nbytes=nbytes)
        self.client_wants_keys(keys=keys, client=client)
        raise gen.Return(keys)

    @gen.coroutine
    def gather(self, stream=None, keys=None):
        """ Collect data in from workers """
        keys = list(keys)
        who_has = {key: self.who_has[key] for key in keys}

        try:
            data = yield gather_from_workers(who_has, deserialize=False)
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
            self.remove_worker(address=addr, heal=False)

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

    def validate(self, allow_overlap=False, allow_bad_stacks=False):
        released = set(self.tasks) - self.in_play
        validate_state(self.dependencies, self.dependents, self.waiting,
                self.waiting_data, self.ready, self.who_has, self.stacks,
                self.processing, None, released, self.in_play, self.who_wants,
                self.wants_what, tasks=self.tasks,
                allow_overlap=allow_overlap, allow_bad_stacks=allow_bad_stacks)
        if not (set(self.ncores) == \
                set(self.has_what) == \
                set(self.stacks) == \
                set(self.processing) == \
                set(self.worker_info) == \
                set(self.worker_queues)):
            raise ValueError("Workers not the same in all collections")

    @gen.coroutine
    def feed(self, stream, function=None, setup=None, teardown=None, interval=1, **kwargs):
        import cloudpickle
        if function:
            function = cloudpickle.loads(function)
        if setup:
            setup = cloudpickle.loads(setup)
        if teardown:
            teardown = cloudpickle.loads(teardown)
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

    def get_who_has(self, stream, keys=None):
        if keys is not None:
            return {k: list(self.who_has[k]) for k in keys}
        else:
            return valmap(list, self.who_has)

    def get_has_what(self, stream, keys=None):
        if keys is not None:
            keys = map(self.coerce_address, keys)
            return {k: list(self.has_what[k]) for k in keys}
        else:
            return valmap(list, self.has_what)

    def get_ncores(self, stream, addresses=None):
        if addresses is not None:
            addresses = map(self.coerce_address, addresses)
            return {k: self.ncores.get(k, None) for k in addresses}
        else:
            return self.ncores

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


def decide_worker(dependencies, stacks, who_has, restrictions,
                  loose_restrictions, nbytes, key):
    """ Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> stacks = {'alice:8000': ['z'], 'bob:8000': []}
    >>> who_has = {'a': {'alice:8000'}}
    >>> nbytes = {'a': 100}
    >>> restrictions = {}
    >>> loose_restrictions = set()

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, who_has, restrictions,
    ...               loose_restrictions, nbytes, 'b')
    'alice:8000'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice:8000', 'bob:8000'}}
    >>> decide_worker(dependencies, stacks, who_has, restrictions,
    ...               loose_restrictions, nbytes, 'b')
    'bob:8000'

    Optionally provide restrictions of where jobs are allowed to occur

    >>> restrictions = {'b': {'alice', 'charile'}}
    >>> decide_worker(dependencies, stacks, who_has, restrictions,
    ...               loose_restrictions, nbytes, 'b')
    'alice:8000'

    If the task requires data communication, then we choose to minimize the
    number of bytes sent between workers. This takes precedence over worker
    occupancy.

    >>> dependencies = {'c': {'a', 'b'}}
    >>> who_has = {'a': {'alice:8000'}, 'b': {'bob:8000'}}
    >>> nbytes = {'a': 1, 'b': 1000}
    >>> stacks = {'alice:8000': [], 'bob:8000': []}

    >>> decide_worker(dependencies, stacks, who_has, {}, set(), nbytes, 'c')
    'bob:8000'
    """
    deps = dependencies[key]
    workers = frequencies(w for dep in deps
                            for w in who_has[dep])
    if not workers:
        workers = stacks
    if key in restrictions:
        r = restrictions[key]
        workers = {w for w in workers if w in r or w.split(':')[0] in r}  # TODO: nonlinear
        if not workers:
            workers = {w for w in stacks if w in r or w.split(':')[0] in r}
            if not workers:
                if key in loose_restrictions:
                    return decide_worker(dependencies, stacks, who_has,
                                         {}, set(), nbytes, key)
                else:
                    raise ValueError("Task has no valid workers", key, r)
    if not workers or not stacks:
        return None

    commbytes = {w: sum(nbytes[k] for k in dependencies[key]
                                   if w not in who_has[k])
                 for w in workers}

    minbytes = min(commbytes.values())

    workers = {w for w, nb in commbytes.items() if nb == minbytes}
    worker = min(workers, key=lambda w: len(stacks[w]))
    return worker


def validate_state(dependencies, dependents, waiting, waiting_data, ready,
        who_has, stacks, processing, finished_results, released, in_play,
        who_wants, wants_what, tasks=None, allow_overlap=False, allow_bad_stacks=False,
        **kwargs):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.

    See Also
    --------
    heal: fix a broken runtime state
    """
    in_stacks = {k for v in stacks.values() for k in v}
    in_processing = {k for v in processing.values() for k in v}
    keys = {key for key in dependents if not dependents[key]}
    ready_set = set(ready)

    assert set(waiting).issubset(dependencies)
    assert set(waiting_data).issubset(dependents)
    if tasks is not None:
        assert ready_set.issubset(tasks)
        assert set(dependents).issubset(set(tasks))
        assert set(dependencies).issubset(set(tasks))

    for k, v in waiting.items():
        assert v
        assert v.issubset(dependencies[k])
        for vv in v:
            assert vv not in who_has
            assert vv in in_play

    for k, v in waiting_data.items():
        for vv in v:
            assert vv in in_play
            assert (vv in ready_set or
                    vv in waiting or
                    vv in in_stacks or
                    vv in in_processing)

    @memoize
    def check_key(key):
        """ Validate a single key, recurse downards """
        val = sum([key in waiting,
                   key in ready,
                   key in in_stacks,
                   key in in_processing,
                   who_has.get(key) is not None,
                   key in released])
        if (allow_overlap and val < 1) or (not allow_overlap and val != 1):
            raise ValueError("Key exists in wrong number of places", key, val)

        if not (key in released) != (key in in_play):
            raise ValueError("Key released != in_play", key)

        if not all(map(check_key, dependencies[key])):# Recursive case
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


def keys_outside_frontier(dependencies, keys, frontier):
    """ All keys required by terminal keys within graph up to frontier

    Given:

    1. A graph
    2. A set of desired keys
    3. A frontier/set of already-computed (or already-about-to-be-computed) keys

    Find all keys necessary to compute the desired set that are outside of the
    frontier.

    Parameters
    ----------
    tasks: dict
    keys: iterable of keys
    frontier: set of keys

    Examples
    --------
    >>> f = lambda:1
    >>> dsk = {'x': 1, 'a': 2, 'y': (f, 'x'), 'b': (f, 'a'),
    ...        'z': (f, 'b', 'y')}
    >>> dependencies, dependents = get_deps(dsk)
    >>> keys = {'z', 'b'}
    >>> frontier = {'y', 'a'}
    >>> list(sorted(keys_outside_frontier(dependencies, keys, frontier)))
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


def heal(dependencies, dependents, who_has, stacks, processing, waiting,
        waiting_data, ready, **kwargs):
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
    ready.clear()
    ready = set()
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
        if who_has.get(key):
            return

        for dep in dependencies[key]:
            make_accessible(dep)  # recurse

        s = {dep for dep in dependencies[key] if not who_has.get(dep)}
        if s:
            waiting[key] = s
        else:
            ready.add(key)

    for key in outputs:
        make_accessible(key)

    waiting_data.update({key: {dep for dep in dependents[key]
                              if dep not in new_released
                              and not who_has.get(dep)}
                    for key in dependents
                    if key not in new_released})

    def unrunnable(key):
        return (key in new_released
             or who_has.get(key)
             or waiting.get(key)
             or not all(who_has.get(dep) for dep in dependencies[key]))

    for key in list(filter(unrunnable, rev_stacks)):
        remove_from_stacks(key)

    for key in list(filter(unrunnable, rev_processing)):
        remove_from_processing(key)

    for key in list(rev_stacks) + list(rev_processing):
        if key in waiting:
            assert not waiting[key]
            del waiting[key]
        if key in ready:
            ready.remove(key)

    finished_results = {key for key in outputs if who_has.get(key)}

    in_play = ({k for k, v in who_has.items() if v}
            | set(waiting)
            | ready
            | (set.union(*processing.values()) if processing else set())
            | set(concat(stacks.values())))

    output = {'keys': outputs,
             'dependencies': dependencies, 'dependents': dependents,
             'waiting': waiting, 'waiting_data': waiting_data,
             'who_has': who_has, 'processing': processing, 'stacks': stacks,
             'finished_results': finished_results, 'released': new_released,
             'in_play': in_play, 'who_wants': {}, 'wants_what': {},
             'ready': ready}

    validate_state(**output)
    return output


_round_robin = [0]


def heal_missing_data(tasks, dependencies, dependents,
                      who_has, in_play, waiting, waiting_data, missing):
    """ Return to healthy state after discovering missing data

    When we identify that we're missing certain keys we rewind runtime state to
    evaluate those keys.
    """
    logger.debug("Healing missing: %s", missing)

    ready_to_run = set()

    for key in missing:
        if key in in_play:
            in_play.remove(key)

    def ensure_key(key):
        if key in in_play:
            return
        for dep in dependencies[key]:
            ensure_key(dep)
            waiting_data[dep].add(key)
        s = {dep for dep in dependencies[key] if not who_has.get(dep)}
        if s:
            waiting[key] = s
            logger.debug("Added key to waiting: %s", key)
        else:
            ready_to_run.add(key)
            logger.debug("Added key to ready-to-run: %s", key)
        waiting_data[key] = {dep for dep in dependents[key] if dep in in_play
                                                    and not who_has.get(dep)}
        in_play.add(key)

    for key in missing:
        ensure_key(key)

    assert set(missing).issubset(in_play)

    return ready_to_run
