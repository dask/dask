from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque, OrderedDict
from datetime import datetime, timedelta
import logging
import math
from math import log
import os
import pickle
import random
import socket
from time import time
from timeit import default_timer

try:
    from cytoolz import frequencies, topk
except ImportError:
    from toolz import frequencies, topk
from toolz import memoize, valmap, first, second, keymap, unique, concat, merge
from tornado import gen
from tornado.gen import Return
from tornado.queues import Queue
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError, IOStream

from dask.compatibility import PY3, unicode
from dask.core import reverse_dict
from dask.order import order

from .batched import BatchedSend
from .utils_comm import (scatter_to_workers, gather_from_workers)
from .core import (rpc, connect, read, write, MAX_BUFFER_SIZE,
        Server, send_recv, coerce_to_address, error_message)
from .utils import (All, ignoring, clear_queue, get_ip, ignore_exceptions,
        ensure_ip, get_fileno_limit, log_errors, key_split, mean,
        divide_n_among_bins)
from .config import config


logger = logging.getLogger(__name__)

BANDWIDTH = config.get('bandwidth', 100e6)
ALLOWED_FAILURES = config.get('allowed-failures', 3)

LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)


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

    The scheduler communicates with the outside world through Tornado IOStreams
    It maintains a consistent and valid view of the world even when listening
    to several clients at once.

    A Scheduler is typically started either with the ``dask-scheduler``
    executable::

         $ dask-scheduler
         Scheduler started at 127.0.0.1:8786

    Or within a LocalCluster a Client starts up without connection
    information::

        >>> c = Client()  # doctest: +SKIP
        >>> c.cluster.scheduler  # doctest: +SKIP
        Scheduler(...)

    Users typically do not interact with the scheduler directly but rather with
    the client object ``Client``.

    **State**

    The scheduler contains the following state variables.  Each variable is
    listed along with what it stores and a brief description.

    * **tasks:** ``{key: task}``:
        Dictionary mapping key to a serialized task like the following:
        ``{'function': b'...', 'args': b'...'}`` or ``{'task': b'...'}``
    * **dependencies:** ``{key: {keys}}``:
        Dictionary showing which keys depend on which others
    * **dependents:** ``{key: {keys}}``:
        Dictionary showing which keys are dependent on which others
    * **task_state:** ``{key: string}``:
        Dictionary listing the current state of every task among the following:
        released, waiting, stacks, queue, no-worker, processing, memory, erred
    * **priority:** ``{key: tuple}``:
        A score per key that determines its priority
    * **waiting:** ``{key: {key}}``:
        Dictionary like dependencies but excludes keys already computed
    * **waiting_data:** ``{key: {key}}``:
        Dictionary like dependents but excludes keys already computed
    * **ready:** ``deque(key)``
        Keys that are ready to run, but not yet assigned to a worker
    * **stacks:** ``{worker: [keys]}``:
        List of keys waiting to be sent to each worker
    * **processing:** ``{worker: {key: cost}}``:
        Set of keys currently in execution on each worker and their expected
        duration
    * **stack_durations:** ``{worker: [ints]}``:
        Expected durations of stacked tasks
    * **stacks_duration:** ``{worker: int}``:
        Total duration of all tasks in each workers stack
    * **rprocessing:** ``{key: {worker}}``:
        Set of workers currently executing a particular task
    * **who_has:** ``{key: {worker}}``:
        Where each key lives.  The current state of distributed memory.
    * **has_what:** ``{worker: {key}}``:
        What worker has what keys.  The transpose of who_has.
    * **released:** ``{keys}``
        Set of keys that are known, but released from memory
    * **unrunnable:** ``{key}``
        Keys that we are unable to run
    * **restrictions:** ``{key: {hostnames}}``:
        A set of hostnames per key of where that key can be run.  Usually this
        is empty unless a key has been specifically restricted to only run on
        certain hosts.  These restrictions don't include a worker port.  Any
        worker on that hostname is deemed valid.
    * **loose_restrictions:** ``{key}``:
        Set of keys for which we are allow to violate restrictions (see above)
        if not valid workers are present.
    *  **exceptions:** ``{key: Exception}``:
        A dict mapping keys to remote exceptions
    *  **tracebacks:** ``{key: list}``:
        A dict mapping keys to remote tracebacks stored as a list of strings
    *  **exceptions_blame:** ``{key: key}``:
        A dict mapping a key to another key on which it depends that has failed
    * **suspicious_tasks:** ``{key: int}``
        Number of times a task has been involved in a worker failure
    *  **deleted_keys:** ``{key: {workers}}``
        Locations of workers that have keys that should be deleted
    * **wants_what:** ``{client: {key}}``:
        What keys are wanted by each client..  The transpose of who_wants.
    * **who_wants:** ``{key: {client}}``:
        Which clients want each key.  The active targets of computation.
    * **nbytes:** ``{key: int}``:
        Number of bytes for a key as reported by workers holding that key.
    * **stealable:** ``[[key]]``
        A list of stacks of stealable keys, ordered by stealability
    * **ncores:** ``{worker: int}``:
        Number of cores owned by each worker
    * **idle:** ``{worker}``:
        Set of workers that are not fully utilized
    * **worker_info:** ``{worker: {str: data}}``:
        Information about each worker
    * **host_info:** ``{hostname: dict}``:
        Information about each worker host
    * **worker_bytes:** ``{worker: int}``:
        Number of bytes in memory on each worker
    * **occupancy:** ``{worker: time}``
        Expected runtime for all tasks currently processing on a worker

    * **services:** ``{str: port}``:
        Other services running on this scheduler, like HTTP
    *  **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **streams:** ``[IOStreams]``:
        A list of Tornado IOStreams from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    * **coroutines:** ``[Futures]``:
        A list of active futures that control operation
    * **scheduler_queues:** ``[Queues]``:
        A list of Tornado Queues from which we accept stimuli
    * **report_queues:** ``[Queues]``:
        A list of Tornado Queues on which we report results
    """
    default_port = 8786

    def __init__(self, center=None, loop=None,
            max_buffer_size=MAX_BUFFER_SIZE, delete_interval=500,
            synchronize_worker_interval=60000,
            ip=None, services=None, allowed_failures=ALLOWED_FAILURES,
            validate=False, steal=True, **kwargs):

        # Attributes
        self.ip = ip or get_ip()
        self.allowed_failures = allowed_failures
        self.validate = validate
        self.status = None
        self.delete_interval = delete_interval
        self.synchronize_worker_interval = synchronize_worker_interval
        self.steal = steal

        # Communication state
        self.loop = loop or IOLoop.current()
        self.scheduler_queues = [Queue()]
        self.report_queues = []
        self.worker_streams = dict()
        self.streams = dict()
        self.coroutines = []
        self._worker_coroutines = []
        self._ipython_kernel = None

        # Task state
        self.tasks = dict()
        self.task_state = dict()
        self.dependencies = dict()
        self.dependents = dict()
        self.generation = 0
        self.released = set()
        self.priority = dict()
        self.nbytes = dict()
        self.worker_bytes = dict()
        self.processing = dict()
        self.rprocessing = defaultdict(set)
        self.task_duration = {prefix: 0.00001 for prefix in fast_tasks}
        self.restrictions = dict()
        self.loose_restrictions = set()
        self.suspicious_tasks = defaultdict(lambda: 0)
        self.stacks = dict()
        self.stack_durations = dict()
        self.stack_duration = dict()
        self.waiting = dict()
        self.waiting_data = dict()
        self.ready = deque()
        self.unrunnable = set()
        self.idle = set()
        self.maybe_idle = set()
        self.who_has = dict()
        self.has_what = dict()
        self.who_wants = defaultdict(set)
        self.wants_what = defaultdict(set)
        self.deleted_keys = defaultdict(set)
        self.exceptions = dict()
        self.tracebacks = dict()
        self.exceptions_blame = dict()
        self.datasets = dict()
        self.stealable = [set() for i in range(12)]
        self.key_stealable = dict()
        self.stealable_unknown_durations = defaultdict(set)

        # Worker state
        self.ncores = dict()
        self.worker_info = dict()
        self.host_info = defaultdict(dict)
        self.aliases = dict()
        self.saturated = set()
        self.occupancy = dict()

        self.plugins = []
        self.transition_log = deque(maxlen=config.get('transition-log-length',
                                                      100000))

        self.compute_handlers = {'update-graph': self.update_graph,
                                 'update-data': self.update_data,
                                 'missing-data': self.stimulus_missing_data,
                                 'client-releases-keys': self.client_releases_keys,
                                 'restart': self.restart}

        self.handlers = {'register-client': self.add_client,
                         'scatter': self.scatter,
                         'register': self.add_worker,
                         'unregister': self.remove_worker,
                         'gather': self.gather,
                         'cancel': self.stimulus_cancel,
                         'feed': self.feed,
                         'terminate': self.close,
                         'broadcast': self.broadcast,
                         'ncores': self.get_ncores,
                         'has_what': self.get_has_what,
                         'who_has': self.get_who_has,
                         'stacks': self.get_stacks,
                         'processing': self.get_processing,
                         'nbytes': self.get_nbytes,
                         'add_keys': self.add_keys,
                         'rebalance': self.rebalance,
                         'replicate': self.replicate,
                         'start_ipython': self.start_ipython,
                         'list_datasets': self.list_datasets,
                         'get_dataset': self.get_dataset,
                         'publish_dataset': self.publish_dataset,
                         'unpublish_dataset': self.unpublish_dataset,
                         'update_data': self.update_data,
                         'change_worker_cores': self.change_worker_cores}

        self.services = {}
        for k, v in (services or {}).items():
            if isinstance(k, tuple):
                k, port = k
            else:
                port = 0

            try:
                service = v(self, io_loop=self.loop)
                service.listen(port)
                self.services[k] = service
            except Exception as e:
                logger.info("Could not launch service: %s-%d", k, port,
                            exc_info=True)

        self._transitions = {
                 ('released', 'waiting'): self.transition_released_waiting,
                 ('waiting', 'ready'): self.transition_waiting_ready,
                 ('waiting', 'released'): self.transition_waiting_released,
                 ('queue', 'processing'): self.transition_ready_processing,
                 ('stacks', 'processing'): self.transition_ready_processing,
                 ('processing', 'released'): self.transition_processing_released,
                 ('queue', 'released'): self.transition_ready_released,
                 ('stacks', 'released'): self.transition_ready_released,
                 ('no-worker', 'released'): self.transition_ready_released,
                 ('processing', 'memory'): self.transition_processing_memory,
                 ('processing', 'erred'): self.transition_processing_erred,
                 ('released', 'forgotten'): self.transition_released_forgotten,
                 ('memory', 'forgotten'): self.transition_memory_forgotten,
                 ('erred', 'forgotten'): self.transition_released_forgotten,
                 ('memory', 'released'): self.transition_memory_released,
                 ('released', 'erred'): self.transition_released_erred
        }

        connection_limit = get_fileno_limit() / 2

        super(Scheduler, self).__init__(handlers=self.handlers,
                max_buffer_size=max_buffer_size, io_loop=self.loop,
                connection_limit=connection_limit, **kwargs)

    ##################
    # Administration #
    ##################

    def __str__(self):
        return '<Scheduler: "%s:%d" processes: %d cores: %d>' % (
                self.ip, self.port, len(self.ncores), sum(self.ncores.values()))

    __repr__ = __str__

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
             'services': {key: v.port for (key, v) in self.services.items()},
             'workers': dict(self.worker_info)}
        return d

    def start(self, port=8786, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        collections = [self.tasks, self.dependencies, self.dependents,
                self.waiting, self.waiting_data, self.released, self.priority,
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

        self._synchronize_data_periodic_callback = \
                PeriodicCallback(callback=self.synchronize_worker_data,
                                 callback_time=self.synchronize_worker_interval,
                                 io_loop=self.loop)
        self._synchronize_data_periodic_callback.start()

        if start_queues:
            self.loop.add_callback(self.handle_queues, self.scheduler_queues[0], None)

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
        """ Close all active IOStreams """
        for stream in self.streams.values():
            stream.stream.close()
        self.rpc.close()

    @gen.coroutine
    def close(self, stream=None, fast=False):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        self._delete_periodic_callback.stop()
        self._synchronize_data_periodic_callback.stop()
        for service in self.services.values():
            service.stop()
        yield self.cleanup()
        if not fast:
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

        for w, bstream in list(self.worker_streams.items()):
            with ignoring(AttributeError):
                yield bstream.close(ignore_closed=True)

        for s in self.scheduler_queues[1:]:
            s.put_nowait({'op': 'close-stream'})

        for q in self.report_queues:
            q.put_nowait({'op': 'close'})

    ###########
    # Stimuli #
    ###########

    def add_worker(self, stream=None, address=None, keys=(), ncores=None,
                   name=None, coerce_address=True, nbytes=None, now=None,
                   host_info=None, **info):
        """ Add a new worker to the cluster """
        with log_errors():
            local_now = time()
            now = now or time()
            info = info or {}
            host_info = host_info or {}
            if coerce_address:
                address = self.coerce_address(address)
                host, port = address.split(':')
                self.host_info[host]['last-seen'] = local_now

            if address not in self.worker_info:
                self.worker_info[address] = dict()

            if info:
                self.worker_info[address].update(info)

            if host_info:
                self.host_info[host].update(host_info)

            delay = time() - now
            self.worker_info[address]['time-delay'] = delay
            self.worker_info[address]['last-seen'] = time()

            if address in self.ncores:
                return 'OK'

            name = name or address
            if name in self.aliases:
                return 'name taken, %s' % name

            if coerce_address:
                if 'ports' not in self.host_info[host]:
                    self.host_info[host].update({'ports': set(), 'cores': 0})

                self.host_info[host]['ports'].add(port)
                self.host_info[host]['cores'] += ncores

            self.ncores[address] = ncores
            self.aliases[name] = address
            self.worker_info[address]['name'] = name

            if address not in self.processing:
                self.has_what[address] = set()
                self.worker_bytes[address] = 0
                self.processing[address] = dict()
                self.occupancy[address] = 0
                self.stacks[address] = deque()
                self.stack_durations[address] = deque()
                self.stack_duration[address] = 0

            if nbytes:
                self.nbytes.update(nbytes)

            # for key in keys:  # TODO
            #     self.mark_key_in_memory(key, [address])

            self.worker_streams[address] = BatchedSend(interval=2, loop=self.loop)
            self._worker_coroutines.append(self.worker_stream(address))

            if self.ncores[address] > len(self.processing[address]):
                self.idle.add(address)

            for key in list(self.unrunnable):
                r = self.restrictions.get(key, [])
                if address in r or host in r or name in r:
                    self.transitions({key: 'released'})

            self.maybe_idle.add(address)
            self.ensure_occupied()

            logger.info("Register %s", str(address))
            return 'OK'

    def update_graph(self, client=None, tasks=None, keys=None,
                     dependencies=None, restrictions=None, priority=None,
                     loose_restrictions=None):
        """
        Add new computations to the internal dask graph

        This happens whenever the Client calls submit, map, get, or compute.
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
            if k in self.dependencies:
                continue
            touched.add(k)
            if k not in self.tasks and k in tasks:
                self.tasks[k] = tasks[k]
                self.dependencies[k] = set(dependencies.get(k, ()))
                self.released.add(k)
                self.task_state[k] = 'released'
                for dep in self.dependencies[k]:
                    if dep not in self.dependents:
                        self.dependents[dep] = set()
                    self.dependents[dep].add(k)
                if k not in self.dependents:
                    self.dependents[k] = set()

            stack.extend(self.dependencies[k])

        recommendations = OrderedDict()

        new_priority = priority or order(tasks)  # TODO: define order wrt old graph
        self.generation += 1  # older graph generations take precedence
        for key in set(new_priority) & touched:
            if key not in self.priority:
                self.priority[key] = (self.generation, new_priority[key]) # prefer old

        if restrictions:
            restrictions = {k: set(map(self.coerce_address, v))
                            for k, v in restrictions.items()}
            self.restrictions.update(restrictions)

            if loose_restrictions:
                self.loose_restrictions |= set(loose_restrictions)

        for key in sorted(touched | keys, key=self.priority.get):
            if self.task_state[key] == 'released':
                recommendations[key] = 'waiting'

        for key in touched | keys:
            for dep in self.dependencies[key]:
                if dep in self.exceptions_blame:
                    self.exceptions_blame[key] = self.exceptions_blame[dep]
                    recommendations[key] = 'erred'
                    break

        self.transitions(recommendations)

        for plugin in self.plugins[:]:
            try:
                plugin.update_graph(self, client=client, tasks=tasks,
                        keys=keys, restrictions=restrictions or {},
                        dependencies=dependencies,
                        loose_restrictions=loose_restrictions)
            except Exception as e:
                logger.exception(e)

        for key in keys:
            if self.task_state[key] in ('memory', 'erred'):
                self.report_on_key(key)

        self.ensure_occupied()

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        # logger.debug("Stimulus task finished %s, %s", key, worker)
        self.maybe_idle.add(worker)

        if key not in self.task_state:
            return {}

        if self.task_state[key] == 'processing':
            recommendations = self.transition(key, 'memory', worker=worker,
                                              **kwargs)
        else:
            recommendations = {}

        if self.task_state[key] == 'memory':
            self.who_has[key].add(worker)
            if key not in self.has_what[worker]:
                self.worker_bytes[worker] += self.nbytes.get(key, 1000)
            self.has_what[worker].add(key)

        return recommendations

    def stimulus_task_erred(self, key=None, worker=None,
                        exception=None, traceback=None, **kwargs):
        """ Mark that a task has erred on a particular worker """
        logger.debug("Stimulus task erred %s, %s", key, worker)
        self.maybe_idle.add(worker)

        if key not in self.task_state:
            return {}

        if self.task_state[key] == 'processing':
            recommendations = self.transition(key, 'erred', cause=key,
                    exception=exception, traceback=traceback)
        else:
            recommendations = {}

        return recommendations

    def stimulus_missing_data(self, keys=None, key=None, worker=None,
            ensure=True, **kwargs):
        """ Mark that certain keys have gone missing.  Recover. """
        logger.debug("Stimulus missing data %s, %s", key, worker)
        if worker:
            self.maybe_idle.add(worker)

        recommendations = OrderedDict()
        for k in set(keys):
            if self.task_state.get(k) == 'memory':
                for w in set(self.who_has[k]):
                    self.has_what[w].remove(k)
                    self.who_has[k].remove(w)
                    self.worker_bytes[w] -= self.nbytes.get(k, 1000)
                recommendations[k] = 'released'

        if key:
            recommendations[key] = 'released'

        self.transitions(recommendations)

        if ensure:
            self.ensure_occupied()

        return {}

    def remove_worker(self, stream=None, address=None, safe=False):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors(pdb=LOG_PDB):
            address = self.coerce_address(address)
            logger.info("Remove worker %s", address)
            if address not in self.processing:
                return 'already-removed'
            with ignoring(AttributeError):
                stream = self.worker_streams[address].stream
                if not stream.closed():
                    stream.close()

            host, port = address.split(':')

            self.host_info[host]['cores'] -= self.ncores[address]
            self.host_info[host]['ports'].remove(port)

            if not self.host_info[host]['ports']:
                del self.host_info[host]

            del self.worker_streams[address]
            del self.ncores[address]
            del self.aliases[self.worker_info[address]['name']]
            del self.worker_info[address]
            if address in self.maybe_idle:
                self.maybe_idle.remove(address)
            if address in self.idle:
                self.idle.remove(address)
            if address in self.saturated:
                self.saturated.remove(address)

            recommendations = OrderedDict()

            in_flight = set(self.processing.pop(address))
            for k in list(in_flight):
                self.rprocessing[k].remove(address)
                if not safe:
                    self.suspicious_tasks[k] += 1
                if not safe and self.suspicious_tasks[k] > self.allowed_failures:
                    e = pickle.dumps(KilledWorker(k, address))
                    r = self.transition(k, 'erred', exception=e, cause=k)
                    recommendations.update(r)
                    in_flight.remove(k)
                elif not self.rprocessing[k]:
                    recommendations[k] = 'released'

            for k in self.stacks.pop(address):
                if k in self.tasks:
                    recommendations[k] = 'waiting'
            del self.stack_durations[address]
            del self.stack_duration[address]

            del self.occupancy[address]
            del self.worker_bytes[address]

            for key in self.has_what.pop(address):
                self.who_has[key].remove(address)
                if not self.who_has[key]:
                    if key in self.tasks:
                        recommendations[key] = 'released'
                    else:
                        recommendations[key] = 'forgotten'

            self.transitions(recommendations)

            if not self.stacks:
                logger.info("Lost all workers")

            self.ensure_occupied()

        return 'OK'

    def stimulus_cancel(self, stream, keys=None, client=None):
        """ Stop execution on a list of keys """
        logger.info("Client %s requests to cancel %d keys", client, len(keys))
        for key in keys:
            self.cancel_key(key, client)

    def cancel_key(self, key, client, retries=5):
        """ Cancel a particular key and all dependents """
        # TODO: this should be converted to use the transition mechanism
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

    def client_releases_keys(self, keys=None, client=None):
        """ Remove keys from client desired list """
        for key in list(keys):
            if key in self.wants_what[client]:
                self.wants_what[client].remove(key)
                s = self.who_wants[key]
                s.remove(client)
                if not s:
                    del self.who_wants[key]
                    if key in self.waiting_data and not self.waiting_data[key]:
                        r = self.transition(key, 'released')
                        self.transitions(r)
                    if key in self.dependents and not self.dependents[key]:
                        r = self.transition(key, 'forgotten')
                        self.transitions(r)

    def client_wants_keys(self, keys=None, client=None):
        for k in keys:
            self.who_wants[k].add(client)
            self.wants_what[client].add(k)

    ######################################
    # Task Validation (currently unused) #
    ######################################

    def validate_released(self, key):
        assert key in self.dependencies
        assert self.task_state[key] == 'released'
        assert key not in self.waiting_data
        assert key not in self.who_has
        assert key not in self.rprocessing
        # assert key not in self.ready
        assert key not in self.waiting
        assert not any(key in self.waiting_data.get(dep, ())
                       for dep in self.dependencies[key])
        assert key in self.released

    def validate_waiting(self, key):
        assert key in self.waiting
        assert key in self.waiting_data
        assert key not in self.who_has
        assert key not in self.rprocessing
        assert key not in self.released
        for dep in self.dependencies[key]:
            assert (dep in self.who_has) + (dep in self.waiting[key]) == 1

    def validate_processing(self, key):
        assert key not in self.waiting
        assert key in self.waiting_data
        assert key in self.rprocessing
        for w in self.rprocessing[key]:
            assert key in self.processing[w]
        assert key not in self.who_has
        for dep in self.dependencies[key]:
            assert dep in self.who_has

    def validate_memory(self, key):
        assert key in self.who_has
        assert key not in self.rprocessing
        assert key not in self.waiting
        assert key not in self.released
        for dep in self.dependents[key]:
            assert (dep in self.who_has) + (dep in self.waiting_data[key]) == 1

    def validate_queue(self, key):
        # assert key in self.ready
        assert key not in self.released
        assert key not in self.rprocessing
        assert key not in self.who_has
        assert key not in self.waiting
        for dep in self.dependencies[key]:
            assert dep in self.who_has

    def validate_stacks(self, key):
        # assert any(key in stack for stack in self.stacks.values())
        assert key not in self.released
        assert key not in self.rprocessing
        assert key not in self.who_has
        assert key not in self.waiting
        for dep in self.dependencies[key]:
            assert dep in self.who_has

    def validate_key(self, key):
        try:
            try:
                func = getattr(self, 'validate_' + self.task_state[key])
            except KeyError:
                logger.debug("Key lost: %s", key)
            except AttributeError:
                logger.info("self.validate_%s not found", self.task_state[key])
            else:
                func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def validate_state(self, allow_overlap=False, allow_bad_stacks=True):
        validate_state(self.dependencies, self.dependents, self.waiting,
                self.waiting_data, self.ready, self.who_has, self.stacks,
                self.processing, None, self.released, self.who_wants,
                self.wants_what, tasks=self.tasks, erred=self.exceptions_blame,
                allow_overlap=allow_overlap, allow_bad_stacks=allow_bad_stacks)
        if not (set(self.ncores) == \
                set(self.has_what) == \
                set(self.stacks) == \
                set(self.processing) == \
                set(self.worker_info) == \
                set(self.worker_streams)):
            raise ValueError("Workers not the same in all collections")

        assert self.worker_bytes == {w: sum(self.nbytes[k] for k in keys)
                                     for w, keys in self.has_what.items()}
        for w in self.stacks:
            assert abs(sum(self.stack_durations[w]) - self.stack_duration[w]) < 1e-8
            assert len(self.stack_durations[w]) == len(self.stacks[w])

    ###################
    # Manage Messages #
    ###################

    def report(self, msg):
        """
        Publish updates to all listening Queues and Streams

        If the message contains a key then we only send the message to those
        streams that care about the key.
        """
        for q in self.report_queues:
            q.put_nowait(msg)
        if 'key' in msg:
            streams = [self.streams[c]
                       for c in self.who_wants.get(msg['key'], ())
                       if c in self.streams]
        else:
            streams = self.streams.values()
        for s in streams:
            try:
                s.send(msg)
                # logger.debug("Scheduler sends message to client %s", msg)
            except StreamClosedError:
                logger.critical("Tried writing to closed stream: %s", msg)

    @gen.coroutine
    def add_client(self, stream, client=None):
        """ Add client to network

        We listen to all future messages from this IOStream.
        """
        logger.info("Receive client connection: %s", client)
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
            logger.info("Close client connection: %s", client)

    def remove_client(self, client=None):
        """ Remove client from network """
        logger.info("Remove client %s", client)
        self.client_releases_keys(self.wants_what.get(client, ()), client)
        with ignoring(KeyError):
            del self.wants_what[client]

    @gen.coroutine
    def handle_messages(self, in_queue, report, client=None):
        """
        The master client coroutine.  Handles all inbound messages from clients.

        This runs once per Client IOStream or Queue.

        See Also
        --------
        Scheduler.worker_stream: The equivalent function for workers
        """
        with log_errors(pdb=LOG_PDB):
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
                    msgs = yield next_message()
                except (StreamClosedError, AssertionError, GeneratorExit):
                    break
                except Exception as e:
                    logger.exception(e)
                    put(error_message(e, status='scheduler-error'))
                    continue

                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    # logger.debug("scheduler receives message %s", msg)
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
            logger.debug('Finished handle_messages coroutine')

    def handle_queues(self, scheduler_queue, report_queue):
        """
        Register new control and report queues to the Scheduler

        Queues are not in common use.  This may be deprecated in the future.
        """
        self.scheduler_queues.append(scheduler_queue)
        if report_queue:
            self.report_queues.append(report_queue)
        future = self.handle_messages(scheduler_queue, report_queue)
        self.coroutines.append(future)
        return future

    def send_task_to_worker(self, worker, key):
        """ Send a single computational task to a worker """
        msg = {'op': 'compute-task',
               'key': key}

        deps = self.dependencies[key]
        if deps:
            msg['who_has'] = {dep: tuple(self.who_has.get(dep, ()))
                              for dep in deps}

        task = self.tasks[key]
        if type(task) is dict:
            msg.update(task)
        else:
            msg['task'] = task

        self.worker_streams[worker].send(msg)

    @gen.coroutine
    def worker_stream(self, worker):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_messages: Equivalent coroutine for clients
        """
        yield gen.sleep(0)
        ip, port = coerce_to_address(worker, out=tuple)
        stream = yield connect(ip, port)
        yield write(stream, {'op': 'compute-stream'})
        self.worker_streams[worker].start(stream)
        logger.info("Starting worker compute stream, %s", worker)

        try:
            while True:
                msgs = yield read(stream)
                if not isinstance(msgs, list):
                    msgs = [msgs]

                if worker in self.worker_info:
                    recommendations = OrderedDict()
                    for msg in msgs:
                        # logger.debug("Compute response from worker %s, %s",
                        #              worker, msg)

                        if msg == 'OK':  # from close
                            break

                        self.correct_time_delay(worker, msg)

                        key = msg['key']
                        if msg['status'] == 'OK':
                            r = self.stimulus_task_finished(worker=worker, **msg)
                            recommendations.update(r)
                        elif msg['status'] == 'error':
                            r = self.stimulus_task_erred(worker=worker, **msg)
                            recommendations.update(r)
                        elif msg['status'] == 'missing-data':
                            r = self.stimulus_missing_data(worker=worker,
                                    ensure=False, **msg)
                            recommendations.update(r)
                        else:
                            logger.warn("Unknown message type, %s, %s",
                                    msg['status'], msg)

                    self.transitions(recommendations)

                    if self.validate:
                        logger.info("Messages: %s\nRecommendations: %s",
                                    msgs, recommendations)
                self.ensure_occupied()

        except (StreamClosedError, IOError, OSError):
            logger.info("Worker failed from closed stream: %s", worker)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise
        finally:
            if not stream.closed():
                stream.close()
            self.remove_worker(address=worker)

    def correct_time_delay(self, worker, msg):
        """
        Apply offset time delay in message times.

        Clocks on different workers differ.  We keep track of a relative "now"
        through periodic heartbeats.  We use this known delay to align message
        times to Scheduler local time.  In particular this helps with
        diagnostics.

        Operates in place
        """
        if 'time-delay' in self.worker_info[worker]:
            delay = self.worker_info[worker]['time-delay']
            for key in ['transfer_start', 'transfer_stop', 'time',
                        'compute_start', 'compute_stop', 'disk_load_start',
                        'disk_load_stop']:
                if key in msg:
                    msg[key] += delay

    @gen.coroutine
    def clear_data_from_workers(self):
        """ Send delete signals to clear unused data from workers

        This watches the ``.deleted_keys`` attribute, which stores a set of
        keys to be deleted from each worker.  This function is run periodically
        by the ``._delete_periodic_callback`` to actually remove the data.

        This runs every ``self.delete_interval`` milliseconds.
        """
        if self.deleted_keys:
            d = self.deleted_keys.copy()
            self.deleted_keys.clear()

            coroutines = [self.rpc(addr=worker).delete_data(
                                   keys=list(keys - self.has_what.get(worker,
                                                                      set())),
                                   report=False)
                          for worker, keys in d.items()
                          if keys]
            for worker, keys in d.items():
                logger.debug("Remove %d keys from worker %s", len(keys), worker)
            yield ignore_exceptions(coroutines, socket.error, StreamClosedError)

        raise Return('OK')

    def add_plugin(self, plugin):
        """
        Add external plugin to scheduler

        See https://distributed.readthedocs.io/en/latest/plugins.html
        """
        self.plugins.append(plugin)

    def remove_plugin(self, plugin):
        """ Remove external plugin from scheduler """
        self.plugins.remove(plugin)

    ############################
    # Less common interactions #
    ############################

    @gen.coroutine
    def scatter(self, stream=None, data=None, workers=None, client=None,
            broadcast=False, timeout=2):
        """ Send data out to workers

        See also
        --------
        Scheduler.broadcast:
        """
        start = time()
        while not self.ncores:
            yield gen.sleep(0.2)
            if time() > start + timeout:
                raise gen.TimeoutError("No workers found")

        if workers is not None:
            workers = [self.coerce_address(w) for w in workers]
        ncores = workers if workers is not None else self.ncores
        keys, who_has, nbytes = yield scatter_to_workers(ncores, data,
                                                         report=False,
                                                         serialize=False)

        self.update_data(who_has=who_has, nbytes=nbytes, client=client)

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
        who_has = {key: self.who_has.get(key, ()) for key in keys}

        try:
            data = yield gather_from_workers(who_has, deserialize=False,
                    rpc=self.rpc, close=False)
            result = {'status': 'OK', 'data': data}
        except KeyError as e:
            logger.debug("Couldn't gather keys %s", e)
            result = {'status': 'error', 'keys': e.args}

        raise gen.Return(result)

    @gen.coroutine
    def restart(self, environment=None):
        """ Restart all workers.  Reset local state. """
        n = len(self.ncores)
        with log_errors():
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
            try:
                yield All([nanny.kill() for nanny in nannies])
                logger.debug("Received done signal from nannies")

                while self.ncores:
                    yield gen.sleep(0.01)

                logger.debug("Workers all removed.  Sending startup signal")

                # All quiet
                resps = yield All([nanny.instantiate(close=True,
                    environment=environment) for nanny in nannies])
                assert all(resp == 'OK' for resp in resps)
            finally:
                for nanny in nannies:
                    nanny.close_rpc()

            self.start()

            logger.debug("All workers reporting in")

            self.report({'op': 'restart'})
            for plugin in self.plugins[:]:
                try:
                    plugin.restart(self)
                except Exception as e:
                    logger.exception(e)

    @gen.coroutine
    def broadcast(self, stream=None, msg=None, workers=None, hosts=None,
            nanny=False):
        """ Broadcast message to workers, return all results """
        if workers is None:
            if hosts is None:
                workers = list(self.ncores)
            else:
                workers = []
        if hosts is not None:
            for host in hosts:
                if host in self.host_info:
                    workers.extend([host + ':' + port
                            for port in self.host_info[host]['ports']])
        # TODO replace with worker_list

        if nanny:
            addresses = []
            for addr in workers:
                ip = addr.split(':')[0]
                port = self.worker_info[addr]['services']['nanny']
                addresses.append('%s:%d' % (ip, port))
        else:
            addresses = workers

        results = yield All([send_recv(arg=address, close=True, **msg)
                             for address in addresses])

        raise Return(dict(zip(workers, results)))

    @gen.coroutine
    def rebalance(self, stream=None, keys=None, workers=None):
        """ Rebalance keys so that each worker stores roughly equal bytes

        **Policy**

        This orders the workers by what fraction of bytes of the existing keys
        they have.  It walks down this list from most-to-least.  At each worker
        it sends the largest results it can find and sends them to the least
        occupied worker until either the sender or the recipient are at the
        average expected load.
        """
        with log_errors():
            keys = set(keys or self.who_has)
            workers = set(workers or self.ncores)

            if not keys.issubset(self.who_has):
                raise Return({'status': 'missing-data',
                              'keys': list(keys - set(self.who_has))})

            workers_by_key = {k: self.who_has.get(k, set()) & workers for k in keys}
            keys_by_worker = {w: set() for w in workers}
            for k, v in workers_by_key.items():
                for vv in v:
                    keys_by_worker[vv].add(k)

            worker_bytes = {w: sum(self.nbytes.get(k, 1000) for k in v)
                            for w, v in keys_by_worker.items()}
            avg = sum(worker_bytes.values()) / len(worker_bytes)

            sorted_workers = list(map(first, sorted(worker_bytes.items(),
                                              key=second, reverse=True)))

            recipients = iter(reversed(sorted_workers))
            recipient = next(recipients)
            msgs = []  # (sender, recipient, key)
            for sender in sorted_workers[:len(workers) // 2]:
                sender_keys = {k: self.nbytes.get(k, 1000)
                                for k in keys_by_worker[sender]}
                sender_keys = iter(sorted(sender_keys.items(),
                                          key=second, reverse=True))

                try:
                    while worker_bytes[sender] > avg:
                        while (worker_bytes[recipient] < avg and
                               worker_bytes[sender] > avg):
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
                self.worker_bytes[recipient] += self.nbytes.get(key, 1000)

            result = yield {r: self.rpc(addr=r).delete_data(keys=v, report=False)
                            for r, v in to_senders.items()}

            for sender, recipient, key in msgs:
                self.who_has[key].remove(sender)
                self.has_what[sender].remove(key)
                self.worker_bytes[sender] -= self.nbytes.get(key, 1000)

            raise Return({'status': 'OK'})

    @gen.coroutine
    def replicate(self, stream=None, keys=None, n=None, workers=None,
            branching_factor=2, delete=True):
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
        if delete:
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
                    self.worker_bytes[worker] -= self.nbytes.get(key, 1000)

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

    def workers_to_close(self, memory_ratio=2):
        if not self.idle or self.ready:
            return []

        limit_bytes = {w: self.worker_info[w]['memory_limit']
                        for w in self.worker_info}
        worker_bytes = self.worker_bytes

        limit = sum(limit_bytes.values())
        total = sum(worker_bytes.values())
        idle = sorted(self.idle, key=worker_bytes.get, reverse=True)

        to_close = []

        while idle:
            w = idle.pop()
            limit -= limit_bytes[w]
            if limit >= memory_ratio * total:  # still plenty of space
                to_close.append(w)
            else:
                break

        return to_close

    @gen.coroutine
    def retire_workers(self, stream=None, workers=None, remove=True):
        if workers is None:
            while True:
                try:
                    workers = self.workers_to_close()
                    if workers:
                        yield self.retire_workers(workers=workers, remove=remove)
                    raise gen.Return(list(workers))
                except KeyError:  # keys left during replicate
                    pass

        workers = set(workers)
        keys = set.union(*[self.has_what[w] for w in workers])
        keys = {k for k in keys if self.who_has[k].issubset(workers)}

        other_workers = set(self.worker_info) - workers
        if keys:
            if other_workers:
                yield self.replicate(keys=keys, workers=other_workers, n=1,
                                    delete=False)
            else:
                raise gen.Return([])

        if remove:
            for w in workers:
                self.remove_worker(address=w, safe=True)
        raise gen.Return(list(workers))

    @gen.coroutine
    def synchronize_worker_data(self, stream=None, worker=None):
        if worker is None:
            result = yield {w: self.synchronize_worker_data(worker=w)
                            for w in self.worker_info}
            result = {k: v for k, v in result.items() if any(v.values())}
            if result:
                logger.info("Excess keys found on workers: %s", result)
            raise Return(result or None)
        else:
            keys = yield self.rpc(addr=worker).keys()
            keys = set(keys)

            missing = self.has_what[worker] - keys
            if missing:
                logger.info("Expected data missing from worker: %s, %s",
                            worker, missing)

            extra = keys - self.has_what[worker] - self.deleted_keys[worker]
            if extra:
                yield gen.sleep(self.synchronize_worker_interval / 1000)  # delay
                keys = yield self.rpc(addr=worker).keys()  # check again
                extra &= set(keys)  # make sure the keys are still on worker
                extra -= self.has_what[worker]  # and still unknown to scheduler
                if extra:  # still around?  delete them
                    yield self.rpc(addr=worker).delete_data(keys=list(extra),
                            report=False)

            raise Return({'extra': list(extra), 'missing': list(missing)})

    def add_keys(self, stream=None, address=None, keys=()):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.
        """
        address = coerce_to_address(address)
        if address not in self.worker_info:
            return 'not found'
        for key in keys:
            if key in self.who_has:
                if key not in self.has_what[address]:
                    self.worker_bytes[address] += self.nbytes.get(key, 1000)
                self.has_what[address].add(key)
                self.who_has[key].add(address)
            # else:
                # TODO: delete key from worker
        return 'OK'

    def update_data(self, stream=None, who_has=None, nbytes=None, client=None):
        """
        Learn that new data has entered the network from an external source

        See Also
        --------
        Scheduler.mark_key_in_memory
        """
        with log_errors():
            who_has = {k: [self.coerce_address(vv) for vv in v]
                       for k, v in who_has.items()}
            logger.debug("Update data %s", who_has)
            if client:
                self.client_wants_keys(keys=list(who_has), client=client)

            # for key, workers in who_has.items():  # TODO
            #     self.mark_key_in_memory(key, workers)

            self.nbytes.update(nbytes)

            for key, workers in who_has.items():
                if key not in self.dependents:
                    self.dependents[key] = set()
                if key not in self.dependencies:
                    self.dependencies[key] = set()
                self.task_state[key] = 'memory'
                self.who_has[key] = set(workers)
                for w in workers:
                    if key not in self.has_what[w]:
                        self.worker_bytes[w] += self.nbytes.get(key, 1000)
                    self.has_what[w].add(key)
                self.waiting_data[key] = set()
                self.report({'op': 'key-in-memory',
                             'key': key,
                             'workers': list(workers)})

    def report_on_key(self, key):
        if key not in self.task_state:
            self.report({'op': 'cancelled-key',
                         'key': key})
        elif self.task_state[key] == 'memory':
            self.report({'op': 'key-in-memory',
                         'key': key})
        elif self.task_state[key] == 'erred':
            failing_key = self.exceptions_blame[key]
            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key, None)})


    @gen.coroutine
    def feed(self, stream, function=None, setup=None, teardown=None, interval=1, **kwargs):
        """
        Provides a data stream to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
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

    def get_stacks(self, stream=None, workers=None):
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: list(self.stacks[w]) for w in workers}
        else:
            return valmap(list, self.stacks)

    def get_processing(self, stream=None, workers=None):
        if workers is not None:
            workers = set(map(self.coerce_address, workers))
            return {w: list(self.processing[w]) for w in workers}
        else:
            return valmap(list, self.processing)

    def get_who_has(self, stream=None, keys=None):
        if keys is not None:
            return {k: list(self.who_has.get(k, [])) for k in keys}
        else:
            return valmap(list, self.who_has)

    def get_has_what(self, stream=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: list(self.has_what.get(w, ())) for w in workers}
        else:
            return valmap(list, self.has_what)

    def get_ncores(self, stream=None, workers=None):
        if workers is not None:
            workers = map(self.coerce_address, workers)
            return {w: self.ncores.get(w, None) for w in workers}
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

    def publish_dataset(self, stream=None, keys=None, data=None, name=None,
                        client=None):
        if name in self.datasets:
            raise KeyError("Dataset %s already exists" % name)
        self.client_wants_keys(keys, 'published-%s' % name)
        self.datasets[name] = {'data': data, 'keys': keys}
        return {'status':  'OK', 'name': name}

    def unpublish_dataset(self, stream=None, name=None):
        out = self.datasets.pop(name, {'keys': []})
        self.client_releases_keys(out['keys'], 'published-%s' % name)

    def list_datasets(self, *args):
        return list(sorted(self.datasets.keys()))

    def get_dataset(self, stream, name=None, client=None):
        if name in self.datasets:
            return self.datasets[name]
        else:
            raise KeyError("Dataset '%s' not found" % name)

    def change_worker_cores(self, stream=None, worker=None, diff=0):
        """ Add or remove cores from a worker

        This is used when a worker wants to spin off a long-running task
        """
        self.ncores[worker] += diff
        self.maybe_idle.add(worker)
        self.ensure_occupied()

    #####################
    # State Transitions #
    #####################

    def transition_released_waiting(self, key):
        try:
            if self.validate:
                assert key in self.tasks
                assert key in self.dependencies
                assert key in self.dependents
                assert key not in self.waiting
                # assert key not in self.readyset
                # assert key not in self.rstacks
                assert key not in self.who_has
                assert key not in self.rprocessing
                # assert all(dep in self.task_state
                #            for dep in self.dependencies[key])

            if not all(dep in self.task_state for dep in
                    self.dependencies[key]):
                return {key: 'forgotten'}

            self.waiting[key] = set()

            recommendations = OrderedDict()

            for dep in self.dependencies[key]:
                if dep in self.exceptions_blame:
                    self.exceptions_blame[key] = self.exceptions_blame[dep]
                    recommendations[key] = 'erred'
                    return recommendations

            for dep in self.dependencies[key]:
                if dep not in self.who_has:
                    self.waiting[key].add(dep)
                if dep in self.released:
                    recommendations[dep] = 'waiting'
                else:
                    self.waiting_data[dep].add(key)

            if not self.waiting[key]:
                recommendations[key] = 'ready'

            self.waiting_data[key] = {dep for dep in self.dependents[key]
                                          if dep not in self.who_has
                                          and dep not in self.released
                                          and dep not in self.exceptions_blame}

            self.task_state[key] = 'waiting'
            self.released.remove(key)

            if self.validate:
                assert key in self.waiting
                assert key in self.waiting_data

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_waiting_ready(self, key):
        try:
            if self.validate:
                assert key in self.waiting
                assert not self.waiting[key]
                assert key not in self.who_has
                assert key not in self.exceptions_blame
                assert key not in self.rprocessing
                # assert key not in self.readyset
                assert key not in self.unrunnable
                assert all(dep in self.who_has
                           for dep in self.dependencies[key])

            del self.waiting[key]

            if self.dependencies.get(key, None) or key in self.restrictions:
                new_worker = decide_worker(self.dependencies, self.stacks,
                        self.stack_duration, self.processing, self.who_has,
                        self.has_what, self.restrictions,
                        self.loose_restrictions, self.nbytes, self.ncores, key)
                if not new_worker:
                    self.unrunnable.add(key)
                    self.task_state[key] = 'no-worker'
                else:
                    self.stacks[new_worker].append(key)
                    duration = self.task_duration.get(key_split(key), 0.5)
                    self.stack_durations[new_worker].append(duration)
                    self.stack_duration[new_worker] += duration
                    self.maybe_idle.add(new_worker)
                    self.put_key_in_stealable(key)
                    self.task_state[key] = 'stacks'
            else:
                self.ready.appendleft(key)
                self.task_state[key] = 'queue'

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_ready_processing(self, key, worker=None, latency=5e-3):
        try:
            if self.validate:
                assert key not in self.waiting
                assert key not in self.who_has
                assert key not in self.exceptions_blame
                assert self.task_state[key] in ('queue', 'stacks')
                if self.task_state[key] == 'no-worker':
                    raise ValueError()

            assert worker

            duration = self.task_duration.get(key_split(key), latency*100)
            self.processing[worker][key] = duration
            self.rprocessing[key].add(worker)
            self.occupancy[worker] += duration
            self.task_state[key] = 'processing'
            self.remove_key_from_stealable(key)

            # logger.debug("Send job to worker: %s, %s", worker, key)

            try:
                self.send_task_to_worker(worker, key)
            except StreamClosedError:
                self.remove_worker(worker)

            return {}
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_processing_memory(self, key, nbytes=None, type=None,
            worker=None, compute_start=None, compute_stop=None,
            transfer_start=None, transfer_stop=None, **kwargs):
        try:
            if self.validate:
                assert key in self.rprocessing
                assert all(key in self.processing[w] for w in self.rprocessing[key])
                assert key not in self.waiting
                assert key not in self.who_has
                assert key not in self.exceptions_blame
                # assert all(dep in self.waiting_data[key ] for dep in
                #         self.dependents[key] if self.task_state[dep] in
                #         ['waiting', 'queue', 'stacks'])
                # assert key not in self.nbytes

                assert self.task_state[key] == 'processing'

            if worker not in self.processing:
                return {key: 'released'}

            #############################
            # Update Timing Information #
            #############################
            if compute_start:
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
                if ks in self.stealable_unknown_durations:
                    for k in self.stealable_unknown_durations.pop(ks, ()):
                        if self.task_state.get(k) == 'stacks':
                            self.put_key_in_stealable(k)

                info['last-task'] = compute_stop

            ############################
            # Update State Information #
            ############################
            if nbytes:
                self.nbytes[key] = nbytes

            self.who_has[key] = set()

            if worker:
                self.who_has[key].add(worker)
                self.has_what[worker].add(key)
                self.worker_bytes[worker] += self.nbytes.get(key, 1000)

            if nbytes:
                self.nbytes[key] = nbytes

            workers = self.rprocessing.pop(key)
            for worker in workers:
                self.occupancy[worker] -= self.processing[worker].pop(key)

            recommendations = OrderedDict()

            deps = self.dependents.get(key, [])
            if len(deps) > 1:
               deps = sorted(deps, key=self.priority.get, reverse=True)

            for dep in deps:
                if dep in self.waiting:
                    s = self.waiting[dep]
                    s.remove(key)
                    if not s:  # new task ready to run
                        recommendations[dep] = 'ready'

            for dep in self.dependencies.get(key, []):
                if dep in self.waiting_data:
                    s = self.waiting_data[dep]
                    s.remove(key)
                    if (not s and dep and
                        dep not in self.who_wants and
                        not self.waiting_data.get(dep)):
                        recommendations[dep] = 'released'

            if (not self.waiting_data.get(key) and
                key not in self.who_wants):
                recommendations[key] = 'released'
            else:
                msg = {'op': 'key-in-memory',
                       'key': key}
                if type is not None:
                    msg['type'] = type
                self.report(msg)

            self.task_state[key] = 'memory'

            if self.validate:
                assert key not in self.rprocessing

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_memory_released(self, key, safe=False):
        try:
            if self.validate:
                assert key in self.who_has
                assert key not in self.released
                # assert key not in self.readyset
                assert key not in self.waiting
                assert key not in self.rprocessing
                if safe:
                    assert not self.waiting_data.get(key)
                # assert key not in self.who_wants

            recommendations = OrderedDict()

            for dep in self.waiting_data.get(key, ()):  # lost dependency
                if self.task_state[dep] == 'waiting':
                    self.waiting[dep].add(key)
                else:
                    recommendations[dep] = 'waiting'

            workers = self.who_has.pop(key)
            for w in workers:
                if w in self.worker_info:  # in case worker has died
                    self.has_what[w].remove(key)
                    self.worker_bytes[w] -= self.nbytes.get(key, 1000)
                    self.deleted_keys[w].add(key)

            self.released.add(key)

            self.task_state[key] = 'released'
            self.report({'op': 'lost-data', 'key': key})

            if key not in self.tasks: # pure data
                recommendations[key] = 'forgotten'
            elif not all(dep in self.task_state
                         for dep in self.dependencies[key]):
                recommendations[key] = 'forgotten'
            elif key in self.who_wants or self.waiting_data.get(key):
                recommendations[key] = 'waiting'

            if key in self.waiting_data:
                del self.waiting_data[key]

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_released_erred(self, key):
        try:
            if self.validate:
                with log_errors(pdb=LOG_PDB):
                    assert key in self.exceptions_blame
                    assert key not in self.who_has
                    assert key not in self.waiting
                    assert key not in self.waiting_data

            recommendations = {}

            failing_key = self.exceptions_blame[key]

            for dep in self.dependents[key]:
                self.exceptions_blame[dep] = failing_key
                if dep not in self.who_has:
                    recommendations[dep] = 'erred'

            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key, None)})

            self.task_state[key] = 'erred'
            self.released.remove(key)

            # TODO: waiting data?
            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_waiting_released(self, key):
        try:
            if self.validate:
                assert key in self.waiting
                assert key in self.waiting_data
                assert key not in self.who_has
                assert key not in self.rprocessing

            recommendations = {}

            del self.waiting[key]

            for dep in self.dependencies[key]:
                if dep in self.waiting_data:
                    if key in self.waiting_data[dep]:
                        self.waiting_data[dep].remove(key)
                    if not self.waiting_data[dep] and dep not in self.who_wants:
                        recommendations[dep] = 'released'
                    assert self.task_state[dep] != 'erred'

            self.task_state[key] = 'released'
            self.released.add(key)

            if self.validate:
                assert not any(key in self.waiting_data.get(dep, ())
                               for dep in self.dependencies[key])

            if any(dep not in self.task_state for dep in
                    self.dependencies[key]):
                recommendations[key] = 'forgotten'

            elif (key not in self.exceptions_blame and
                (key in self.who_wants or self.waiting_data.get(key))):
                recommendations[key] = 'waiting'

            del self.waiting_data[key]

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_processing_released(self, key):
        try:
            if self.validate:
                assert key in self.rprocessing
                assert key not in self.who_has
                assert self.task_state[key] == 'processing'

            for w in self.rprocessing.pop(key):
                self.occupancy[w] -= self.processing[w].pop(key)

            self.released.add(key)
            self.task_state[key] = 'released'

            recommendations = OrderedDict()

            if any(dep not in self.task_state
                   for dep in self.dependencies[key]):
                recommendations[key] = 'forgotten'
            elif self.waiting_data[key] or key in self.who_wants:
                recommendations[key] = 'waiting'
            else:
                for dep in self.dependencies[key]:
                    if dep not in self.released:
                        assert key in self.waiting_data[dep]
                        self.waiting_data[dep].remove(key)
                        if not self.waiting_data[dep] and dep not in self.who_wants:
                            recommendations[dep] = 'released'
                del self.waiting_data[key]

            if self.validate:
                assert key not in self.rprocessing

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_ready_released(self, key):
        try:
            if self.validate:
                assert key not in self.who_has
                assert self.task_state[key] in ('stacks', 'queue', 'no-worker')

            if self.task_state[key] == 'no-worker':
                self.unrunnable.remove(key)
            if self.task_state[key] == 'stacks':  # TODO: non-linear
                for w in self.stacks:
                    if key in self.stacks[w]:
                        for i, k in enumerate(self.stacks[w]):
                            if k == key:
                                del self.stacks[w][i]
                                duration = self.stack_durations[w][i]
                                del self.stack_durations[w][i]
                                self.stack_duration[w] -= duration
                            break

            self.released.add(key)
            self.task_state[key] = 'released'

            for dep in self.dependencies[key]:
                try:
                    self.waiting_data[dep].remove(key)
                except KeyError:  # dep may also be released
                    pass
                # TODO: maybe release dep if not about to wait?

            if self.waiting_data[key] or key in self.who_wants:
                recommendations = {key: 'waiting'}
            else:
                recommendations = {}

            del self.waiting_data[key]

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_processing_erred(self, key, cause=None, exception=None,
            traceback=None):
        try:
            if self.validate:
                assert cause or key in self.exceptions_blame
                assert key in self.rprocessing
                assert key not in self.who_has
                assert key not in self.waiting
                # assert key not in self.rstacks
                # assert key not in self.readyset

            if exception:
                self.exceptions[key] = exception
            if traceback:
                self.tracebacks[key] = traceback
            if cause:
                self.exceptions_blame[key] = cause

            failing_key = self.exceptions_blame[key]

            recommendations = {}

            for dep in self.dependents[key]:
                self.exceptions_blame[dep] = key
                recommendations[dep] = 'erred'

            for dep in self.dependencies.get(key, []):
                if dep in self.waiting_data:
                    s = self.waiting_data[dep]
                    if key in s:
                        s.remove(key)
                    if (not s and dep and
                        dep not in self.who_wants and
                        not self.waiting_data.get(dep)):
                        recommendations[dep] = 'released'

            for w in self.rprocessing.pop(key):
                self.occupancy[w] -= self.processing[w].pop(key)

            del self.waiting_data[key]  # do anything with this?

            self.task_state[key] = 'erred'

            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key)})

            if self.validate:
                assert key not in self.rprocessing

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def remove_key(self, key):
        if key in self.tasks:
            del self.tasks[key]
        del self.task_state[key]
        if key in self.dependencies:
            del self.dependencies[key]
        del self.dependents[key]
        if key in self.restrictions:
            del self.restrictions[key]
        if key in self.loose_restrictions:
            self.loose_restrictions.remove(key)
        if key in self.priority:
            del self.priority[key]
        if key in self.exceptions:
            del self.exceptions[key]
        if key in self.exceptions_blame:
            del self.exceptions_blame[key]
        if key in self.released:
            self.released.remove(key)
        if key in self.waiting_data:
            del self.waiting_data[key]
        if key in self.suspicious_tasks:
            del self.suspicious_tasks[key]
        if key in self.nbytes:
            del self.nbytes[key]

    def transition_memory_forgotten(self, key):
        try:
            if self.validate:
                assert key in self.dependents
                assert self.task_state[key] == 'memory'
                assert key in self.waiting_data
                assert key in self.who_has
                assert key not in self.rprocessing
                # assert key not in self.ready
                assert key not in self.waiting

            recommendations = {}

            for dep in self.waiting_data[key]:
                recommendations[dep] = 'forgotten'

            for dep in self.dependents[key]:
                if self.task_state[dep] == 'released':
                    recommendations[dep] = 'forgotten'

            for dep in self.dependencies.get(key, ()):
                try:
                    s = self.dependents[dep]
                    s.remove(key)
                    if not s and dep not in self.who_wants:
                        assert dep is not key
                        recommendations[dep] = 'forgotten'
                except KeyError:
                    pass

            workers = self.who_has.pop(key)
            for w in workers:
                if w in self.worker_info:  # in case worker has died
                    self.has_what[w].remove(key)
                    self.worker_bytes[w] -= self.nbytes.get(key, 1000)
                    self.deleted_keys[w].add(key)

            if self.validate:
                assert all(key not in self.dependents[dep]
                            for dep in self.dependencies[key]
                            if dep in self.task_state)
                assert all(key not in self.waiting_data.get(dep, ())
                            for dep in self.dependencies[key]
                            if dep in self.task_state)

            self.remove_key(key)

            self.report_on_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_released_forgotten(self, key):
        try:
            if self.validate:
                assert key in self.dependencies
                assert self.task_state[key] in ('released', 'erred')
                # assert not self.waiting_data[key]
                if key in self.tasks and self.dependencies[key].issubset(self.task_state):
                    assert key not in self.who_wants
                    assert not self.dependents[key]
                    assert not any(key in self.waiting_data.get(dep, ())
                                   for dep in self.dependencies[key])
                assert key not in self.who_has
                assert key not in self.rprocessing
                # assert key not in self.ready
                assert key not in self.waiting

            recommendations = {}
            for dep in self.dependencies[key]:
                try:
                    s = self.dependents[dep]
                    s.remove(key)
                    if not s and dep not in self.who_wants:
                        assert dep is not key
                        recommendations[dep] = 'forgotten'
                except KeyError:
                    pass

            for dep in self.dependents[key]:
                if self.task_state[dep] not in ('memory', 'error'):
                    recommendations[dep] = 'forgotten'

            for dep in self.dependents[key]:
                if self.task_state[dep] == 'released':
                    recommendations[dep] = 'forgotten'

            for dep in self.dependencies[key]:
                try:
                    self.waiting_data[dep].remove(key)
                except KeyError:
                    pass

            if self.validate:
                assert all(key not in self.dependents[dep]
                            for dep in self.dependencies[key]
                            if dep in self.task_state)
                assert all(key not in self.waiting_data.get(dep, ())
                            for dep in self.dependencies[key]
                            if dep in self.task_state)

            self.remove_key(key)

            self.report_on_key(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition(self, key, finish, *args, **kwargs):
        """ Transition a key from its current state to the finish state

        Examples
        --------
        >>> self.transition('x', 'waiting')
        {'x': 'ready'}

        Returns
        -------
        Dictionary of recommendations for future transitions

        See Also
        --------
        Scheduler.transitions: transitive version of this function
        """
        try:
            try:
                start = self.task_state[key]
            except KeyError:
                return {}
            if start == finish:
                return {}

            if (start, finish) in self._transitions:
                func = self._transitions[start, finish]
                recommendations = func(key, *args, **kwargs)
            else:
                func = self._transitions['released', finish]
                assert not args and not kwargs
                a = self.transition(key, 'released')
                if key in a:
                    func = self._transitions['released', a[key]]
                b = func(key)
                a = a.copy()
                a.update(b)
                recommendations = a
                start = 'released'
            finish2 = self.task_state.get(key, 'forgotten')
            self.transition_log.append((key, start, finish2, recommendations))
            if self.validate:
                logger.info("Transition %s->%s: %s New: %s",
                            start, finish2, key, recommendations)
            for plugin in self.plugins:
                try:
                    plugin.transition(key, start, finish2, *args, **kwargs)
                except Exception:
                    logger.info("Plugin failed with exception", exc_info=True)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transitions(self, recommendations):
        """ Process transitions until none are left

        This includes feedback from previous transitions and continues until we
        reach a steady state
        """
        keys = set()
        recommendations = recommendations.copy()
        while recommendations:
            key, finish = recommendations.popitem()
            keys.add(key)
            new = self.transition(key, finish)
            recommendations.update(new)

        if self.validate:
            for key in keys:
                self.validate_key(key)

    def transition_story(self, *keys):
        """ Get all transitions that touch one of the input keys """
        keys = set(keys)
        return [t for t in self.transition_log
                if t[0] in keys or keys.intersection(t[3])]

    ##############################
    # Assigning Tasks to Workers #
    ##############################

    def ensure_occupied(self):
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

        See also
        --------
        Scheduler.ensure_occupied_queue
        Scheduler.ensure_occupied_stacks
        Scheduler.work_steal
        """
        with log_errors(pdb=LOG_PDB):
            for worker in self.maybe_idle:
                self.ensure_occupied_stacks(worker)
            self.maybe_idle.clear()

            if self.idle and self.ready:
                if len(self.ready) < len(self.idle):
                    def keyfunc(w):
                        return (-len(self.stacks[w]) - len(self.processing[w]),
                                -len(self.has_what.get(w, ())))
                    for worker in topk(len(self.ready), self.idle, key=keyfunc):
                        self.ensure_occupied_queue(worker, count=1)
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
                            self.ensure_occupied_queue(worker, count=count)

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
                            if self.task_state.get(task) != 'queue':
                                continue
                            total_free_time -= self.task_duration.get(key_split(task), 1)
                            tasks.append(task)

                        self.ready.extend(tasks[::-1])

                        counts = divide_n_among_bins(len(tasks), free_time2)
                        for worker, count in zip(workers2, counts):
                            self.ensure_occupied_queue(worker, count=count)

            if self.idle and any(self.stealable):
                thieves = self.work_steal()
                for worker in thieves:
                    self.ensure_occupied_stacks(worker)

    def ensure_occupied_stacks(self, worker):
        """ Send tasks to worker while it has tasks and free cores

        These tasks may come from the worker's own stacks or from the global
        ready deque.

        We update the idle workers set appropriately.

        See Also
        --------
        Scheduler.ensure_occupied
        Scheduler.ensure_occupied_queue
        """
        stack = self.stacks[worker]
        latency = 5e-3

        while (stack and
               (self.ncores[worker] > len(self.processing[worker]) or
                self.occupancy[worker] < latency * self.ncores[worker])):
            key = stack.pop()
            duration = self.stack_durations[worker].pop()
            self.stack_duration[worker] -= duration

            if self.task_state.get(key) == 'stacks':
                r = self.transition(key, 'processing',
                                    worker=worker, latency=latency)

        if stack:
            self.saturated.add(worker)
            if worker in self.idle:
                self.idle.remove(worker)
        else:
            if worker in self.saturated:
                self.saturated.remove(worker)
            self._check_idle(worker)

    def put_key_in_stealable(self, key):
        ratio, loc = self.steal_time_ratio(key)
        if ratio is not None:
            self.stealable[loc].add(key)
            self.key_stealable[key] = loc

    def remove_key_from_stealable(self, key):
        loc = self.key_stealable.pop(key, None)
        if loc is not None:
            try:
                self.stealable[loc].remove(key)
            except:
                pass

    def ensure_occupied_queue(self, worker, count):
        """
        Send at most count tasks from the ready queue to the specified worker

        See also
        --------
        Scheduler.ensure_occupied
        Scheduler.ensure_occupied_stacks
        """
        for i in range(count):
            try:
                key = self.ready.pop()
                while self.task_state.get(key) != 'queue':
                    key = self.ready.pop()
            except (IndexError, KeyError):
                break

            if self.task_state[key] == 'queue':
                r = self.transition(key, 'processing', worker=worker)

        self._check_idle(worker)

    def work_steal(self):
        """ Steal tasks from saturated workers to idle workers

        This moves tasks from the bottom of the stacks of over-occupied workers
        to the stacks of idling workers.

        See also
        --------
        Scheduler.ensure_occupied
        """
        if not self.steal:
            return []
        with log_errors():
            thieves = set()
            for level, stealable in enumerate(self.stealable[:-1]):
                if not stealable:
                    continue
                if len(self.idle) == len(self.ncores):  # no stacks
                    stealable.clear()
                    continue
                # Enough idleness to continue?
                ratio = 2 ** (level - 3)
                n_saturated = len(self.ncores) - len(self.idle)
                duration_if_hold = len(stealable) / n_saturated
                duration_if_steal = ratio
                if level > 1 and duration_if_hold < duration_if_steal:
                    break
                while stealable and self.idle:
                    for w in list(self.idle):
                        try:
                            key = stealable.pop()
                        except:
                            break
                        else:
                            if self.task_state.get(key, 'stacks'):
                                self.stacks[w].append(key)
                                duration = self.task_duration.get(key_split(key), 0.5)
                                self.stack_durations[w].append(duration)
                                self.stack_duration[w] += duration

                                thieves.add(w)
                                if (self.ncores[w] <=
                                    len(self.processing[w]) + len(self.stacks[w])):
                                    self.idle.remove(w)

                if stealable:
                    break
            logger.debug('Stolen tasks for %d workers', len(thieves))
            return thieves

    def steal_time_ratio(self, key, bandwidth=BANDWIDTH):
        """ The compute to communication time ratio of a key

        Returns
        -------

        ratio: The compute/communication time ratio of the task
        loc: The self.stealable bin into which this key should go
        """
        if key in self.restrictions and key not in self.loose_restrictions:
            return None, None  # don't steal

        nbytes = sum(self.nbytes.get(k, 1000) for k in self.dependencies[key])
        transfer_time = nbytes / bandwidth
        split = key_split(key)
        if split in fast_tasks:
            return None, None
        try:
            compute_time = self.task_duration[split]
        except KeyError:
            self.stealable_unknown_durations[split].add(key)
            return None, None
        else:
            try:
                ratio = compute_time / transfer_time
            except ZeroDivisionError:
                ratio = 10000
            if ratio > 8:
                loc = 0
            elif ratio < 2**-8:
                loc = -1
            else:
                loc = int(-round(log(ratio) / log(2), 0) + 3)
            return ratio, loc

    def issaturated(self, worker, latency=5e-3):
        """
        Determine if a worker has enough work to avoid being idle

        A worker is saturated if the following criteria are met

        1.  It is working on at least as many tasks as it has cores
        2.  The expected time it will take to complete all of its currently
            assigned  tasks is at least a full round-trip time.  This is
            relevant when it has many small tasks
        """
        return (len(self.stacks[worker]) + len(self.processing[worker])
                > self.ncores[worker] and
                self.occupancy[worker] > latency * self.ncores[worker])

    def _check_idle(self, worker, latency=5e-3):
        if not self.issaturated(worker, latency=latency):
            self.idle.add(worker)
        elif worker in self.idle:
            self.idle.remove(worker)

    #####################
    # Utility functions #
    #####################

    def coerce_address(self, addr):
        """
        Coerce possible input addresses to canonical form

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

    def workers_list(self, workers):
        """
        List of qualifying workers

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

    def start_ipython(self, stream=None):
        """Start an IPython kernel

        Returns Jupyter connection info dictionary.
        """
        from ._ipython_utils import start_ipython
        if self._ipython_kernel is None:
            self._ipython_kernel = start_ipython(
                ip=self.ip,
                ns={'scheduler': self},
                log=logger,
            )
        return self._ipython_kernel.get_connection_info()


def decide_worker(dependencies, stacks, stack_duration, processing, who_has,
        has_what, restrictions, loose_restrictions, nbytes, ncores, key):

    """ Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> stacks = {'alice:8000': ['z'], 'bob:8000': []}
    >>> processing = {'alice:8000': set(), 'bob:8000': set()}
    >>> who_has = {'a': {'alice:8000'}}
    >>> has_what = {'alice:8000': {'a'}}
    >>> nbytes = {'a': 100}
    >>> ncores = {'alice:8000': 1, 'bob:8000': 1}
    >>> restrictions = {}
    >>> loose_restrictions = set()

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, ncores, 'b')
    'alice:8000'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice:8000', 'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'a'}}
    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, ncores, 'b')
    'bob:8000'

    Optionally provide restrictions of where jobs are allowed to occur

    >>> restrictions = {'b': {'alice', 'charlie'}}
    >>> decide_worker(dependencies, stacks, processing, who_has, has_what,
    ...               restrictions, loose_restrictions, nbytes, ncores, 'b')
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
    ...               {}, set(), nbytes, ncores, 'c')
    'bob:8000'
    """
    deps = dependencies[key]
    assert all(d in who_has for d in deps)
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
                    return decide_worker(dependencies, stacks, stack_duration,
                            processing, who_has, has_what, {}, set(), nbytes,
                            ncores, key)
                else:
                    return None
    if not workers or not stacks:
        return None

    if len(workers) == 1:
        return first(workers)

    # Select worker that will finish task first
    def objective(w):
        comm_bytes = sum([nbytes.get(k, 1000) for k in dependencies[key]
                          if w not in who_has[k]])
        stack_time = stack_duration[w] / ncores[w]
        start_time = comm_bytes / BANDWIDTH + stack_time
        return start_time

    return min(workers, key=objective)


def validate_state(dependencies, dependents, waiting, waiting_data, ready,
        who_has, stacks, processing, finished_results, released,
        who_wants, wants_what, tasks=None, allow_overlap=False, allow_bad_stacks=False,
        erred=None, **kwargs):
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
        assert v, "waiting on empty set"
        assert v.issubset(dependencies[k]), "waiting set not dependencies"
        for vv in v:
            assert vv not in who_has, ("waiting dependency in memory", k, vv)
            assert vv not in released, ("dependency released", k, vv)
        for dep in dependencies[k]:
            assert dep in v or who_has.get(dep), ("dep missing", k, dep)

    for k, v in waiting_data.items():
        for vv in v:
            if vv in released:
                raise ValueError('dependent not in play', k, vv)
            if not (vv in ready_set or
                    vv in waiting or
                    vv in in_stacks or
                    vv in in_processing):
                raise ValueError('dependent not in play2', k, vv)

    for v in concat(processing.values()):
        assert v in dependencies, "all processing keys in dependencies"

    for key in who_has:
        assert key in waiting_data or key in who_wants

    @memoize
    def check_key(key):
        """ Validate a single key, recurse downwards """
        vals = ([key in waiting,
                 key in ready,
                 key in in_stacks,
                 key in in_processing,
                 not not who_has.get(key),
                 key in released,
                 key in erred])
        if ((allow_overlap and sum(vals) < 1) or
            (not allow_overlap and sum(vals) != 1)):
            if not (in_stacks and waiting):  # known ok state
                raise ValueError("Key exists in wrong number of places", key, vals)

        for dep in dependencies[key]:
            if dep in dependents:
                check_key(dep)  # Recursive case

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


fast_tasks = {'rechunk-split', 'shuffle-split'}


class KilledWorker(Exception):
    pass
