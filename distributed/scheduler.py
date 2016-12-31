from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque, OrderedDict
from datetime import datetime, timedelta
from functools import partial
import logging
import math
from math import log
import os
import pickle
import random
import socket
from timeit import default_timer

from sortedcontainers import SortedSet
try:
    from cytoolz import frequencies, topk
except ImportError:
    from toolz import frequencies, topk
from toolz import memoize, valmap, first, second, keymap, unique, concat, merge
from tornado import gen
from tornado.gen import Return
from tornado.ioloop import IOLoop, PeriodicCallback
from tornado.iostream import StreamClosedError, IOStream

from dask.compatibility import PY3, unicode
from dask.core import reverse_dict
from dask.order import order

from .batched import BatchedSend
from .config import config
from .core import (rpc, connect, read, write, close, MAX_BUFFER_SIZE,
        Server, send_recv, coerce_to_address, error_message, clean_exception)
from .metrics import time
from .publish import PublishExtension
from .channels import ChannelScheduler
from .stealing import WorkStealing
from .utils import (All, ignoring, get_ip, ignore_exceptions,
        ensure_ip, get_fileno_limit, log_errors, key_split, mean,
        divide_n_among_bins, validate_key)
from .utils_comm import (scatter_to_workers, gather_from_workers)
from .versions import get_versions


logger = logging.getLogger(__name__)

BANDWIDTH = config.get('bandwidth', 100e6)
ALLOWED_FAILURES = config.get('allowed-failures', 3)

LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)
DEFAULT_DATA_SIZE = config.get('default-data-size', 1000)


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
        released, waiting, queue, no-worker, processing, memory, erred
    * **priority:** ``{key: tuple}``:
        A score per key that determines its priority
    * **waiting:** ``{key: {key}}``:
        Dictionary like dependencies but excludes keys already computed
    * **waiting_data:** ``{key: {key}}``:
        Dictionary like dependents but excludes keys already computed
    * **ready:** ``deque(key)``
        Keys that are ready to run, but not yet assigned to a worker
    * **processing:** ``{worker: {key: cost}}``:
        Set of keys currently in execution on each worker and their expected
        duration
    * **rprocessing:** ``{key: worker}``:
        The worker currently executing a particular task
    * **who_has:** ``{key: {worker}}``:
        Where each key lives.  The current state of distributed memory.
    * **has_what:** ``{worker: {key}}``:
        What worker has what keys.  The transpose of who_has.
    * **released:** ``{keys}``
        Set of keys that are known, but released from memory
    * **unrunnable:** ``{key}``
        Keys that we are unable to run
    * **host_restrictions:** ``{key: {hostnames}}``:
        A set of hostnames per key of where that key can be run.  Usually this
        is empty unless a key has been specifically restricted to only run on
        certain hosts.
    * **worker_restrictions:** ``{key: {workers}}``:
        Like host_restrictions except that these include specific host:port
        worker names
    * **loose_restrictions:** ``{key}``:
        Set of keys for which we are allow to violate restrictions (see above)
        if not valid workers are present.
    * **resource_restrictions:** ``{key: {str: Number}}``:
        Resources required by a task, such as ``{'GPU': 1}`` or
        ``{'memory': 1e9}``.  These names must match resources specified when
        creating workers.
    * **worker_resources:** ``{worker: {str: Number}}``:
        The available resources on each worker like ``{'gpu': 2, 'mem': 1e9}``.
        These are abstract quantities that constrain certain tasks from running
        at the same time.
    * **used_resources:** ``{worker: {str: Number}}``:
        The sum of each resource used by all tasks allocated to a particular
        worker.
    * **exceptions:** ``{key: Exception}``:
        A dict mapping keys to remote exceptions
    * **tracebacks:** ``{key: list}``:
        A dict mapping keys to remote tracebacks stored as a list of strings
    * **exceptions_blame:** ``{key: key}``:
        A dict mapping a key to another key on which it depends that has failed
    * **suspicious_tasks:** ``{key: int}``
        Number of times a task has been involved in a worker failure
    * **deleted_keys:** ``{key: {workers}}``
        Locations of workers that have keys that should be deleted
    * **wants_what:** ``{client: {key}}``:
        What keys are wanted by each client..  The transpose of who_wants.
    * **who_wants:** ``{key: {client}}``:
        Which clients want each key.  The active targets of computation.
    * **nbytes:** ``{key: int}``:
        Number of bytes for a key as reported by workers holding that key.
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
    * **loop:** ``IOLoop``:
        The running Tornado IOLoop
    * **streams:** ``[IOStreams]``:
        A list of Tornado IOStreams from which we both accept stimuli and
        report results
    * **task_duration:** ``{key-prefix: time}``
        Time we expect certain functions to take, e.g. ``{'sum': 0.25}``
    * **coroutines:** ``[Futures]``:
        A list of active futures that control operation
    """
    default_port = 8786

    def __init__(self, center=None, loop=None,
            max_buffer_size=MAX_BUFFER_SIZE, delete_interval=500,
            synchronize_worker_interval=60000,
            ip=None, services=None, allowed_failures=ALLOWED_FAILURES,
            extensions=[ChannelScheduler, PublishExtension, WorkStealing],
            validate=False,
            **kwargs):

        # Attributes
        self.ip = ip or get_ip()
        self.allowed_failures = allowed_failures
        self.validate = validate
        self.status = None
        self.delete_interval = delete_interval
        self.synchronize_worker_interval = synchronize_worker_interval
        self.digests = None

        # Communication state
        self.loop = loop or IOLoop.current()
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
        self.rprocessing = dict()
        self.task_duration = {prefix: 0.00001 for prefix in fast_tasks}
        self.unknown_durations = defaultdict(set)
        self.host_restrictions = dict()
        self.worker_restrictions = dict()
        self.resource_restrictions = dict()
        self.loose_restrictions = set()
        self.suspicious_tasks = defaultdict(lambda: 0)
        self.waiting = dict()
        self.waiting_data = dict()
        self.ready = deque()
        self.unrunnable = set()
        self.idle = set()
        self.who_has = dict()
        self.has_what = dict()
        self.who_wants = defaultdict(set)
        self.wants_what = defaultdict(set)
        self.exceptions = dict()
        self.tracebacks = dict()
        self.exceptions_blame = dict()
        self.datasets = dict()
        self.n_tasks = 0

        self.idle = SortedSet()
        self.saturated = set()

        # Worker state
        self.ncores = dict()
        self.workers = SortedSet()
        self.total_ncores = 0
        self.total_occupancy = 0
        self.worker_info = dict()
        self.host_info = defaultdict(dict)
        self.worker_resources = dict()
        self.used_resources = dict()
        self.resources = defaultdict(dict)
        self.aliases = dict()
        self.occupancy = dict()

        self.extensions = {}
        self.plugins = []
        self.transition_log = deque(maxlen=config.get('transition-log-length',
                                                      100000))
        self._transition_counter = 0

        self.worker_handlers = {'task-finished': self.handle_task_finished,
                                'task-erred': self.handle_task_erred,
                                'missing-data': self.handle_missing_data,
                                'add-keys': self.add_keys}

        self.compute_handlers = {'update-graph': self.update_graph,
                                 'client-desires-keys': self.client_desires_keys,
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
                         'processing': self.get_processing,
                         'nbytes': self.get_nbytes,
                         'versions': self.get_versions,
                         'add_keys': self.add_keys,
                         'rebalance': self.rebalance,
                         'replicate': self.replicate,
                         'start_ipython': self.start_ipython,
                         'update_data': self.update_data}

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
                 ('waiting', 'processing'): self.transition_waiting_processing,
                 ('waiting', 'released'): self.transition_waiting_released,
                 ('processing', 'released'): self.transition_processing_released,
                 ('processing', 'memory'): self.transition_processing_memory,
                 ('processing', 'erred'): self.transition_processing_erred,
                 ('no-worker', 'released'): self.transition_no_worker_released,
                 ('no-worker', 'waiting'): self.transition_no_worker_waiting,
                 ('released', 'forgotten'): self.transition_released_forgotten,
                 ('memory', 'forgotten'): self.transition_memory_forgotten,
                 ('erred', 'forgotten'): self.transition_released_forgotten,
                 ('memory', 'released'): self.transition_memory_released,
                 ('released', 'erred'): self.transition_released_erred
        }

        connection_limit = get_fileno_limit() / 2

        super(Scheduler, self).__init__(handlers=self.handlers,
                max_buffer_size=max_buffer_size, io_loop=self.loop,
                connection_limit=connection_limit, deserialize=False, **kwargs)

        for ext in extensions:
            ext(self)

    ##################
    # Administration #
    ##################

    def __str__(self):
        return '<Scheduler: "%s:%d" processes: %d cores: %d>' % (
                self.ip, self.port, len(self.workers), self.total_ncores)

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
             'workers': list(self.workers),
             'services': {key: v.port for (key, v) in self.services.items()},
             'workers': dict(self.worker_info)}
        return d

    def get_versions(self, stream):
        """ Basic information about ourselves and our cluster """
        return get_versions()

    def start(self, port=8786, start_queues=True):
        """ Clear out old state and restart all running coroutines """
        collections = [self.tasks, self.dependencies, self.dependents,
                self.waiting, self.waiting_data, self.released, self.priority,
                self.nbytes, self.host_restrictions, self.worker_restrictions,
                self.loose_restrictions, self.ready, self.who_wants,
                self.wants_what, self.unknown_durations]
        for collection in collections:
            collection.clear()

        with ignoring(AttributeError):
            for c in self._worker_coroutines:
                c.cancel()

        for cor in self.coroutines:
            if cor.done():
                exc = cor.exception()
                if exc:
                    raise exc

        if self.status != 'running':
            self.listen(port)

            self.status = 'running'
            logger.info("  Scheduler at: %20s:%s", self.ip, self.port)
            for k, v in self.services.items():
                logger.info("%11s at: %20s:%s", k, self.ip, v.port)

        return self.finished()

    @gen.coroutine
    def finished(self):
        """ Wait until all coroutines have ceased """
        while any(not c.done() for c in self.coroutines):
            yield All(self.coroutines)

    def close_streams(self):
        """ Close all active IOStreams """
        for stream in self.streams.values():
            close(stream.stream)
        self.rpc.close()

    @gen.coroutine
    def close(self, stream=None, fast=False):
        """ Send cleanup signal to all coroutines then wait until finished

        See Also
        --------
        Scheduler.cleanup
        """
        if self.status == 'closed':
            return
        for service in self.services.values():
            service.stop()
        for ext in self.extensions:
            with ignoring(AttributeError):
                ext.teardown()
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

    ###########
    # Stimuli #
    ###########

    def add_worker(self, stream=None, address=None, keys=(), ncores=None,
                   name=None, coerce_address=True, nbytes=None, now=None,
                   resources=None, host_info=None, **info):
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
            if resources:
                self.add_resources(address, resources)
                self.worker_info[address]['resources'] = resources

            if address in self.workers:
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
            self.workers.add(address)
            self.total_ncores += ncores
            self.aliases[name] = address
            self.worker_info[address]['name'] = name

            if address not in self.processing:
                self.has_what[address] = set()
                self.worker_bytes[address] = 0
                self.processing[address] = dict()
                self.occupancy[address] = 0
                self.check_idle_saturated(address)

            if nbytes is not None:
                self.nbytes.update(nbytes)

            # for key in keys:  # TODO
            #     self.mark_key_in_memory(key, [address])

            self.worker_streams[address] = BatchedSend(interval=5, loop=self.loop)
            self._worker_coroutines.append(self.handle_worker(address))

            if self.ncores[address] > len(self.processing[address]):
                self.idle.add(address)

            recommendations = {}
            for key in list(self.unrunnable):
                valid = self.valid_workers(key)
                if valid is True or address in valid:
                    recommendations[key] = 'waiting'

            for plugin in self.plugins[:]:
                try:
                    plugin.add_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if recommendations:
                self.transitions(recommendations)

            logger.info("Register %s", str(address))
            return 'OK'

    def update_graph(self, client=None, tasks=None, keys=None,
                     dependencies=None, restrictions=None, priority=None,
                     loose_restrictions=None, resources=None):
        """
        Add new computations to the internal dask graph

        This happens whenever the Client calls submit, map, get, or compute.
        """
        start = time()
        original_keys = keys
        keys = set(keys)
        self.client_desires_keys(keys=keys, client=client)

        for k in list(tasks):
            if tasks[k] is k:
                del tasks[k]
            if k in self.tasks:
                del tasks[k]

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
            restrictions = {k: v for k, v in restrictions.items()
                                 if v is not None}
            restrictions = {k: set(map(self.coerce_address, v))
                            for k, v in restrictions.items()}
            worker_restrictions = {k: {w for w in s if ':' in w}
                                    for k, s in restrictions.items()}
            host_restrictions = {k: {w for w in s if ':' not in w}
                                 for k, s in restrictions.items()}
            self.worker_restrictions.update(worker_restrictions)
            self.host_restrictions.update(host_restrictions)

            if loose_restrictions:
                self.loose_restrictions |= set(loose_restrictions)

        if resources:
            self.resource_restrictions.update(resources)

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
                self.report_on_key(key, client=client)

        end = time()
        if self.digests is not None:
            self.digests['update-graph-duration'].add(end - start)

        # TODO: balance workers

    def stimulus_task_finished(self, key=None, worker=None, **kwargs):
        """ Mark that a task has finished execution on a particular worker """
        logger.debug("Stimulus task finished %s, %s", key, worker)
        if key not in self.task_state:
            return {}

        if self.task_state[key] == 'processing':
            recommendations = self.transition(key, 'memory', worker=worker,
                                              **kwargs)
        else:
            recommendations = {}

        if self.task_state[key] == 'memory':
            if key not in self.has_what[worker]:
                self.worker_bytes[worker] += self.nbytes.get(key,
                                                             DEFAULT_DATA_SIZE)
            self.who_has[key].add(worker)
            self.has_what[worker].add(key)

        return recommendations

    def stimulus_task_erred(self, key=None, worker=None,
                        exception=None, traceback=None, **kwargs):
        """ Mark that a task has erred on a particular worker """
        logger.debug("Stimulus task erred %s, %s", key, worker)

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
        with log_errors():
            logger.debug("Stimulus missing data %s, %s", key, worker)
            if key and self.task_state.get(key) in (None, 'memory'):
                return {}

            recommendations = OrderedDict()
            for k in set(keys):
                if self.task_state.get(k) == 'memory':
                    for w in set(self.who_has[k]):
                        self.has_what[w].remove(k)
                        self.who_has[k].remove(w)
                        self.worker_bytes[w] -= self.nbytes.get(k,
                                                        DEFAULT_DATA_SIZE)
                    recommendations[k] = 'released'

            if key:
                recommendations[key] = 'released'

            self.transitions(recommendations)

            return {}

    def remove_worker(self, stream=None, address=None, safe=False):
        """
        Remove worker from cluster

        We do this when a worker reports that it plans to leave or when it
        appears to be unresponsive.  This may send its tasks back to a released
        state.
        """
        with log_errors():
            address = self.coerce_address(address)
            logger.info("Remove worker %s", address)
            if address not in self.processing:
                return 'already-removed'
            with ignoring(AttributeError):
                stream = self.worker_streams[address].stream
                close(stream)

            host, port = address.split(':')

            self.host_info[host]['cores'] -= self.ncores[address]
            self.host_info[host]['ports'].remove(port)
            self.total_ncores -= self.ncores[address]

            if not self.host_info[host]['ports']:
                del self.host_info[host]

            del self.worker_streams[address]
            del self.ncores[address]
            self.workers.remove(address)
            del self.aliases[self.worker_info[address]['name']]
            del self.worker_info[address]
            if address in self.idle:
                self.idle.remove(address)
            if address in self.saturated:
                self.saturated.remove(address)

            recommendations = OrderedDict()

            in_flight = set(self.processing.pop(address))
            for k in list(in_flight):
                # del self.rprocessing[k]
                if not safe:
                    self.suspicious_tasks[k] += 1
                if not safe and self.suspicious_tasks[k] > self.allowed_failures:
                    e = pickle.dumps(KilledWorker(k, address))
                    r = self.transition(k, 'erred', exception=e, cause=k)
                    recommendations.update(r)
                    in_flight.remove(k)
                else:
                    recommendations[k] = 'released'

            self.total_occupancy -= self.occupancy.pop(address)
            del self.worker_bytes[address]
            self.remove_resources(address)

            for key in self.has_what.pop(address):
                self.who_has[key].remove(address)
                if not self.who_has[key]:
                    if key in self.tasks:
                        recommendations[key] = 'released'
                    else:
                        recommendations[key] = 'forgotten'

            self.transitions(recommendations)

            for plugin in self.plugins[:]:
                try:
                    plugin.remove_worker(scheduler=self, worker=address)
                except Exception as e:
                    logger.exception(e)

            if not self.processing:
                logger.info("Lost all workers")

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

    def client_desires_keys(self, keys=None, client=None):
        for k in keys:
            self.who_wants[k].add(client)
            self.wants_what[client].add(k)

            if self.task_state.get(k) in ('memory', 'erred'):
                self.report_on_key(k, client=client)

    def client_releases_keys(self, keys=None, client=None):
        """ Remove keys from client desired list """
        keys2 = set()
        for key in list(keys):
            if key in self.wants_what[client]:
                self.wants_what[client].remove(key)
                s = self.who_wants[key]
                s.remove(client)
                if not s:
                    del self.who_wants[key]
                    keys2.add(key)

        for key in keys2:
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
        w = self.rprocessing[key]
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

    def validate_no_worker(self, key):
        assert key in self.unrunnable
        assert key not in self.waiting
        assert key not in self.released
        assert key not in self.rprocessing
        assert key not in self.who_has
        for dep in self.dependencies[key]:
            assert dep in self.who_has

    def validate_key(self, key):
        try:
            try:
                func = getattr(self, 'validate_' + self.task_state[key].replace('-', '_'))
            except KeyError:
                logger.debug("Key lost: %s", key)
            except AttributeError:
                logger.info("self.validate_%s not found",
                            self.task_state[key].replace('-', '_'))
            else:
                func(key)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def validate_state(self, allow_overlap=False):
        validate_state(self.dependencies, self.dependents, self.waiting,
                self.waiting_data, self.ready, self.who_has,
                self.processing, None, self.released, self.who_wants,
                self.wants_what, tasks=self.tasks, erred=self.exceptions_blame,
                allow_overlap=allow_overlap)
        if not (set(self.ncores) ==
                set(self.workers) ==
                set(self.has_what) ==
                set(self.processing) ==
                set(self.worker_info) ==
                set(self.worker_streams)):
            raise ValueError("Workers not the same in all collections")

        assert self.worker_bytes == {w: sum(self.nbytes[k] for k in keys)
                                     for w, keys in self.has_what.items()}

        for key, workers in self.who_has.items():
            for worker in workers:
                assert key in self.has_what[worker]

        for worker, keys in self.has_what.items():
            for key in keys:
                assert worker in self.who_has[key]

        for worker, occ in self.occupancy.items():
            assert abs(sum(self.processing[worker].values()) - occ) < 1e-8

    ###################
    # Manage Messages #
    ###################

    def report(self, msg, client=None):
        """
        Publish updates to all listening Queues and Streams

        If the message contains a key then we only send the message to those
        streams that care about the key.
        """
        if client is not None:
            try:
                stream = self.streams[client]
                stream.send(msg)
            except StreamClosedError:
                logger.critical("Tried writing to closed stream: %s", msg)
            except KeyError:
                pass

        if 'key' in msg:
            if msg['key'] not in self.who_wants:
                return
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
        try:
            yield self.handle_client(stream, client=client)
        finally:
            if not stream.closed():
                self.streams[client].send({'op': 'stream-closed'})
                yield self.streams[client].close(ignore_closed=True)
            del self.streams[client]
            logger.info("Close client connection: %s", client)

    def remove_client(self, client=None):
        """ Remove client from network """
        logger.info("Remove client %s", client)
        self.client_releases_keys(self.wants_what.get(client, ()), client)
        with ignoring(KeyError):
            del self.wants_what[client]

    @gen.coroutine
    def handle_client(self, stream, client=None):
        """
        Listen and respond to messages from clients

        This runs once per Client IOStream or Queue.

        See Also
        --------
        Scheduler.worker_stream: The equivalent function for workers
        """
        bstream = BatchedSend(interval=2, loop=self.loop)
        bstream.start(stream)
        self.streams[client] = bstream

        with log_errors(pdb=LOG_PDB):
            bstream.send({'op': 'stream-start'})

            breakout = False

            while True:
                try:
                    msgs = yield read(stream, deserialize=self.deserialize)
                except (StreamClosedError, AssertionError, GeneratorExit):
                    break
                except Exception as e:
                    logger.exception(e)
                    bstream.send(error_message(e, status='scheduler-error'))
                    continue

                if not isinstance(msgs, list):
                    msgs = [msgs]

                for msg in msgs:
                    # logger.debug("scheduler receives message %s", msg)
                    try:
                        op = msg.pop('op')
                    except Exception as e:
                        logger.exception(e)
                        bstream.end(error_message(e, status='scheduler-error'))

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
            logger.debug('Finished handle_client coroutine')

    def send_task_to_worker(self, worker, key):
        """ Send a single computational task to a worker """
        msg = {'op': 'compute-task',
               'key': key,
               'priority': self.priority[key],
               'duration': self.task_duration.get(key_split(key), 0.5)}
        if key not in self.loose_restrictions:
            if key in self.host_restrictions:
                msg['host_restrictions'] = list(self.host_restrictions[key])
            if key in self.worker_restrictions:
                msg['worker_restrictions'] = list(self.worker_restrictions[key])
        if key in self.resource_restrictions:
            msg['resource_restrictions'] = self.resource_restrictions[key]

        deps = self.dependencies[key]
        if deps:
            msg['who_has'] = {dep: tuple(self.who_has.get(dep, ()))
                              for dep in deps}
            msg['nbytes'] = {dep: self.nbytes.get(dep) for dep in deps}

        task = self.tasks[key]
        if type(task) is dict:
            msg.update(task)
        else:
            msg['task'] = task

        self.worker_streams[worker].send(msg)

    def handle_uncaught_error(self, **msg):
        logger.exception(clean_exception(**msg)[1])

    def handle_task_finished(self, key=None, **msg):
        validate_key(key)
        r = self.stimulus_task_finished(key=key, **msg)
        self.transitions(r)

    def handle_task_erred(self, key=None, **msg):
        r = self.stimulus_task_erred(key=key, **msg)
        self.transitions(r)

    def handle_missing_data(self, key=None, **msg):
        r = self.stimulus_missing_data(key=key, ensure=False, **msg)
        self.transitions(r)

    @gen.coroutine
    def handle_worker(self, worker):
        """
        Listen to responses from a single worker

        This is the main loop for scheduler-worker interaction

        See Also
        --------
        Scheduler.handle_client: Equivalent coroutine for clients
        """
        yield gen.sleep(0)
        ip, port = coerce_to_address(worker, out=tuple)
        try:
            stream = yield connect(ip, port)
        except Exception as e:
            logger.error("Failed to connect to worker '%s:%s': %s",
                         ip, port, e)
            return
        yield write(stream, {'op': 'compute-stream'})
        self.worker_streams[worker].start(stream)
        logger.info("Starting worker compute stream, %s", worker)

        try:
            while True:
                msgs = yield read(stream)
                start = time()

                if not isinstance(msgs, list):
                    msgs = [msgs]

                if worker in self.worker_info and not stream.closed():
                    self.counters['worker-message-length'].add(len(msgs))
                    recommendations = OrderedDict()
                    for msg in msgs:
                        if msg == 'OK':  # from close
                            break

                        if 'status' in msg and 'error' in msg['status']:
                            logger.exception(clean_exception(**msg)[1])

                        op = msg.pop('op')
                        if op:
                            self.correct_time_delay(worker, msg)
                            handler = self.worker_handlers[op]
                            handler(worker=worker, **msg)

                end = time()
                if self.digests is not None:
                    self.digests['handle-worker-duration'].add(end - start)

        except (StreamClosedError, IOError, OSError):
            logger.info("Worker failed from closed stream: %s", worker)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise
        finally:
            close(stream)
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
        while not self.workers:
            yield gen.sleep(0.2)
            if time() > start + timeout:
                raise gen.TimeoutError("No workers found")

        if workers is None:
            ncores = self.ncores
        else:
            workers = [self.coerce_address(w) for w in workers]
            ncores = {w: self.ncores[w] for w in workers}

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
            data = yield gather_from_workers(who_has, rpc=self.rpc, close=False)
            result = {'status': 'OK', 'data': data}
        except KeyError as e:
            logger.debug("Couldn't gather keys %s", e)
            result = {'status': 'error', 'keys': e.args}

        raise gen.Return(result)

    @gen.coroutine
    def restart(self, environment=None):
        """ Restart all workers.  Reset local state. """
        n = len(self.workers)
        with log_errors():
            logger.debug("Send shutdown signal to workers")

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
                resps = yield All([nanny.restart(environment=environment, close=True)
                           for nanny in nannies])
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
                workers = list(self.workers)
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
            workers = set(workers or self.workers)

            if not keys.issubset(self.who_has):
                raise Return({'status': 'missing-data',
                              'keys': list(keys - set(self.who_has))})

            workers_by_key = {k: self.who_has.get(k, set()) & workers for k in keys}
            keys_by_worker = {w: set() for w in workers}
            for k, v in workers_by_key.items():
                for vv in v:
                    keys_by_worker[vv].add(k)

            worker_bytes = {w: sum(self.nbytes.get(k, DEFAULT_DATA_SIZE)
                                   for k in v)
                            for w, v in keys_by_worker.items()}
            avg = sum(worker_bytes.values()) / len(worker_bytes)

            sorted_workers = list(map(first, sorted(worker_bytes.items(),
                                              key=second, reverse=True)))

            recipients = iter(reversed(sorted_workers))
            recipient = next(recipients)
            msgs = []  # (sender, recipient, key)
            for sender in sorted_workers[:len(workers) // 2]:
                sender_keys = {k: self.nbytes.get(k, DEFAULT_DATA_SIZE)
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
                self.worker_bytes[recipient] += self.nbytes.get(key,
                                                        DEFAULT_DATA_SIZE)

            result = yield {r: self.rpc(addr=r).delete_data(keys=v, report=False)
                            for r, v in to_senders.items()}

            for sender, recipient, key in msgs:
                self.who_has[key].remove(sender)
                self.has_what[sender].remove(key)
                self.worker_bytes[sender] -= self.nbytes.get(key,
                                                             DEFAULT_DATA_SIZE)

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
                    self.worker_bytes[worker] -= self.nbytes.get(key,
                                                            DEFAULT_DATA_SIZE)

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
                    self.add_keys(worker=w, keys=list(gathers[w]))

    def workers_to_close(self, memory_ratio=2):
        """
        Find workers that we can close with low cost

        This returns a list of workers that are good candidates to retire.
        These workers are idle (not running anything) and are storing
        relatively little data relative to their peers.  If all workers are
        idle then we still maintain enough workers to have enough RAM to store
        our data, with a comfortable buffer.

        This is for use with systems like ``distributed.deploy.adaptive``.

        Parameters
        ----------
        memory_factor: Number
            Amount of extra space we want to have for our stored data.
            Defaults two 2, or that we want to have twice as much memory as we
            currently have data.

        Returns
        -------
        to_close: list of workers that are OK to close
        """
        with log_errors():
            if all(self.processing.values()):
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
        with log_errors():
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

    def add_keys(self, stream=None, worker=None, keys=()):
        """
        Learn that a worker has certain keys

        This should not be used in practice and is mostly here for legacy
        reasons.
        """
        worker = coerce_to_address(worker)
        if worker not in self.worker_info:
            return 'not found'
        for key in keys:
            if key in self.who_has:
                if key not in self.has_what[worker]:
                    self.worker_bytes[worker] += self.nbytes.get(key,
                                                            DEFAULT_DATA_SIZE)
                self.has_what[worker].add(key)
                self.who_has[key].add(worker)
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
                if key not in self.who_has:
                    self.who_has[key] = set()
                for w in workers:
                    if key not in self.has_what[w]:
                        self.worker_bytes[w] += self.nbytes.get(key,
                                                            DEFAULT_DATA_SIZE)
                    self.has_what[w].add(key)
                    self.who_has[key].add(w)
                self.waiting_data[key] = set()
                self.report({'op': 'key-in-memory',
                             'key': key,
                             'workers': list(workers)})

    def report_on_key(self, key, client=None):
        if key not in self.task_state:
            self.report({'op': 'cancelled-key',
                         'key': key}, client=client)
        elif self.task_state[key] == 'memory':
            self.report({'op': 'key-in-memory',
                         'key': key}, client=client)
        elif self.task_state[key] == 'erred':
            failing_key = self.exceptions_blame[key]
            self.report({'op': 'task-erred',
                         'key': key,
                         'exception': self.exceptions[failing_key],
                         'traceback': self.tracebacks.get(failing_key, None)},
                         client=client)


    @gen.coroutine
    def feed(self, stream, function=None, setup=None, teardown=None, interval=1, **kwargs):
        """
        Provides a data stream to external requester

        Caution: this runs arbitrary Python code on the scheduler.  This should
        eventually be phased out.  It is mostly used by diagnostics.
        """
        import pickle
        with log_errors():
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

            self.waiting_data[key] = {dep for dep in self.dependents[key]
                                          if dep not in self.who_has
                                          and dep not in self.released
                                          and dep not in self.exceptions_blame}

            if not self.waiting[key]:
                if self.workers:
                    self.task_state[key] = 'waiting'
                    recommendations[key] = 'processing'
                else:
                    self.unrunnable.add(key)
                    del self.waiting[key]
                    self.task_state[key] = 'no-worker'
            else:
                self.task_state[key] = 'waiting'

            if self.validate:
                if self.task_state[key] == 'waiting':
                    assert key in self.waiting

            self.released.remove(key)

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_no_worker_waiting(self, key):
        try:
            if self.validate:
                assert key in self.unrunnable
                assert key not in self.waiting
                assert key not in self.who_has
                assert key not in self.rprocessing

            self.unrunnable.remove(key)

            if not all(dep in self.task_state for dep in
                    self.dependencies[key]):
                return {key: 'forgotten'}

            recommendations = OrderedDict()

            self.waiting[key] = set()

            for dep in self.dependencies[key]:
                if dep not in self.who_has:
                    self.waiting[key].add(dep)
                if dep in self.released:
                    recommendations[dep] = 'waiting'
                else:
                    self.waiting_data[dep].add(key)

            self.task_state[key] = 'waiting'

            if not self.waiting[key]:
                if self.workers:
                    recommendations[key] = 'processing'
                else:
                    self.task_state[key] = 'no-worker'

            return recommendations
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb; pdb.set_trace()
            raise

    def transition_waiting_processing(self, key):
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

            valid_workers = self.valid_workers(key)

            del self.waiting[key]

            if not valid_workers and key not in self.loose_restrictions and self.ncores:
                self.unrunnable.add(key)
                self.task_state[key] = 'no-worker'
                return {}

            if self.dependencies.get(key, None) or valid_workers is not True:
                worker = decide_worker(self.dependencies, self.occupancy,
                        self.who_has, valid_workers, self.loose_restrictions,
                        partial(self.worker_objective, key), key)
            elif self.idle:
                if len(self.idle) < 20:  # smart but linear in small case
                    worker = min(self.idle, key=self.occupancy.get)
                else:  # dumb but fast in large case
                    worker = self.idle[self.n_tasks % len(self.idle)]
            else:
                if len(self.workers) < 20:  # smart but linear in small case
                    worker = min(self.workers, key=self.occupancy.get)
                else:  # dumb but fast in large case
                    worker = self.workers[self.n_tasks % len(self.workers)]

            assert worker

            ks = key_split(key)

            duration = self.task_duration.get(ks)
            if duration is None:
                self.unknown_durations[ks].add(key)
                duration = 0.5

            comm = (sum(self.nbytes[dep]
                       for dep in self.dependencies[key] - self.has_what[worker])
                    / BANDWIDTH)

            self.processing[worker][key] = duration + comm
            self.rprocessing[key] = worker
            self.occupancy[worker] += duration + comm
            self.total_occupancy += duration + comm
            self.task_state[key] = 'processing'
            self.consume_resources(key, worker)
            self.check_idle_saturated(worker)
            self.n_tasks += 1

            # logger.debug("Send job to worker: %s, %s", worker, key)

            try:
                self.send_task_to_worker(worker, key)
            except StreamClosedError:
                self.remove_worker(worker)

            if self.validate:
                assert key not in self.waiting

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
                w = self.rprocessing[key]
                assert key in self.processing[w]
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

                if ks in self.unknown_durations:
                    for k in self.unknown_durations.pop(ks):
                        if k in self.rprocessing:
                            w = self.rprocessing[k]
                            old = self.processing[w][k]
                            comm = (sum(self.nbytes[d] for d in
                                         self.dependencies[k] - self.has_what[w])
                                    / BANDWIDTH)
                            self.processing[w][k] = avg_duration + comm
                            self.occupancy[w] += avg_duration + comm - old
                            self.total_occupancy += avg_duration + comm - old

                info['last-task'] = compute_stop

            ############################
            # Update State Information #
            ############################
            if nbytes is not None:
                self.nbytes[key] = nbytes

            self.who_has[key] = set()
            self.release_resources(key, worker)

            if worker:
                self.who_has[key].add(worker)
                self.has_what[worker].add(key)
                self.worker_bytes[worker] += self.nbytes.get(key,
                                                             DEFAULT_DATA_SIZE)

            w = self.rprocessing.pop(key)
            duration = self.processing[w].pop(key)
            if not self.processing[w]:
                self.total_occupancy -= self.occupancy[w]
                self.occupancy[w] = 0
            else:
                self.total_occupancy -= duration
                self.occupancy[w] -= duration
            self.check_idle_saturated(w)
            if w != worker:
                msg = {'op': 'release-task', 'key': key}
                # self.worker_streams[w].send(msg)

            recommendations = OrderedDict()

            deps = self.dependents.get(key, [])
            if len(deps) > 1:
               deps = sorted(deps, key=self.priority.get, reverse=True)

            for dep in deps:
                if dep in self.waiting:
                    s = self.waiting[dep]
                    s.remove(key)
                    if not s:  # new task ready to run
                        recommendations[dep] = 'processing'

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
                assert key not in self.waiting

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
                dep_state = self.task_state[dep]
                if dep_state in ('no-worker', 'processing'):
                    recommendations[dep] = 'waiting'
                if dep_state == 'waiting':
                    self.waiting[dep].add(key)

            workers = self.who_has.pop(key)
            for w in workers:
                if w in self.worker_info:  # in case worker has died
                    self.has_what[w].remove(key)
                    self.worker_bytes[w] -= self.nbytes.get(key,
                                                            DEFAULT_DATA_SIZE)
                    try:
                        self.worker_streams[w].send({'op': 'delete-data',
                                                     'keys': [key], 'report': False})
                    except EnvironmentError:
                        self.loop.add_callback(self.remove_worker, address=w)

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

            if self.validate:
                assert key not in self.waiting

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

            w = self.rprocessing.pop(key)
            if w in self.workers:
                duration = self.processing[w].pop(key)
                self.occupancy[w] -= duration
                self.total_occupancy -= duration
                self.check_idle_saturated(w)
                self.release_resources(key, w)

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

    def transition_processing_erred(self, key, cause=None, exception=None,
            traceback=None):
        try:
            if self.validate:
                assert cause or key in self.exceptions_blame
                assert key in self.rprocessing
                assert key not in self.who_has
                assert key not in self.waiting

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

            w = self.rprocessing.pop(key)
            if w in self.processing:
                duration = self.processing[w].pop(key)
                self.occupancy[w] -= duration
                self.total_occupancy -= duration
                self.check_idle_saturated(w)
                self.release_resources(key, w)

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
        if key in self.worker_restrictions:
            del self.worker_restrictions[key]
        if key in self.host_restrictions:
            del self.host_restrictions[key]
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
        if key in self.resource_restrictions:
            del self.resource_restrictions[key]

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
                    self.worker_bytes[w] -= self.nbytes.get(key,
                                                            DEFAULT_DATA_SIZE)
                    try:
                        self.worker_streams[w].send({'op': 'delete-data',
                                                     'keys': [key], 'report': False})
                    except EnvironmentError:
                        self.loop.add_callback(self.remove_worker, address=w)

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

    def transition_no_worker_released(self, key):
        try:
            if self.validate:
                assert self.task_state[key] == 'no-worker'
                assert key not in self.who_has

            self.unrunnable.remove(key)
            self.released.add(key)
            self.task_state[key] = 'released'

            for dep in self.dependencies[key]:
                try:
                    self.waiting_data[dep].remove(key)
                except KeyError:  # dep may also be released
                    pass

            del self.waiting_data[key]

            return {}
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
        {'x': 'processing'}

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
            self.transition_log.append((key, start, finish2, recommendations,
                                        self._transition_counter))
            self._transition_counter += 1
            if self.validate:
                logger.debug("Transition %s->%s: %s New: %s",
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
        original = recommendations
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

    def check_idle_saturated(self, worker):
        occ = self.occupancy[worker]
        nc = self.ncores[worker]
        p = len(self.processing[worker])

        avg = self.total_occupancy / self.total_ncores

        if p < nc or occ / nc < avg / 2:
            self.idle.add(worker)
            if worker in self.saturated:
                self.saturated.remove(worker)
        else:
            if worker in self.idle:
                self.idle.remove(worker)

            pending = occ * (p - nc) / p / nc
            if p > nc and pending > 0.4 and pending > 1.9 * avg:
                self.saturated.add(worker)
            elif worker in self.saturated:
                self.saturated.remove(worker)

    def valid_workers(self, key):
        """ Return set of currently valid worker addresses for key

        If all workers are valid then this returns ``True``.
        This checks tracks the following state:

        *  worker_restrictions
        *  host_restrictions
        *  resource_restrictions
        """
        s = True

        if key in self.worker_restrictions:
            s = {w for w in self.worker_restrictions[key] if w in
                    self.worker_info}

        if key in self.host_restrictions:
            hr = self.host_restrictions[key]
            ss = {w for w in self.worker_info if w.split(':')[0] in hr}
            if s is True:
                s = ss
            else:
                s |= ss

        if self.resource_restrictions.get(key):
            w = {resource: {w for w, supplied in self.resources[resource].items()
                              if supplied >= required}
                 for resource, required in self.resource_restrictions[key].items()}

            ww = set.intersection(*w.values())

            if s is True:
                s = ww
            else:
                s &= ww

        return s

    def consume_resources(self, key, worker):
        if key in self.resource_restrictions:
            for r, required in self.resource_restrictions[key].items():
                self.used_resources[worker][r] += required

    def release_resources(self, key, worker):
        if key in self.resource_restrictions:
            for r, required in self.resource_restrictions[key].items():
                self.used_resources[worker][r] -= required

    #####################
    # Utility functions #
    #####################

    def add_resources(self, worker, resources):
        if worker not in self.worker_resources:
            self.worker_resources[worker] = resources
            self.used_resources[worker] = resources
            for resource, quantity in resources.items():
                self.resources[resource][worker] = quantity

        # TODO: add host_resources

    def remove_resources(self, worker):
        if worker in self.worker_resources:
            del self.used_resources[worker]
            for resource, quantity in self.worker_resources.pop(worker).items():
                del self.resources[resource][worker]

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
            return list(self.workers)

        out = set()
        for w in workers:
            if ':' in w:
                out.add(w)
            else:
                out.update({ww for ww in self.workers if w in ww}) # TODO: quadratic
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

    def worker_objective(self, key, worker):
        """
        Objective function to determine which worker should get the key

        Minimize expected start time.  If a tie then break with data storate.
        """
        comm_bytes = sum([self.nbytes.get(k, DEFAULT_DATA_SIZE)
                          for k in self.dependencies[key]
                          if worker not in self.who_has[k]])
        stack_time = self.occupancy[worker] / self.ncores[worker]
        start_time = comm_bytes / BANDWIDTH + stack_time
        return (start_time, self.worker_bytes[worker])


def decide_worker(dependencies, occupancy, who_has, valid_workers,
                  loose_restrictions, objective, key):
    """
    Decide which worker should take task

    >>> dependencies = {'c': {'b'}, 'b': {'a'}}
    >>> occupancy = {'alice:8000': 0, 'bob:8000': 0}
    >>> who_has = {'a': {'alice:8000'}}
    >>> nbytes = {'a': 100}
    >>> ncores = {'alice:8000': 1, 'bob:8000': 1}
    >>> valid_workers = True
    >>> loose_restrictions = set()

    We choose the worker that has the data on which 'b' depends (alice has 'a')

    >>> decide_worker(dependencies, occupancy, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'alice:8000'

    If both Alice and Bob have dependencies then we choose the less-busy worker

    >>> who_has = {'a': {'alice:8000', 'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'a'}}
    >>> decide_worker(dependencies, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'bob:8000'

    Optionally provide valid workers of where jobs are allowed to occur

    >>> valid_workers = {'alice:8000', 'charlie:8000'}
    >>> decide_worker(dependencies, who_has, has_what,
    ...               valid_workers, loose_restrictions, nbytes, ncores, 'b')
    'alice:8000'

    If the task requires data communication, then we choose to minimize the
    number of bytes sent between workers. This takes precedence over worker
    occupancy.

    >>> dependencies = {'c': {'a', 'b'}}
    >>> who_has = {'a': {'alice:8000'}, 'b': {'bob:8000'}}
    >>> has_what = {'alice:8000': {'a'}, 'bob:8000': {'b'}}
    >>> nbytes = {'a': 1, 'b': 1000}

    >>> decide_worker(dependencies, who_has, has_what,
    ...               {}, set(), nbytes, ncores, 'c')
    'bob:8000'
    """
    deps = dependencies[key]
    assert all(d in who_has for d in deps)
    workers = frequencies([w for dep in deps
                             for w in who_has[dep]])
    if not workers:
        workers = occupancy
    if valid_workers is not True:
        workers = valid_workers & set(workers)
        if not workers:
            workers = valid_workers
            if not workers:
                if key in loose_restrictions:
                    return decide_worker(dependencies, occupancy, who_has,
                            True, set(), objective, key)

                else:
                    return None
    if not workers or not occupancy:
        return None

    if len(workers) == 1:
        return first(workers)

    return min(workers, key=objective)


def validate_state(dependencies, dependents, waiting, waiting_data, ready,
        who_has, processing, finished_results, released,
        who_wants, wants_what, tasks=None, allow_overlap=False,
        erred=None, **kwargs):
    """
    Validate a current runtime state

    This performs a sequence of checks on the entire graph, running in about
    linear time.  This raises assert errors if anything doesn't check out.
    """
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
            if not (vv in waiting or
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
                 key in in_processing,
                 not not who_has.get(key),
                 key in released,
                 key in erred])
        if ((allow_overlap and sum(vals) < 1) or
            (not allow_overlap and sum(vals) != 1)):
            raise ValueError("Key exists in wrong number of places", key, vals)

        for dep in dependencies[key]:
            if dep in dependents:
                check_key(dep)  # Recursive case

        if who_has.get(key):
            assert not any(key in waiting.get(dep, ())
                           for dep in dependents.get(key, ()))
            assert not waiting.get(key)

        if key in in_processing:
            if not all(who_has.get(dep) for dep in dependencies[key]):
                raise ValueError("Key in processing without all deps",
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
