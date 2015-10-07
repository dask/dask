from __future__ import print_function, division, absolute_import

from collections import namedtuple
import random
import uuid
from itertools import cycle, count

from toolz import merge, partial, pipe, concat, frequencies, concat, groupby
from toolz.curried import map, filter

from tornado import gen
from tornado.gen import Return
from tornado.concurrent import Future
from tornado.ioloop import IOLoop
from tornado.iostream import StreamClosedError

from .core import read, write, connect, rpc, coerce_to_rpc
from .client import (RemoteData, _scatter, _gather, unpack_remotedata)


log = print


class Pool(object):
    """ Remote computation pool

    This connects to a metadata ``Center`` and from there learns to where it can
    dispatch jobs, typically through an ``apply_async`` call.

    Examples
    --------
    >>> pool = Pool('192.168.0.100:8787')  # doctest: +SKIP
    >>> rd = pool.apply(func, args, kwargs)  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10

    >>> results = pool.map(func, sequence)  # doctest: +SKIP
    >>> pool.gather(results)  # doctest: +SKIP
    """
    def __init__(self, center):
        self.center = coerce_to_rpc(center)
        self._open_streams = set()

    @gen.coroutine
    def _sync_center(self):
        self.who_has = yield self.center.who_has()
        self.has_what = yield self.center.has_what()
        self.ncores = yield self.center.ncores()
        self.available_cores = self.ncores

    @gen.coroutine
    def _map(self, func, seq, **kwargs):
        yield self._sync_center()
        tasks = []
        for i, item in enumerate(seq):
            [args2, kwargs2], needed = unpack_remotedata([(item,), kwargs])
            tasks.append(dict(key=str(uuid.uuid1()),
                              function=func, args=args2,
                              kwargs=kwargs2,
                              needed=needed, index=i))

        output = [None for i in seq]
        running, finished, erred = set(), set(), set()
        needed = {i: task['needed'] for i, task in enumerate(tasks)}
        remaining = set(needed)

        shares, extra = divide_tasks(self.who_has, needed)

        computation_done = Future()

        for worker, count in self.available_cores.items():
            for i in range(count):
                IOLoop.current().spawn_callback(handle_worker,
                    self.who_has, self.has_what, tasks, shares, extra, remaining,
                    running, finished, erred,
                    output, worker, computation_done,
                    self.center.ip, self.center.port)

        yield computation_done                         # wait until done
        assert all(isinstance(o, RemoteData) for o in output)

        """
        for cor in coroutines:                             # Cancel lingering
            if cor.cancel():                               # workers
                log('Cancel running worker')
        """

        raise Return(output)

    def map(self, func, seq, **kwargs):
        return IOLoop.current().run_sync(
                lambda: self._map(func, seq, **kwargs))

    def sync_center(self):
        """ Get who_has and has_what dictionaries from a center

        In particular this tells us what workers we have at our disposal
        """
        return IOLoop.current().run_sync(self._sync_center)

    @gen.coroutine
    def _close_connections(self):
        """ Close active connections """
        for stream in self._open_streams:
            if not stream._closed:
                r = rpc(stream)
                result = yield r.close(close=True)
        yield self.center.close(close=True)
        self.center.close_streams()

    def close_connections(self):
        return IOLoop.current().run_sync(self._close_connections)

    def close(self):
        """ Close the thread that manages our event loop """
        self.close_connections()

    @gen.coroutine
    def _apply_async(self, func, args=(), kwargs={}, key=None):
        if not isinstance(args, (tuple, list)):
            raise TypeError('args must be a tuple as in:\n'
                    '  pool.apply_async(func, args=(x,))')
        [args2, kwargs2], needed = unpack_remotedata([args, kwargs])

        ip, port = choose_worker(needed, self.who_has, self.has_what,
                                 self.available_cores)

        if key is None:
            key = str(uuid.uuid1())

        pc = PendingComputation(key, func, args2, kwargs2, needed,
                                self.center.ip, self.center.port)
        yield pc._start(ip, port, self.who_has, self.has_what,
                        self.available_cores)
        raise Return(pc)

    def apply_async(self, func, args=(), kwargs={}, key=None):
        """ Execute a function in a remote worker

        If an arg or a kwarg is a ``RemoteData`` object then that data will be
        communicated as necessary among the ``Worker`` peers.
        """
        return IOLoop.current().run_sync(
                lambda: self._apply_async(func, args, kwargs, key))

    def apply(self, func, args=(), kwargs={}, key=None):
        return self.apply_async(func, args, kwargs, key).get()

    @gen.coroutine
    def _scatter(self, data, key=None):
        yield self._sync_center()
        result = yield _scatter(self.center, data, key=key)
        raise Return(result)

    def scatter(self, data, key=None):
        return IOLoop.current().run_sync(lambda: self._scatter(data, key))

    @gen.coroutine
    def _gather(self, data):
        result = yield _gather(self.center, data)
        raise Return(result)

    def gather(self, data):
        return IOLoop.current().run_sync(lambda: self._gather(data))


class PendingComputation(object):
    """ A future for a computation that done in a remote worker

    This is generally created by ``Pool.apply_async``.  It can be converted
    into a ``RemoteData`` object by calling the ``.get()`` method.

    Example
    -------

    >>> pc = pool.apply_async(func, args, kwargs)  # doctest: +SKIP
    >>> rd = pc.get()  # doctest: +SKIP
    >>> rd.get()  # doctest: +SKIP
    10
    """
    def __init__(self, key, function, args, kwargs, needed, center_ip,
            center_port):
        self.key = key
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.needed = needed
        self.status = None
        self.center_ip = center_ip
        self.center_port = center_port

    @gen.coroutine
    def _start(self, ip, port, who_has=None, has_what=None,
               available_cores=None):
        msg = dict(op='compute', key=self.key, function=self.function,
                   args=self.args, kwargs=self.kwargs, needed=self.needed,
                   reply=True, close=True)
        self.ip = ip
        self.port = port
        self._has_what = has_what
        self._who_has = who_has
        self._available_cores = available_cores
        self.stream  = yield connect(ip, port)
        self._available_cores[(ip, port)] -= 1
        self.status = b'running'
        yield write(self.stream, msg)

    @gen.coroutine
    def _get(self):
        result = yield read(self.stream)
        self.status = result
        self._who_has[self.key].add((self.ip, self.port))
        self._has_what[(self.ip, self.port)].add(self.key)
        self._available_cores[(self.ip, self.port)] += 1
        self._result = RemoteData(self.key, self.center_ip, self.center_port,
                                  status=self.status)
        raise Return(self._result)

    def get(self):
        return IOLoop.current().run_sync(self._get)


def choose_worker(needed, who_has, has_what, available_cores):
    """ Select worker to run computation

    Currently selects the worker with the most data local
    """
    workers = {w for w, c in available_cores.items() if c}
    counts = pipe(needed, map(who_has.__getitem__), concat,
            filter(workers.__contains__), frequencies)
    if not counts:
        return random.choice(list(workers))
    else:
        biggest = max(counts.values())
        best = {k: v for k, v in counts.items() if v == biggest}
        return random.choice(list(best))


def divide_tasks(who_has, needed):
    """ Divvy up work between workers

    Given the following dictionaries:

    who_has: {data: [workers who have data]}
    needed: {task: [data required by task]}

    Produce a dictionary of tasks for each worker to do in sorted order of
    priority.  These lists of tasks may overlap.

    Example
    -------

    >>> who_has = {'x': {'Alice'},
    ...            'y': {'Alice', 'Bob'},
    ...            'z': {'Bob'}}
    >>> needed = {1: {'x'},       # doable by Alice
    ...           2: {'y'},       # doable by Alice and Bob
    ...           3: {'z'},       # doable by Bob
    ...           4: {'x', 'z'},  # doable by neither
    ...           5: set()}       # doable by all
    >>> shares, extra = divide_tasks(who_has, needed)
    >>> shares  # doctest: +SKIP
    {'Alice': [2, 1],
       'Bob': [2, 3]}
    >>> extra
    {4, 5}

    Ordering
    --------

    The tasks are ordered by the number of workers able to perform them.  In
    this way we prioritize those tasks that few others can perform.
    """
    n = sum(map(len, who_has.values()))
    scores = {k: len(v) / n for k, v in who_has.items()}

    task_workers = {task: set.intersection(*[who_has[d] for d in data])
                          if data else set()
                    for task, data in needed.items()}
    extra = {task for task in needed if not task_workers[task]}

    worker_tasks = reverse_dict(task_workers)
    worker_tasks = {k: sorted(v, key=lambda task: len(task_workers[task]),
                                 reverse=True)
                    for k, v in worker_tasks.items()}

    return worker_tasks, extra


def reverse_dict(d):
    """

    >>> a, b, c = 'abc'
    >>> d = {'a': [1, 2], 'b': [2], 'c': []}
    >>> reverse_dict(d)  # doctest: +SKIP
    {1: {'a'}, 2: {'a', 'b'}}
    """
    result = dict((v, set()) for v in concat(d.values()))
    for k, vals in d.items():
        for val in vals:
            result[val].add(k)
    return result


@gen.coroutine
def handle_task(task, output, stream, center_ip, center_port):
    task = task.copy()
    index = task.pop('index')

    response = yield rpc(stream).compute(**task)

    output[index] = RemoteData(task['key'], center_ip, center_port,
                               status=response)


@gen.coroutine
def handle_worker(who_has, has_what, tasks, shares, extra,
                  remaining, running, finished, erred,
                  output, ident, computation_done,
                  center_ip, center_port):
    """ Handle one core on one remote worker

    Parameters
    ----------

    who_has: dict
        {data-key: [worker]}
    has_what: dict
        {worker: [data-key]}
    tasks: list
        List of tasks to send to worker
    shares: dict
        Tasks that each worker can do :: {worker: [task-key]}
    extra: set
        Tasks that no worker can do alone :: {task-key}
    remaining: set
        Tasks that no one has yet started
    running: set
        Tasks that are in process
    finished: set
        Tasks that have completed
    output: list
        Remote data results to send back to user :: [RemoteData]
    ident: (ip, port)
        Identity of the worker that we're managing
    computation_done: Future
        A flag to set once we've completed all work
    """
    stream = yield connect(*ident)

    passed = set()

    def mark_done(i):
        if i in remaining:
            remaining.remove(i)
        if i in running:
            running.remove(i)
        finished.add(i)

    @gen.coroutine
    def _handle_task(task):
        result = yield handle_task(task, output, stream, center_ip, center_port)
        raise Return(result)

    while ident in shares and shares[ident]:    # Process our own tasks
        i = shares[ident].pop()
        if i in finished:
            continue
        if i in running:
            passed.add(i)
            log("%s: Pass on %s" % (str(ident), str(i)))
            continue

        running.add(i)
        try:
            yield _handle_task(tasks[i])
        except StreamClosedError:
            erred.add(i)
            raise Return(None)
        mark_done(i)

        who_has[tasks[i]['key']].add(ident)
        has_what[ident].add(tasks[i]['key'])

    if ident in shares:
        del shares[ident]

    while extra:                                # Process shared tasks
        i = extra.pop()
        running.add(i)
        try:
            yield _handle_task(tasks[i])
        except StreamClosedError:
            erred.add(i)
            raise Return(None)
        mark_done(i)

        who_has[tasks[i]['key']].add(ident)
        has_what[ident].add(tasks[i]['key'])

    passed -= finished

    while passed:                               # Do our own passed work
        i = passed.pop()                        # Sometimes others are slow
        if i in finished:                       # We're redundant here
            continue

        log("%s: Redundantly compute %s" % (str(ident), str(i)))
        try:
            yield _handle_task(tasks[i])
        except StreamClosedError:
            erred.add(i)
            raise Return(None)
        mark_done(i)
        who_has[tasks[i]['key']].add(ident)
        has_what[ident].add(tasks[i]['key'])

    while shares:                               # Steal work from others
        yield []
        worker = random.choice(list(shares))
        jobs = shares[worker]
        if not jobs:
            del shares[worker]
            continue

        # Walk down the list of jobs and find one that isn't finished/running
        j = 0
        while j < len(jobs) and (jobs[-j] in finished or jobs[-j] in running):
            j += 1

        # If we found one then run it
        if j < len(jobs):
            i = jobs[-j]
            log("%s <- %s: Steal %s" % (str(ident), str(worker), str(i)))
        else:
            break

        running.add(i)
        try:
            yield _handle_task(tasks[i])
        except StreamClosedError:
            erred.add(i)
            raise Return(None)
        mark_done(i)

        who_has[tasks[i]['key']].add(ident)
        has_what[ident].add(tasks[i]['key'])

    while erred:                                # Process errored tasks
        i = erred.pop()
        running.add(i)
        try:
            yield _handle_task(tasks[i])
        except StreamClosedError:
            erred.add(i)
            raise Return(None)
        mark_done(i)

        who_has[tasks[i]['key']].add(ident)
        has_what[ident].add(tasks[i]['key'])

    yield write(stream, {'op': 'close', 'close': True})

    if not remaining:
        computation_done.set_result(True)
