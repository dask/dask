from __future__ import print_function, division, absolute_import

from collections import defaultdict
import logging
import sys
import threading
import time
from timeit import default_timer

import dask
from toolz import valmap, groupby, concat
from tornado.ioloop import PeriodicCallback, IOLoop
from tornado import gen

from .plugin import SchedulerPlugin
from ..utils import sync, key_split, tokey
from ..executor import default_executor


logger = logging.getLogger(__name__)


def dependent_keys(keys, who_has, processing, stacks, dependencies, exceptions,
                   complete=False):
    """ All keys that need to compute for these keys to finish """
    out = set()
    errors = set()
    stack = list(keys)
    while stack:
        key = stack.pop()
        if key in out:
            continue
        if not complete and (who_has.get(key) or
                             key in processing or
                             key in stacks):
            continue
        if key in exceptions:
            errors.add(key)
            if not complete:
                continue

        out.add(key)
        stack.extend(dependencies.get(key, []))
    return out, errors


class Progress(SchedulerPlugin):
    """ Tracks progress of a set of keys or futures

    On creation we provide a set of keys or futures that interest us as well as
    a scheduler.  We traverse through the scheduler's dependencies to find all
    relevant keys on which our keys depend.  We then plug into the scheduler to
    learn when our keys become available in memory at which point we record
    their completion.

    State
    -----
    keys: set
        Set of keys that are not yet computed
    all_keys: set
        Set of all keys that we track

    This class performs no visualization.  However it is used by other classes,
    notably TextProgressBar and ProgressWidget, which do perform visualization.
    """
    def __init__(self, keys, scheduler, minimum=0, dt=0.1, complete=False):
        self.keys = {k.key if hasattr(k, 'key') else k for k in keys}
        self.keys = {tokey(k) for k in self.keys}
        self.scheduler = scheduler
        self.complete = complete
        self._minimum = minimum
        self._dt = dt
        self.last_duration = 0
        self._start_time = default_timer()
        self._running = False
        self.status = None

    @gen.coroutine
    def setup(self):
        keys = self.keys

        while not keys.issubset(self.scheduler.tasks):
            yield gen.sleep(0.05)

        self.keys = None

        self.scheduler.add_plugin(self)  # subtle race condition here
        self.all_keys, errors = dependent_keys(keys, self.scheduler.who_has,
                self.scheduler.processing, self.scheduler.stacks,
                self.scheduler.dependencies, self.scheduler.exceptions,
                complete=self.complete)
        if not self.complete:
            self.keys = self.all_keys.copy()
        else:
            self.keys, _ = dependent_keys(keys, self.scheduler.who_has,
                    self.scheduler.processing, self.scheduler.stacks,
                    self.scheduler.dependencies, self.scheduler.exceptions,
                    complete=False)
        self.all_keys.update(keys)
        self.keys |= errors & self.all_keys

        if not self.keys:
            self.stop(exception=None, key=None)

        logger.debug("Set up Progress keys")

        for k in errors:
            self.task_erred(None, k, None, True)

    def start(self):
        self.status = 'running'
        logger.debug("Start Progress Plugin")
        self._start()
        if any(k in self.scheduler.exceptions_blame for k in self.all_keys):
            self.stop(True)
        elif not self.keys:
            self.stop()

    def _start(self):
        pass

    def task_finished(self, scheduler, key, worker, nbytes, **kwargs):
        logger.debug("Progress sees key %s", key)
        if key in self.keys:
            self.keys.remove(key)

        if not self.keys:
            self.stop()

    def task_erred(self, scheduler, key, worker, exception, **kwargs):
        logger.debug("Progress sees task erred")
        if key in self.all_keys:
            self.stop(exception=exception, key=key)

    def restart(self, scheduler):
        self.stop()

    def stop(self, exception=None, key=None):
        if self in self.scheduler.plugins:
            self.scheduler.plugins.remove(self)
        if exception:
            self.status = 'error'
        else:
            self.status = 'finished'
        logger.debug("Remove Progress plugin")

    @property
    def elapsed(self):
        return default_timer() - self._start_time


class MultiProgress(Progress):
    """ Progress variant that keeps track of different groups of keys

    See Progress for most details.  This only adds a function ``func=``
    that splits keys.  This defaults to ``key_split`` which aligns with naming
    conventions chosen in the dask project (tuples, hyphens, etc..)

    State
    -----
    keys: dict
        Maps group name to set of not-yet-complete keys for that group
    all_keys: dict
        Maps group name to set of all keys for that group

    Examples
    --------
    >>> split = lambda s: s.split('-')[0]
    >>> p = MultiProgress(['y-2'], func=split)  # doctest: +SKIP
    >>> p.keys   # doctest: +SKIP
    {'x': {'x-1', 'x-2', 'x-3'},
     'y': {'y-1', 'y-2'}}
    """
    def __init__(self, keys, scheduler=None, func=key_split, minimum=0, dt=0.1,
                 complete=False):
        self.func = func
        Progress.__init__(self, keys, scheduler, minimum=minimum, dt=dt,
                            complete=complete)

    @gen.coroutine
    def setup(self):
        keys = self.keys

        while not keys.issubset(self.scheduler.tasks):
            yield gen.sleep(0.05)

        self.keys = None

        self.scheduler.add_plugin(self)  # subtle race condition here
        self.all_keys, errors = dependent_keys(keys, self.scheduler.who_has,
                self.scheduler.processing, self.scheduler.stacks,
                self.scheduler.dependencies, self.scheduler.exceptions,
                complete=self.complete)
        if not self.complete:
            self.keys = self.all_keys.copy()
        else:
            self.keys, _ = dependent_keys(keys, self.scheduler.who_has,
                    self.scheduler.processing, self.scheduler.stacks,
                    self.scheduler.dependencies, self.scheduler.exceptions,
                    complete=False)
        self.all_keys.update(keys)
        self.keys |= errors & self.all_keys

        if not self.keys:
            self.stop(exception=None, key=None)

        # Group keys by func name
        self.keys = valmap(set, groupby(self.func, self.keys))
        self.all_keys = valmap(set, groupby(self.func, self.all_keys))
        for k in self.all_keys:
            if k not in self.keys:
                self.keys[k] = set()

        for k in errors:
            self.task_erred(None, k, None, True)
        logger.debug("Set up Progress keys")

    def task_finished(self, scheduler, key, worker, nbytes, **kwargs):
        s = self.keys.get(self.func(key), None)
        if s and key in s:
            s.remove(key)

        if not self.keys or not any(self.keys.values()):
            self.stop()

    def task_erred(self, scheduler, key, worker, exception, **kwargs):
        logger.debug("Progress sees task erred")

        if (self.func(key) in self.all_keys and
            key in self.all_keys[self.func(key)]):
            self.stop(exception=exception, key=key)

    def start(self):
        self.status = 'running'
        logger.debug("Start Progress Plugin")
        self._start()
        if not self.keys or not any(v for v in self.keys.values()):
            self.stop()
        elif all(k in self.scheduler.exceptions_blame for k in
                concat(self.keys.values())):
            key = next(k for k in concat(self.keys.values()) if k in
                    self.scheduler.exceptions_blame)
            self.stop(exception=True, key=key)


def format_time(t):
    """Format seconds into a human readable form.

    >>> format_time(10.4)
    '10.4s'
    >>> format_time(1000.4)
    '16min 40.4s'
    """
    m, s = divmod(t, 60)
    h, m = divmod(m, 60)
    if h:
        return '{0:2.0f}hr {1:2.0f}min {2:4.1f}s'.format(h, m, s)
    elif m:
        return '{0:2.0f}min {1:4.1f}s'.format(m, s)
    else:
        return '{0:4.1f}s'.format(s)


class AllProgress(SchedulerPlugin):
    """ Keep track of all keys, grouped by key_split """
    def __init__(self, scheduler):
        self.all = defaultdict(set)
        self.in_memory = defaultdict(set)
        self.erred = defaultdict(set)
        self.released = defaultdict(set)

        for key in scheduler.tasks:
            k = key_split(key)
            self.all[k].add(key)

        for key in scheduler.who_has:
            k = key_split(key)
            self.in_memory[k].add(key)

        for key in scheduler.exceptions_blame:
            k = key_split(key)
            self.erred[k].add(key)

        for key in scheduler.released:
            k = key_split(key)
            self.released[k].add(key)

        scheduler.add_plugin(self)

    def update_graph(self, scheduler, tasks=None, **kwargs):
        for key in tasks:
            k = key_split(key)
            self.all[k].add(key)

    def mark_key_in_memory(self, scheduler, key, **kwargs):
        if key in scheduler.tasks:
            k = key_split(key)
            self.in_memory[k].add(key)

    def task_erred(self, scheduler, key=None, **kwargs):
        k = key_split(key)
        self.erred[k].add(key)

    def delete(self, scheduler, key):
        k = key_split(key)
        try:
            self.in_memory[k].remove(key)
        except KeyError:
            pass
        self.released[k].add(key)

    def forget(self, scheduler, key):
        k = key_split(key)
        for d in [self.all, self.in_memory, self.released, self.erred]:
            if k in d:
                if key in d[k]:
                    d[k].remove(key)
                if not d[k]:
                    del d[k]

    def lost_data(self, scheduler, key=None):
        k = key_split(key)
        if k in self.in_memory:
            try:
                self.in_memory[k].remove(key)
            except KeyError:
                pass

    def restart(self, scheduler):
        self.all.clear()
        self.in_memory.clear()
        self.erred.clear()
        self.released.clear()

    def validate(self):
        for c in [self.in_memory, self.erred, self.released]:
            assert set(self.all).issuperset(set(c))
            for k in c:
                assert c[k] <= self.all[k]
