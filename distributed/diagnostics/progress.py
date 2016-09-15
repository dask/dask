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

        while not keys.issubset(self.scheduler.task_state):
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
            self.transition(k, None, 'erred', exception=True)

    def transition(self, key, start, finish, *args, **kwargs):
        if key in self.keys and start == 'processing' and finish == 'memory':
            logger.debug("Progress sees key %s", key)
            self.keys.remove(key)

            if not self.keys:
                self.stop()

        if key in self.all_keys and finish == 'erred':
            logger.debug("Progress sees task erred")
            self.stop(exception=kwargs['exception'], key=key)

        if key in self.keys and finish == 'forgotten':
            logger.debug("A task was cancelled (%s), stopping progress", key)
            self.stop(exception=True)

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
            self.transition(k, None, 'erred', exception=True)
        logger.debug("Set up Progress keys")

    def transition(self, key, start, finish, *args, **kwargs):
        if start == 'processing' and finish == 'memory':
            s = self.keys.get(self.func(key), None)
            if s and key in s:
                s.remove(key)

            if not self.keys or not any(self.keys.values()):
                self.stop()

        if finish == 'erred':
            logger.debug("Progress sees task erred")
            k = self.func(key)
            if (k in self.all_keys and key in self.all_keys[k]):
                self.stop(exception=kwargs.get('exception'), key=key)

        if finish == 'forgotten':
            k = self.func(key)
            if k in self.all_keys and key in self.all_keys[k]:
                logger.debug("A task was cancelled (%s), stopping progress", key)
                self.stop(exception=True)


def format_time(t):
    """Format seconds into a human readable form.

    >>> format_time(10.4)
    '10.4s'
    >>> format_time(1000.4)
    '16min 40.4s'
    >>> format_time(100000.4)
    '27hr 46min 40.4s'
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
        self.nbytes = defaultdict(lambda: 0)
        self.state = defaultdict(lambda: defaultdict(set))
        self.scheduler = scheduler

        for key, state in self.scheduler.task_state.items():
            k = key_split(key)
            self.all[k].add(key)
            self.state[state][k].add(key)
            if key in self.scheduler.nbytes:
                self.nbytes[k] += self.scheduler.nbytes[key]

        scheduler.add_plugin(self)

    def transition(self, key, start, finish, *args, **kwargs):
        k = key_split(key)
        self.all[k].add(key)
        try:
            self.state[start][k].remove(key)
        except KeyError: # TODO: remove me once we have a new or clean state
            pass
        if finish != 'forgotten':
            self.state[finish][k].add(key)
        else:
            self.all[k].remove(key)
            if not self.all[k]:
                del self.all[k]
                try:
                    del self.nbytes[k]
                except KeyError:
                    pass
                for v in self.state.values():
                    try:
                        del v[k]
                    except KeyError:
                        pass

        if start == 'memory':
            self.nbytes[k] -= self.scheduler.nbytes[key]
        if finish == 'memory':
            self.nbytes[k] += self.scheduler.nbytes[key]

    def restart(self, scheduler):
        self.all.clear()
        self.state.clear()
