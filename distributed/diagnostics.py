import logging
import sys
import threading
import time
from timeit import default_timer

from .utils import ignoring
from .executor import default_executor


logger = logging.getLogger(__name__)


class Diagnostic(object):
    def start(self, scheduler):
        pass

    def task_finished(self, scheduler, key, worker, nbytes):
        pass

    def update_graph(self, scheduler, dsk, keys, restrictions):
        pass


class ProgressBar(Diagnostic):
    def __init__(self, scheduler=None, keys=None, minimum=0, dt=0.1):
        self._minimum = minimum
        self._dt = dt
        self.last_duration = 0
        self.all_keys = set()
        self.keys = set()

        self._running = False

        if scheduler is None and default_executor():
            scheduler = default_executor().scheduler

        if scheduler:
            self.scheduler = scheduler
            scheduler.add_diagnostic(self)
        else:
            raise ValueError("Can not find scheduler.\n"
                 "Either create an executor or supply scheduler as an argument")

        if keys:
            self.add_keys(keys)

    def add_keys(self, keys, scheduler=None):
        keys = {k.key if hasattr(k, 'key') else k for k in keys}
        stack = list(keys)
        while stack:
            key = stack.pop()
            if (key in self.keys or
                self.scheduler.who_has.get(key) or
                key in self.scheduler.processing or key in self.scheduler.stacks):
                continue

            self.keys.add(key)
            self.all_keys.add(key)
            stack.extend(self.scheduler.waiting.get(key, []))

        if not self._running:
            self._start_time = default_timer()
            # Start background thread
            self._running = True
            self._timer = threading.Thread(target=self._timer_func)
            self._timer.daemon = True
            self._timer.start()

    def update_graph(self, scheduler, dsk, keys, restrictions):
        self.add_keys(keys)

    def task_finished(self, scheduler, key, worker, nbytes):
        if key in self.keys:
            self.keys.remove(key)

        if not self.keys:
            self.finish()

    def finish(self):
        if self._running:
            self._running = False
            self._timer.join()
            elapsed = default_timer() - self._start_time
            self.last_duration = elapsed
            if elapsed < self._minimum:
                return
            else:
                self._update_bar(elapsed)
            self._finish_bar()

    def _finish_bar(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.finish()
        if self in self.scheduler.diagnostics:
            self.scheduler.diagnostics.remove(self)

    def _timer_func(self):
        """Background thread for updating the progress bar"""
        while self._running:
            elapsed = default_timer() - self._start_time
            if elapsed > self._minimum:
                self._update_bar(elapsed)
            time.sleep(self._dt)

    def _update_bar(self, elapsed):
        pass


class TextProgressBar(ProgressBar):
    def __init__(self, scheduler=None, keys=None, minimum=0, dt=0.1, width=40):
        ProgressBar.__init__(self, scheduler, keys, minimum, dt)
        self._width = width

    def _finish_bar(self):
        sys.stdout.write('\n')
        sys.stdout.flush()

    def _update_bar(self, elapsed):
        ntasks = len(self.all_keys)
        ndone = ntasks - len(self.keys)
        self._draw_bar(ndone / ntasks if ntasks else 1.0, elapsed)

    def _draw_bar(self, frac, elapsed):
        bar = '#' * int(self._width * frac)
        percent = int(100 * frac)
        elapsed = format_time(elapsed)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self._width,
                                                           percent, elapsed)
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()


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
