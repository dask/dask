from __future__ import division
import sys
import threading
import time
from timeit import default_timer

from ..callbacks import Callback
from ..utils import ignoring


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


class ProgressBar(Callback):
    """A progress bar for dask.

    Can be used as a context manager around dask computations.

    Examples
    --------
    >>> with ProgressBar():    # doctest: +SKIP
    ...     out = res.compute()
    [########################################] | 100% Completed | 10.4 s
    """

    def __init__(self, minimum=0, width=40, dt=0.1):
        self._minimum = minimum
        self._width = width
        self._dt = dt

    def _start(self, dsk):
        self._state = None
        self._start_time = default_timer()
        # Start background thread
        self._running = True
        self._timer = threading.Thread(target=self._timer_func)
        self._timer.start()

    def _pretask(self, key, dsk, state):
        self._state = state
        sys.stdout.flush()

    def _finish(self, dsk, state, errored):
        self._running = False
        self._timer.join()
        elapsed = default_timer() - self._start_time
        if elapsed < self._minimum:
            return
        if not errored:
            self._draw_bar(1, elapsed)
        else:
            self._update_bar()
        sys.stdout.write('\n')
        sys.stdout.flush()

    def _timer_func(self):
        """Background thread for updating the progress bar"""
        while self._running:
            elapsed = default_timer() - self._start_time
            if elapsed > self._minimum:
                self._update_bar(elapsed)
            time.sleep(self._dt)

    def _update_bar(self, elapsed):
        s = self._state
        if not s:
            self._draw_bar(0, elapsed)
            return
        ndone = len(s['finished'])
        ntasks = sum(len(s[k]) for k in ['ready', 'waiting', 'running']) + ndone
        self._draw_bar(ndone / ntasks if ntasks else 0, elapsed)

    def _draw_bar(self, frac, elapsed):
        bar = '#' * int(self._width * frac)
        percent = int(100 * frac)
        elapsed = format_time(elapsed)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self._width,
                                                           percent, elapsed)
        with ignoring(ValueError):
            sys.stdout.write(msg)
            sys.stdout.flush()
