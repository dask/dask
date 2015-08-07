from __future__ import division
import sys
import threading
import time
from timeit import default_timer

from ..core import istask
from ..callbacks import Callback


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

    def __init__(self, width=40, dt=0.1):
        self._width = width
        self._dt = dt

    def _start(self, dsk, state):
        self._ntasks = len([k for (k, v) in dsk.items() if istask(v)])
        self._ndone = 0
        self._update_rate = max(1, self._ntasks // self._width)
        self._start_time = default_timer()
        # Start background thread
        self._running = True
        self._timer = threading.Thread(target=self._timer_func)
        self._timer.start()

    def _posttask(self, key, value, dsk, state, id):
        self._ndone += 1
        sys.stdout.flush()

    def _finish(self, dsk, state, errored):
        self._running = False
        self._timer.join()
        self._finalize_bar()

    def _timer_func(self):
        """Background thread for updating the progress bar"""
        while self._running:
            self._update_bar()
            time.sleep(self._dt)

    def _update_bar(self):
        if self._ntasks:
            tics = int(self._ndone * self._width / self._ntasks)
            percent = (100 * self._ndone) // self._ntasks
        else:
            tics = self._width
            percent = 100
        bar = '#' * tics
        elapsed = format_time(default_timer() - self._start_time)
        msg = '\r[{0:<{1}}] | {2}% Completed | {3}'.format(bar, self._width,
                                                           percent, elapsed)
        sys.stdout.write(msg)
        sys.stdout.flush()

    def _finalize_bar(self):
        self._update_bar()
        sys.stdout.write('\n')
        sys.stdout.flush()
