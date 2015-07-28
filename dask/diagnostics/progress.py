from __future__ import division
import sys

from ..core import istask
from .core import Callback


class ProgressBar(Callback):

    def __init__(self, nbins=50):
        self._nbins = nbins

    def _start(self, dsk, state):
        self._ntasks = len([k for (k, v) in dsk.items() if istask(v)])
        self._ndone = 0
        self._update_rate = max(1, self._ntasks // self._nbins)
        self._update_bar()

    def _posttask(self, key, value, dsk, state, id):
        self._ndone += 1
        if not self._ndone % self._update_rate:
            self._update_bar()

    def _finish(self, dsk, state):
        self._finalize_bar()

    @staticmethod
    def _draw_bar(ndone, ntasks, nbins):
        tics = int(ndone * nbins / ntasks)
        bar = '#' * tics
        percent = (100 * ndone) // ntasks
        msg = '\r[{0:<{1}}] | {2}% Completed'.format(bar, nbins, percent)
        sys.stdout.write(msg)
        sys.stdout.flush()

    def _update_bar(self):
        self._draw_bar(self._ndone, self._ntasks, self._nbins)

    def _finalize_bar(self):
        self._draw_bar(self._ndone, self._ntasks, self._nbins)
        sys.stdout.write('\n')
