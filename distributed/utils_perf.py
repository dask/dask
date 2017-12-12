from __future__ import print_function, division, absolute_import

import gc
import logging

from .metrics import thread_time


logger = _logger = logging.getLogger(__name__)


class ThrottledGC(object):
    """Wrap gc.collect to protect against excessively repeated calls.

    Allows to run throttled garbage collection in the workers as a
    countermeasure to e.g.: https://github.com/dask/zict/issues/19

    collect() does nothing when repeated calls are so costly and so frequent
    that the thread would spend more than max_in_gc_frac doing GC.

    warn_if_longer is a duration in seconds (10s by default) that can be used
    to log a warning level message whenever an actual call to gc.collect()
    lasts too long.
    """
    def __init__(self, max_in_gc_frac=0.05, warn_if_longer=1, logger=None):
        self.max_in_gc_frac = max_in_gc_frac
        self.warn_if_longer = warn_if_longer
        self.last_collect = thread_time()
        self.last_gc_duration = 0
        self.logger = logger if logger is not None else _logger

    def collect(self):
        # In case of non-monotonicity in the clock, assume that any Python
        # operation lasts at least 1e-6 second.
        MIN_RUNTIME = 1e-6

        collect_start = thread_time()
        elapsed = max(collect_start - self.last_collect, MIN_RUNTIME)
        if self.last_gc_duration / elapsed < self.max_in_gc_frac:
            self.logger.debug("Calling gc.collect(). %0.3fs elapsed since "
                              "previous call.", elapsed)
            gc.collect()
            self.last_collect = collect_start
            self.last_gc_duration = max(thread_time() - collect_start, MIN_RUNTIME)
            if self.last_gc_duration > self.warn_if_longer:
                self.logger.warning("gc.collect() took %0.3fs. This is usually"
                                    " a sign that the some tasks handle too"
                                    " many Python objects at the same time."
                                    " Rechunking the work into smaller tasks"
                                    " might help.",
                                    self.last_gc_duration)
            else:
                self.logger.debug("gc.collect() took %0.3fs",
                                  self.last_gc_duration)
        else:
            self.logger.debug("gc.collect() lasts %0.3fs but only %0.3fs "
                              "elapsed since last call: throttling.",
                              self.last_gc_duration, elapsed)
