from __future__ import print_function, division, absolute_import

import logging

logger = logging.getLogger(__name__)


class SchedulerPlugin(object):
    """ Interface to extend the Scheduler

    The scheduler operates by triggering and responding to events like
    ``task_finished``, ``update_graph``, ``task_erred``, etc..

    A plugin enables custom code to run at each of those same events.  The
    scheduler will run the analogous methods on this class when each event is
    triggered.  This runs user code within the scheduler thread that can
    perform arbitrary operations in synchrony with the scheduler itself.

    Plugins are often used for diagnostics and measurement, but have full
    access to the scheduler and could in principle affect core scheduling.

    To implement a plugin implement some of the methods of this class and add
    the plugin to the scheduler with ``Scheduler.add_plugin(myplugin)``.

    Examples
    --------
    >>> class Counter(SchedulerPlugin):
    ...     def __init__(self):
    ...         self.counter = 0
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if start == 'processing' and finish == 'memory':
    ...             self.counter += 1
    ...
    ...     def restart(self, scheduler):
    ...         self.counter = 0

    >>> c = Counter()
    >>> scheduler.add_plugin(c)  # doctest: +SKIP
    """

    def update_graph(self, scheduler, dsk=None, keys=None,
                     restrictions=None, **kwargs):
        """ Run when a new graph / tasks enter the scheduler """

    def restart(self, scheduler, **kwargs):
        """ Run when the scheduler restarts itself """

    def transition(self, key, start, finish, *args, **kwargs):
        """ Run whenever a task changes state

        Parameters
        ----------
        key: string
        start: string
            Start state of the transition.
            One of released, waiting, processing, memory, error.
        finish: string
            Final state of the transition.
        *args, **kwargs: More options passed when transitioning
            This may include worker ID, compute time, etc.
        """

    def add_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a new worker enters the cluster """

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        """ Run when a worker leaves the cluster"""
