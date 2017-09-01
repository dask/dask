from __future__ import print_function, division, absolute_import

import logging
from tornado import gen
from .client import futures_of, wait
from .utils import sync, tokey
from .utils_comm import pack_data
from .worker import _deserialize

logger = logging.getLogger(__name__)


class ReplayExceptionScheduler(object):
    """ A plugin for the scheduler to recreate exceptions locally

    This adds the following routes to the scheduler

    *  cause_of_failure
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.scheduler.handlers['cause_of_failure'] = self.cause_of_failure
        self.scheduler.extensions['exceptions'] = self

    def cause_of_failure(self, *args, **kwargs):
        """
        Return details of first failed task required by set of keys

        Parameters
        ----------
        keys: list of keys known to the scheduler

        Returns
        -------
        Dictionary with:
        cause: the key that failed
        task: the definition of that key
        deps: keys that the task depends on
        """

        keys = kwargs.pop('keys', [])
        for key in keys:
            if isinstance(key, list):
                key = tuple(key)  # ensure not a list from msgpack
            key = tokey(key)
            if key in self.scheduler.exceptions_blame:
                cause = self.scheduler.exceptions_blame[key]
                # cannot serialize sets
                return {'deps': list(self.scheduler.dependencies[cause]),
                        'cause': cause,
                        'task': self.scheduler.tasks[cause]}


class ReplayExceptionClient(object):
    """
    A plugin for the client allowing replay of remote exceptions locally

    Adds the following methods (and their async variants)to the given client:

    - ``recreate_error_locally``: main user method
    - ``get_futures_error``: gets the task, its details and dependencies,
        responsible for failure of the given future.
    """

    def __init__(self, client):
        self.client = client
        self.client.extensions['exceptions'] = self
        # monkey patch
        self.client.recreate_error_locally = self.recreate_error_locally
        self.client._recreate_error_locally = self._recreate_error_locally
        self.client._get_futures_error = self._get_futures_error
        self.client.get_futures_error = self.get_futures_error

    @property
    def scheduler(self):
        return self.client.scheduler

    @gen.coroutine
    def _get_futures_error(self, future):
        # only get errors for futures that errored.
        futures = [f for f in futures_of(future) if f.status == 'error']
        if not futures:
            raise ValueError("No errored futures passed")
        out = yield self.scheduler.cause_of_failure(
            keys=[f.key for f in futures])
        deps, task = out['deps'], out['task']
        if isinstance(task, dict):
            function, args, kwargs = _deserialize(**task)
            raise gen.Return((function, args, kwargs, deps))
        else:
            function, args, kwargs = _deserialize(task=task)
            raise gen.Return((function, args, kwargs, []))

    def get_futures_error(self, future):
        """
        Ask the scheduler details of the sub-task of the given failed future

        When a future evaluates to a status of "error", i.e., an exception
        was raised in a task within its graph, we an get information from
        the scheduler. This function gets the details of the specific task
        that raised the exception and led to the error, but does not fetch
        data from the cluster or execute the function.

        Parameters
        ----------
        future : future that failed, having ``status=="error"``, typically
            after an attempt to ``gather()`` shows a stack-stace.

        Returns
        -------

        Tuple:
        - the function that raised an exception
        - argument list (a tuple), may include values and keys
        - keyword arguments (a dictionary), may include values and keys
        - list of keys that the function requires to be fetched to run

        See Also
        --------
        ReplayExceptionClient.recreate_error_locally
        """
        return self.client.sync(self._get_futures_error, future)

    @gen.coroutine
    def _recreate_error_locally(self, future):
        yield wait(future)
        out = yield self._get_futures_error(future)
        function, args, kwargs, deps = out
        futures = self.client._graph_to_futures({}, deps)
        data = yield self.client._gather(futures)
        args = pack_data(args, data)
        kwargs = pack_data(kwargs, data)
        raise gen.Return((function, args, kwargs))

    def recreate_error_locally(self, future):
        """
        For a failed calculation, perform the blamed task locally for debugging.

        This operation should be performed after a future (result of ``gather``,
        ``compute``, etc) comes back with a status of "error", if the stack-
        trace is not informative enough to diagnose the problem. The specific
        task (part of the graph pointing to the future) responsible for the
        error will be fetched from the scheduler, together with the values of
        its inputs. The function will then be executed, so that ``pdb`` can
        be used for debugging.

        Examples
        --------
        >>> future = c.submit(div, 1, 0)         # doctest: +SKIP
        >>> future.status                        # doctest: +SKIP
        'error'
        >>> c.recreate_error_locally(future)     # doctest: +SKIP
        ZeroDivisionError: division by zero

        If you're in IPython you might take this opportunity to use pdb

        >>> %pdb                                 # doctest: +SKIP
        Automatic pdb calling has been turned ON

        >>> c.recreate_error_locally(future)     # doctest: +SKIP
        ZeroDivisionError: division by zero
              1 def div(x, y):
        ----> 2     return x / y
        ipdb>

        Parameters
        ----------
        future : future or collection that failed
            The same thing as was given to ``gather``, but came back with
            an exception/stack-trace. Can also be a (persisted) dask collection
            containing any errored futures.

        Returns
        -------
        Nothing; the function runs and should raise an exception, allowing
        the debugger to run.
        """
        func, args, kwargs = sync(self.client.loop,
                                  self._recreate_error_locally, future)
        func(*args, **kwargs)
