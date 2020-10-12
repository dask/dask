Custom Initialization
=====================

Often we want to run custom code when we start up or tear down a scheduler or
worker.  We might do this manually with functions like ``Client.run`` or
``Client.run_on_scheduler``, but this is error prone and difficult to automate.

To resolve this, Dask includes a few mechanisms to run arbitrary code around
the lifecycle of a Scheduler or Worker.

Preload Scripts
---------------

Both ``dask-scheduler`` and ``dask-worker`` support a ``--preload`` option that
allows custom initialization of each scheduler/worker respectively. A module or
Python file passed as a ``--preload`` value is guaranteed to be imported before
establishing any connection. A ``dask_setup(service)`` function is called if
found, with a ``Scheduler`` or ``Worker`` instance as the argument. As the
service stops, ``dask_teardown(service)`` is called if present.

To support additional configuration, a single ``--preload`` module may register
additional command-line arguments by exposing ``dask_setup`` as a  Click_
command.  This command will be used to parse additional arguments provided to
``dask-worker`` or ``dask-scheduler`` and will be called before service
initialization.

.. _Click: http://click.pocoo.org/

Example
~~~~~~~

As an example, consider the following file that creates a
`scheduler plugin <https://distributed.dask.org/en/latest/plugins.html>`_
and registers it with the scheduler

.. code-block:: python

   # scheduler-setup.py
   import click

   from distributed.diagnostics.plugin import SchedulerPlugin

   class MyPlugin(SchedulerPlugin):
       def __init__(self, print_count):
         self.print_count = print_count
         SchedulerPlugin.__init__(self)

       def add_worker(self, scheduler=None, worker=None, **kwargs):
           print("Added a new worker at:", worker)
           if self.print_count and scheduler is not None:
               print("Total workers:", len(scheduler.workers))

   @click.command()
   @click.option("--print-count/--no-print-count", default=False)
   def dask_setup(scheduler, print_count):
       plugin = MyPlugin(print_count)
       scheduler.add_plugin(plugin)

We can then run this preload script by referring to its filename (or module name
if it is on the path) when we start the scheduler::

   dask-scheduler --preload scheduler-setup.py --print-count

Types
~~~~~

Preloads can be specified as any of the following forms:

-   A path to a script, like ``/path/to/myfile.py``
-   A module name that is on the path, like ``my_module.initialize``
-   The text of a Python script, like ``import os; os.environ["A"] = "value"``

Configuration
~~~~~~~~~~~~~

Preloads can also be registered with configuration at the following values:

.. code-block:: yaml

   distributed:
     scheduler:
       preload:
       - "import os; os.environ['A'] = 'b'"  # use Python text
       - /path/to/myfile.py                  # or a filename
       - my_module                           # or a module name
       preload_argv:
       - []                                  # Pass optional keywords
       - ["--option", "value"]
       - []
     worker:
       preload: []
       preload_argv: []
     nanny:
       preload: []
       preload_argv: []

.. note::

   Because the ``dask-worker`` command needs to accept keywords for both the
   Worker and the Nanny (if a nanny is used) it has both a ``--preload`` and
   ``--preload-nanny`` keyword.  All extra keywords (like ``--print-count``
   above) will be sent to the workers rather than the nanny.  There is no way
   to specify extra keywords to the nanny preload scripts on the command line.
   We recommend the use of the more flexible configuration if this is
   necessary.


Worker Lifecycle Plugins
------------------------

You can also create a class with ``setup``, ``teardown``, and ``transition`` methods,
and register that class with the scheduler to give to every worker using the
``Client.register_worker_plugin`` method.

.. currentmodule:: distributed  #doctest: +SKIP

.. autosummary::
   Client.register_worker_plugin

.. automethod:: Client.register_worker_plugin
   :noindex:
