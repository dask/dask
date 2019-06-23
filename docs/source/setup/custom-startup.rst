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


As an example, consider the following file that creates a
:doc:`scheduler plugin <plugins>` and registers it with the scheduler

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


Worker Lifecycle Plugins
------------------------

You can also create a class with setup and teardown methods, and register that
class with the scheduler to give to every worker.

.. currentmodule:: distributed

.. autosummary::
   Client.register_worker_plugin

.. automethod:: Client.register_worker_plugin
