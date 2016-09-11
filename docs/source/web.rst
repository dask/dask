Web Interface
=============

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-resources.gif
   :alt: Resource plot of Dask web interface
   :width: 70%

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-memory-use.gif
   :alt: Memory use plot of Dask web interface
   :width: 70%

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-task-stream.gif
   :alt: Task stream plot of Dask web interface
   :width: 70%

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-progress.gif
   :alt: Progress plot of Dask web interface
   :width: 70%

Information about the current state of the network helps to track progress,
identify performance issues, and debug failures.

Dask.distributed includes a web interface to help deliver this information over
a normal web page in real time.  This web interface is launched by default
wherever the scheduler is launched if the scheduler machine has Bokeh_
installed (``conda install bokeh -c bokeh``).  The web interface is normally
available at  ``http://scheduler-address:8787/status/`` and can be viewed any
normal web browser.

The available pages are ``http://scheduler-address:8787/<page>/`` where ``<page>`` is one of

- ``status``
- ``tasks``
- ``workers``

.. _Bokeh: http://bokeh.pydata.org/en/latest/

Plots
-----

Example Computation
~~~~~~~~~~~~~~~~~~~

The following plots show a trace of the following computation:

.. code-block:: python

   from distributed import Client
   from time import sleep
   import random

   def inc(x):
       sleep(random.random() / 10)
       return x + 1

   def dec(x):
       sleep(random.random() / 10)
       return x - 1

   def add(x, y):
       sleep(random.random() / 10)
       return x + y


   client = Client('127.0.0.1:8786')

   incs = client.map(inc, range(100))
   decs = client.map(dec, range(100))
   adds = client.map(add, incs, decs)
   total = client.submit(sum, adds)

   del incs, decs, adds
   total.result()

Progress
~~~~~~~~

The interface shows the progress of the various computations as well as the
exact number completed.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-progress.gif
   :alt: Resources view of Dask web interface

Each bar is assigned a color according to the function being run.  Each bar
has a few components.  On the left the lighter shade is the number of tasks
that have both completed and have been released from memory.  The darker shade
to the right corresponds to the tasks that are completed and whose data still
reside  in memory.  If errors occur then they appear as a black colored block
to the right.

Typical computations may involve dozens of kinds of functions.  We handle this
visually with the following approaches:

1.  Functions are ordered by the number of total tasks
2.  The colors are assigned in a round-robin fashion from a standard palette
3.  The progress bars shrink horizontally to make space for more functions
4.  Only the largest functions (in terms of number of tasks) are displayed

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-progress-large.gif
   :alt: Progress bar plot of Dask web interface

Counts of tasks processing, waiting for dependencies, processing, etc.. are
displayed in the title bar.

Memory Use
~~~~~~~~~~

The interface shows the relative memory use of each function with a horizontal
bar sorted by function name.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-memory-use.gif
   :alt: Memory use plot of Dask web interface

The title shows the number of total bytes in use.  Hovering over any bar
tells you the specific function and how many bytes its results are actively
taking up in memory.  This does not count data that has been released.

Task Stream
~~~~~~~~~~~

The task stream plot shows when tasks complete on which workers.  Worker cores
are on the y-axis and time is on the x-axis.  As a worker completes a task its
start and end times are recorded and a rectangle is added to this plot
accordingly.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-task-stream.gif
   :alt: Task stream plot of Dask web interface

If data transfer occurs between workers a *red* bar appears preceding the
task bar showing the duration of the transfer.  If an error occurs than a
*black* bar replaces the normal color.  This plot show the last 1000 tasks.
It resets if there is a delay greater than 10 seconds.

For a full history of the last 100,000 tasks see the ``tasks/`` page.

Resources
~~~~~~~~~

The resources plot show the average CPU and Memory use over time as well as
average network traffic.  More detailed information on a per-worker basis is
available in the ``workers/`` page.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-resources.gif
   :alt: Resources view of Dask web interface

Connecting to Web Interface
---------------------------

Default
~~~~~~~

By default, ``dask-scheduler`` prints out the address of the web interface::

   INFO -  Bokeh UI at:  http://10.129.39.91:8787/status
   ...
   INFO - Starting Bokeh server on port 8787 with applications at paths ['/status', '/tasks']

The machine hosting the scheduler runs an HTTP server serving at that address.


Troubleshooting
---------------

Some clusters restrict the ports that are visible to the outside world.  These
ports may include the default port for the web interface, ``8787``.  There are
a few ways to handle this:

1.  Open port ``8787`` to the outside world.  Often this involves asking your
    cluster administrator.
2.  Use a different port that is publicly accessible using the
    ``--bokeh-port PORT`` option on the ``dask-scheduler`` command.
3.  Use fancier techniques, like `Port Forwarding`_

.. _`Port Forwarding`: https://en.wikipedia.org/wiki/Port_forwarding
Running distributed on a remote machine can cause issues with viewing the web
UI -- this depends on the remote machines network configuration.


Port Forwarding
~~~~~~~~~~~~~~~

If you have SSH access then one way to gain access to a blocked port is through
SSH port forwarding. A typical use case looks like the following:

.. code:: bash

   local$ ssh -L 8000:localhost:8787 user@remote
   remote$ dask-scheduler  # now, the web UI is visible at localhost:8000
   remote$ # continue to set up dask if needed -- add workers, etc

It is then possible to go to ``localhost:8000`` and see Dask Web UI. This same approach is
not specific to dask.distributed, but can be used by any service that operates over a
network, such as Jupyter notebooks. For example, if we chose to do this we could
forward port 8888 (the default Jupyter port) to port 8001 with
``ssh -L 8001:localhost:8888 user@remote``.
