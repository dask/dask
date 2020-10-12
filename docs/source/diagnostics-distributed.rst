Diagnostics (distributed)
=========================

The :doc:`Dask distributed scheduler <scheduling>` provides live feedback in two
forms:

1.  An interactive dashboard containing many plots and tables with live
    information
2.  A progress bar suitable for interactive use in consoles or notebooks

Dashboard
---------

.. raw:: html

    <iframe width="560"
            height="315"
            src="https://www.youtube.com/embed/N_GqzcuGLCY"
            frameborder="0"
            allow="autoplay; encrypted-media"
            allowfullscreen>
    </iframe>

If `Bokeh <https://bokeh.pydata.org/en/latest/>`_ is installed
then the dashboard will start up automatically whenever the scheduler is created.
For local use this happens when you create a client with no arguments:

.. code-block:: python

   from dask.distributed import Client
   client = Client()  # start distributed scheduler locally.  Launch dashboard

It is typically served at ``http://localhost:8787/status`` ,
but may be served elsewhere if this port is taken.
The address of the dashboard will be displayed if you are in a Jupyter Notebook,
or can be queried from ``client.scheduler_info()['services']``.

There are numerous pages with information about task runtimes, communication,
statistical profiling, load balancing, memory use, and much more.
For more information we recommend the video guide above.

.. currentmodule:: dask.distributed     #doctest: +SKIP

.. autosummary::
   Client


Capture diagnostics
-------------------

.. autosummary::
   get_task_stream
   Client.profile
   performance_report

You can capture some of the same information that the dashboard presents for
offline processing using the ``get_task_stream`` and ``Client.profile``
functions.  These capture the start and stop time of every task and transfer,
as well as the results of a statistical profiler.

.. code-block:: python

   with get_task_stream(plot='save', filename="task-stream.html") as ts:
       x.compute()

   client.profile(filename="dask-profile.html")

   history = ts.data

Additionally, Dask can save many diagnostics dashboards at once including the
task stream, worker profiles, bandwidths, etc. with the ``performance_report``
context manager:

.. code-block:: python

    from dask.distributed import performance_report

    with performance_report(filename="dask-report.html"):
        ## some dask computation

The following video demonstrates the ``performance_report`` context manager in greater
detail:

.. raw:: html

    <iframe width="560"
            height="315"
            src="https://www.youtube.com/embed/nTMGbkS761Q"
            frameborder="0"
            allow="autoplay; encrypted-media"
            allowfullscreen>
    </iframe>


Progress bar
------------

.. currentmodule:: dask.distributed     #doctest: +SKIP

.. autosummary::
   progress

The ``dask.distributed`` progress bar differs from the ``ProgressBar`` used for
:doc:`local diagnostics <diagnostics-local>`.
The ``progress`` function takes a Dask object that is executing in the background:

.. code-block:: python

   # Single machine progress bar
   from dask.diagnostics import ProgressBar

   with ProgressBar():
       x.compute()

   # Distributed scheduler ProgressBar

   from dask.distributed import Client, progress

   client = Client()  # use dask.distributed by default

   x = x.persist()  # start computation in the background
   progress(x)      # watch progress

   x.compute()      # convert to final result when done if desired


Connecting to the Dashboard
---------------------------

Some computer networks may restrict access to certain ports or only allow
access from certain machines.  If you are unable to access the dashboard then
you may want to contact your IT administrator.

Some common problems and solutions follow:

Specify an accessible port
~~~~~~~~~~~~~~~~~~~~~~~~~~

Some clusters restrict the ports that are visible to the outside world.  These
ports may include the default port for the web interface, ``8787``.  There are
a few ways to handle this:

1.  Open port ``8787`` to the outside world.  Often this involves asking your
    cluster administrator.
2.  Use a different port that is publicly accessible using the
    ``--dashboard-address :8787`` option on the ``dask-scheduler`` command.
3.  Use fancier techniques, like `Port Forwarding`_

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

Required Packages
~~~~~~~~~~~~~~~~~

Bokeh must be installed in your scheduler's environment to run the dashboard. If it's not the dashboard page will instruct you to install it.

Depending on your configuration, you might also need to install ``jupyter-server-proxy`` to access the dashboard.

API
---

.. autofunction:: progress
.. autofunction:: get_task_stream
