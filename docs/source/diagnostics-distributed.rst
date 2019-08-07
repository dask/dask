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

It is typically served at http://localhost:8787/status ,
but may be served elsewhere if this port is taken.
The address of the dashboard will be displayed if you are in a Jupyter Notebook,
or can be queriesd from ``client.scheduler_info()['services']``.

There are numerous pages with information about task runtimes, communication,
statistical profiling, load balancing, memory use, and much more.
For more information we recommend the video guide above.

.. currentmodule:: dask.distributed

.. autosummary::
   Client


Progress bar
------------

.. currentmodule:: dask.distributed

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


External Documentation
----------------------

More in-depth technical documentation about Dask's distributed scheduler is
available at https://distributed.dask.org/en/latest

API
---

.. autofunction:: progress
