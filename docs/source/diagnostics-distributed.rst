Diagnostics (distributed)
=========================

The :doc:`Dask distributed scheduler <scheduling>` provides feedback in two
forms:

1.  A progress bar suitable for interactive use in consoles or notebooks
2.  An interactive dashboard containing several plots and tables with live
    information


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


Dashboard
---------

.. currentmodule:: dask.distributed

.. autosummary::
   Client

If `Bokeh <https://bokeh.pydata.org/en/latest/>`_ is installed
then the dashboard will start up automatically whenever the scheduler is created.
For local use this happens automatically when you create a client with no arguments:

.. code-block:: python

   from dask.distributed import Client
   client = Client()  # start distributed scheduler locally.  Launch dashboard

It is typically served at http://localhost:8787/status ,
but may be served elsewhere if this port is taken.
The address of the dashboard will be displayed if you are in a Jupyter Notebook.

There are numerous pages with information about task runtimes, communication,
statistical profiling, load balancing, memory use, and much more.
For more information we recommend the following video guide:

.. raw:: html

    <iframe width="560"
            height="315"
            src="https://www.youtube.com/embed/N_GqzcuGLCY"
            frameborder="0"
            allow="autoplay; encrypted-media"
            allowfullscreen>
    </iframe>

External Documentation
----------------------

More in-depth technical documentation about Dask's distributed scheduler is
available at https://distributed.dask.org/en/latest

API
---

.. autofunction:: progress
