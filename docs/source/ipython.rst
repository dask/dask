IPython Integration
===================

Dask.distributed integrates with IPython in three ways:

1.  You can launch a Dask.distributed cluster from an IPyParallel_ cluster
2.  You can launch IPython kernels from Dask Workers and Schedulers to assist
    with debugging
3.  They both support the common concurrent.futures_ interface

.. _IPyParallel: https://ipyparallel.readthedocs.io/en/latest/
.. _concurrent.futures: https://docs.python.org/3/library/concurrent.futures.html


Launch Dask from IPyParallel
----------------------------

IPyParallel is IPython's distributed computing framework that allows you to
easily manage many IPython engines on different computers.

An IPyParallel ``Client`` can launch a ``dask.distributed`` Scheduler and
Workers on those IPython engines, effectively launching a full dask.distributed
system.

This is possible with the Client.become_dask_ method::

   $ ipcluster start

.. code-block:: python

   >>> from ipyparallel import Client
   >>> c = Client()  # connect to IPyParallel cluster

   >>> e = c.become_dask()  # start dask on top of IPyParallel
   >>> e
   <Client: scheduler="127.0.0.1:59683" processes=8 cores=8>

.. _Client.become_dask: https://ipyparallel.readthedocs.io/en/latest/api/ipyparallel.html#ipyparallel.Client.become_dask


Launch IPython within Dask Workers
----------------------------------

It is sometimes convenient to inspect the ``Worker`` or ``Scheduler`` process
interactively.  Fortunately IPython gives us a way to launch interactive
sessions within Python processes.  This is available through the following
methods:

.. currentmodule:: distributed.client

.. autosummary::
   Client.start_ipython_workers
   Client.start_ipython_scheduler

These methods start IPython kernels running in a separate thread within the
specified Worker or Schedulers.  These kernels are accessible either through
IPython magics or a QT-Console.

Example with IPython Magics
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   >>> e.start_ipython_scheduler()
   >>> %scheduler scheduler.processing
   {'127.0.0.1:3595': ['inc-1', 'inc-2'],
    '127.0.0.1:53589': ['inc-2', 'add-5']}

   >>> info = e.start_ipython_workers()
   >>> %remote info['127.0.0.1:3595'] worker.active
   {'inc-1', 'inc-2'}

Example with qt-console
~~~~~~~~~~~~~~~~~~~~~~~

You can also open up a full interactive `IPython qt-console`_ on the scheduler
or each of the workers:

.. code-block:: python

   >>> e.start_ipython_scheduler(qtconsole=True)
   >>> e.start_ipython_workers(qtconsole=True)

.. _`IPython qt-console`: https://ipython.org/ipython-doc/3/interactive/qtconsole.html
