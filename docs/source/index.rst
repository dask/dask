Dask.distributed
================

Dask.distributed is a lightweight library for distributed computing in Python.
It extends both the ``concurrent.futures`` and ``dask`` APIs to moderate sized
clusters.

See :doc:`the quickstart <quickstart>` to get started.

Motivation
----------

``Distributed`` serves to complement the existing PyData analysis stack.
In particular it meets the following needs:

*   **Low latency:** Each task suffers about 1ms of overhead.  A small
    computation and network roundtrip can complete in less than 10ms.
*   **Peer-to-peer data sharing:** Workers communicate with each other to share
    data.  This removes central bottlenecks for data transfer.
*   **Complex Scheduling:** Supports complex workflows (not just
    map/filter/reduce) which are necessary for sophisticated algorithms used in
    nd-arrays, machine learning, image processing, and statistics.
*   **Pure Python:** Built in Python using well-known technologies.  This eases
    installation, improves efficiency (for Python users), and simplifies debugging.
*   **Data Locality:** Scheduling algorithms cleverly execute computations where
    data lives.  This minimizes network traffic and improves efficiency.
*   **Familiar APIs:** Compatible with the `concurrent.futures`_ API in the
    Python standard library.  Compatible with `dask`_ API for parallel
    algorithms
*   **Easy Setup:** As a Pure Python package distributed is ``pip`` installable
    and easy to :doc:`set up <setup>` on your own cluster.

.. _`concurrent.futures`: https://www.python.org/dev/peps/pep-3148/
.. _`dask`: https://dask.org

Architecture
------------

Dask.distributed is a centrally managed, distributed, dynamic task scheduler.
The central ``dask-scheduler`` process coordinates the actions of several
``dask-worker`` processes spread across multiple machines and the concurrent
requests of several clients.

The scheduler is asynchronous and event driven, simultaneously responding to
requests for computation from multiple clients and tracking the progress of
multiple workers.  The event-driven and asynchronous nature makes it flexible
to concurrently handle a variety of workloads coming from multiple users at the
same time while also handling a fluid worker population with failures and
additions.  Workers communicate amongst each other for bulk data transfer over
TCP.

Internally the scheduler tracks all work as a constantly changing directed
acyclic graph of tasks.  A task is a Python function operating on Python
objects, which can be the results of other tasks.  This graph of tasks grows as
users submit more computations, fills out as workers complete tasks, and
shrinks as users leave or become disinterested in previous results.

Users interact by connecting a local Python session to the scheduler and
submitting work, either by individual calls to the simple interface
``client.submit(function, *args, **kwargs)`` or by using the large data
collections and parallel algorithms of the parent ``dask`` library.  The
collections in the dask_ library like dask.array_ and dask.dataframe_
provide easy access to sophisticated algorithms and familiar APIs like NumPy
and Pandas, while the simple ``client.submit`` interface provides users with
custom control when they want to break out of canned "big data" abstractions
and submit fully custom workloads.

.. _dask.array: https://dask.docs.org/en/latest/array.html
.. _dask.dataframe: https://dask.docs.org/en/latest/dataframe.html

Contents
--------

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   install
   quickstart
   setup
   client
   api
   faq

.. toctree::
   :maxdepth: 1
   :caption: Build Understanding

   diagnosing-performance
   efficiency
   limitations
   locality
   manage-computation
   memory
   priority
   related-work
   resilience
   scheduling-policies
   scheduling-state
   worker
   work-stealing

.. toctree::
   :maxdepth: 1
   :caption: Additional Features

   actors
   adaptive
   asynchronous
   configuration
   local-cluster
   ipython
   Joblib Integration <https://ml.dask.org/joblib.html>
   publish
   queues
   resources
   submitting-applications
   task-launch
   tls
   web

.. toctree::
   :maxdepth: 1
   :caption: Developer Documentation

   changelog
   communications
   develop
   foundations
   journey
   protocol
   serialization
   plugins
