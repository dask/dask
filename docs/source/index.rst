distributed
===========

Distributed is a lightweight library for distributed computing in Python.  It
extends both the ``concurrent.futures`` and ``dask`` APIs to moderate sized
clusters.  Distributed provides data-local computation by keeping data on
worker nodes, running computations where data lives, and by managing complex
data dependencies between tasks.

See :doc:`the quickstart <quickstart>` to get started.


Motivation
----------

Why build yet-another-distributed-system?

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
.. _`dask`: http://dask.pydata.org/en/latest/

Contents
--------

.. toctree::
   :maxdepth: 1
   :caption: Getting Started

   install
   quickstart
   setup
   dec2
   web
   examples-overview

.. toctree::
   :maxdepth: 1
   :caption: Using Distributed

   executor
   efficiency
   locality
   joblib
   queues
   api

.. toctree::
   :maxdepth: 1
   :caption: Developer Documentation

   foundations
   clients
   worker-center
   scheduler
   resilience
   journey
   protocol
   plugins
   related-work
