distributed
===========

Distributed is a lightweight library for distributed computing in Python.  It
extends both the ``concurrent.futures`` and ``dask`` APIs to moderate sized
clusters.  Distributed provides data-local computation by keeping data on
worker nodes, running computations where data lives, and by managing complex
data dependencies between tasks.

See :doc:`the quickstart <quickstart>` to get started.

This library is experimental, broken, and unstable.


**User Documentation**

.. toctree::
   :maxdepth: 2

   quickstart
   executor
   setup

**Developer Documentation**

.. toctree::
   :maxdepth: 2

   foundations
   worker-center
   clients
   scheduler
   related-work
   comparison
