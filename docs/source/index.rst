.. distributed documentation master file, created by
   sphinx-quickstart on Tue Oct  6 14:42:44 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

distributed
===========

Distributed is a lightweight library for distributed computing in Python.  It
extends both the ``concurrent.futures`` and ``dask`` APIs to moderate sized
clusters.  See :doc:`the quickstart <quickstart>` to get started.

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
