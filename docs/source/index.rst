Dask - A Task Scheduling Specification
======================================

Dask encodes directed acyclic graphs of task-oriented computations
using ordinary Python dicts, tuples, and functions.

Dask schedulers execute these graphs efficiently.

Dask Arrays and Bags help users create complex task graphs through high-level
operations.

Contents
--------

.. toctree::
   :maxdepth: 1

   overview.rst
   spec.rst
   array.rst
   frame.rst
   bag.rst
   pframe.rst
   scheduling.rst
   distributed-design.rst
   faq.rst

Dask is part of the Blaze_ project supported by `Continuum Analytics`_

.. _Blaze: http://continuum.io/open-source/blaze/
.. _`Continuum Analytics`: http://continuum.io
