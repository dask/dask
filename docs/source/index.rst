Dask - Parallel Processing Through Blocked Algorithms
=====================================================

Dask enables parallel processing on larger-than-memory datasets.
Dask collections mimic NumPy, Pandas, and Toolz interfaces but operate smoothly
from disk and use all of your cores.  Dask primarily operates on a single
machine and is trivial to set up::

    conda install dask
    or
    pip install dask

Technically speaking, operations on dask collections (Array, Bag, DataFrame)
produce task graphs that encode blocked algorithms.  Dask schedulers execute
these task graphs in parallel in a variety of contexts.

.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 50%

**Collections:**

Dask collections are the main interaction point for users.  They look like
NumPy and Pandas but generate dask graphs internally.  If you are a dask *user*
then you should start here.

.. toctree::
   :maxdepth: 1

   array.rst
   bag.rst
   dataframe.rst

**Graphs:**

Dask graphs encode blocked algorithms in a
.. toctree::
   :maxdepth: 1

   graphs.rst
   spec.rst

**Scheduling:**

High level objects like ``Array``, ``Bag``, and ``DataFrame`` create task
dependency graphs.  These encode blocked algorithms that accomplish macro-scale
effects with many small tasks, each of which is suitable to run on a single
worker.  Neither the high-level objects nor the task graphs that they create encode
the order of computation nor on which workers each task will execute.
This is handled separately by schedulers.

This separation between graph creation and graph execution is key to dask's
design and somewhat particular among similar systems.

Schedulers execute a task dependency graph using whatever workers they have
available.  Schedulers can range in complexity from a quick, 20-line
single-threaded Python function, to large distributed systems.

These schedulers are ignorant of the macro-scale operations or the fact that
they operate on arrays, dataframes, etc..  They only know how to execute Python
functions on data in a way that is consistent with the graph's data
dependencies.

.. toctree::
   :maxdepth: 1

   shared.rst
   distributed.rst

**Administrative:**

.. toctree::
   :maxdepth: 1

   install.rst
   faq.rst

Dask is part of the Blaze_ project supported by `Continuum Analytics`_

.. _Blaze: http://continuum.io/open-source/blaze/
.. _`Continuum Analytics`: http://continuum.io
