Dask - Parallel Processing Through Blocked Algorithms
=====================================================

Dask collections provide parallel computation on larger-than-memory data while
mimicking existing libraries

* ``dask.array`` = ``numpy`` + ``threading``
* ``dask.bag`` = ``map, filter, toolz`` + ``multiprocessing``
* ``dask.dataframe`` = ``pandas`` + ``multiprocessing`` (experimental)

This increases the scale of comfortable data from *fits-in-memory* to
*fits-on-disk* by intelligently streaming data from disk and by leveraging all
the cores of a modern CPU.

Dask primarily targets parallel computations that run on a single machine.  It
integrates nicely with the existing PyData ecosystem and is trivial to setup
and use::

    conda install dask
    or
    pip install dask

Operations on dask collections (array, bag, dataframe) produce task graphs that
encode blocked algorithms.  Task schedulers execute these task graphs in
parallel in a variety of contexts.

.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 80%
   :align: center

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

Dask graphs encode algorithms in a simple format involving Python dicts,
tuples, and functions.  This graph format can be used in isolation from the
dask collections.  If you are a dask *developer* then you should start here.

.. toctree::
   :maxdepth: 1

   graphs.rst
   spec.rst
   custom-graphs.rst
   optimize.rst

**Scheduling:**

Schedulers execute task graphs.  After a collection produces a graph we execute
this graph in parallel, either using all of the cores on a single workstation
or using a distributed cluster.

.. toctree::
   :maxdepth: 1

   shared.rst
   distributed.rst

**Other**

.. toctree::
   :maxdepth: 1

   install.rst
   inspect.rst
   diagnostics.rst
   faq.rst
   spark.rst

Dask is part of the Blaze_ project supported by `Continuum Analytics`_

.. _Blaze: http://continuum.io/open-source/blaze/
.. _`Continuum Analytics`: http://continuum.io
