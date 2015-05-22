Dask - Parallel Processing Through Blocked Algorithms
=====================================================

Dask enables parallel processing on larger-than-memory datasets.
Dask collections mimic NumPy, Pandas, and Toolz interfaces but operate smoothly
from disk and use all of your cores.  Dask primarily operates on a single
machine and is trivial to set up::

    conda install dask
    or
    pip install dask

Operations on dask collections (Array, Bag, DataFrame) produce task graphs that
encode blocked algorithms.  Dask schedulers execute these task graphs in
parallel in a variety of contexts.

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

Dask graphs encode algorithms in a simple format involving Python dicts,
tuples, and functions.  This graph format can be used in isolation from the
dask collections.  This section is useful for developers.

.. toctree::
   :maxdepth: 1

   graphs.rst
   spec.rst
   custom-graphs.rst

**Scheduling:**

Schedulers execute task graphs.  After a collection produces a graph we execute
this graph in parallel, either using all of the cores on a single workstation
or using a distributed cluster.

.. toctree::
   :maxdepth: 1

   shared.rst
   distributed.rst

**Administrative:**

.. toctree::
   :maxdepth: 1

   install.rst
   inspect.rst
   faq.rst
   dask.rst

Dask is part of the Blaze_ project supported by `Continuum Analytics`_

.. _Blaze: http://continuum.io/open-source/blaze/
.. _`Continuum Analytics`: http://continuum.io
