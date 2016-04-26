====
Dask
====

Dask is a flexible parallel computing library for analytics.  Dask emphasizes
the following virtues:

*  **Familiar**: Provides parallelized NumPy array and Pandas DataFrame objects
*  **Native**: Enables distributed computing in Pure Python with access to
   the PyData stack.
*  **Fast**: Operates with low overhead, low latency, and minimal serialization
   necessary for fast numerical algorithms
*  **Flexible**: Supports complex and messy workloads
*  **Scales up**: Runs resiliently on clusters with 100s of nodes
*  **Scales down**: Trivial to set up and run on a laptop in a single process
*  **Responsive**: Designed with interactive computing in mind it provides rapid
   feedback and diagnostics to aid humans


.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 80%
   :align: center


Familiar user interface
-----------------------

**Dask DataFrame** mimics Pandas

.. code-block:: python

    import pandas as pd                     import dask.dataframe as dd
    df = pd.read_csv('2015-01-01.csv')      df = dd.read_csv('2015-*-*.csv')
    df.groupby(df.user_id).value.mean()     df.groupby(df.user_id).value.mean().compute()

**Dask Array** mimics NumPy

.. code-block:: python

   import numpy as np                       import dask.array as da
   f = h5py.File('myfile.hdf5')             f = h5py.File('myfile.hdf5')
   x = np.array(f['/small-data'])           x = da.from_array(f['/big-data'],
                                                              chunks=(1000, 1000))
   x - x.mean(axis=1)                       x - x.mean(axis=1).compute()

**Dask Bag** mimics iterators, Toolz, PySpark

.. code-block:: python

   import dask.bag as db
   b = db.read_text('2015-*-*.json.gz').map(json.loads)
   b.pluck('name').frequencies().topk(10, lambda pair: pair[1]).compute()

**Dask Delayed** mimics for loops and wraps custom code

.. code-block:: python

   from dask import delayed
   L = []
   for fn in filenames:                  # Use for loops to build up computation
       data = delayed(load)(fn)          # Delay execution of function
       L.append(delayed(process)(data))  # Build connections between variables

   result = delayed(summarize)(L)
   result.compute()


Scales from laptops to clusters
-------------------------------

Dask is convenient on a laptop.  It :doc:`installs <install>` trivially with
``conda`` or ``pip`` and extends the size of convenient datasets from "fits in
memory" to "fits on disk".

Dask can scale to a cluster of 100s of machines. It is resilient, elastic, data
local, and low latency.  For more information see documentation on the
`distributed scheduler`_.

This ease of transition between single-machine to moderate cluster enables
users both to start simple and to grow when necessary.


Complex Algorithms
------------------

Dask represents parallel computations with :doc:`task graphs<graphs>`.  These
directed acyclic graphs may have arbitrary structure, which enables both
developers and users the freedom to build sophisticated algorithms and to
handle messy situations not easily managed by the ``map/filter/groupby``
paradigm common in most data engineering frameworks.

We originally needed this complexity to build complex algorithms for
n-dimensional arrays but have found it to be equally valuable when dealing with
messy situations in everyday problems.


Index
-----

**Getting Started**

* :doc:`install`
* :doc:`examples-tutorials`
* :doc:`cheatsheet`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   install.rst
   examples-tutorials.rst
   cheatsheet.rst

**Collections**

Dask collections are the main interaction point for users. They look like
NumPy and pandas but generate dask graphs internally. If you are a dask *user*
then you should start here.

* :doc:`array`
* :doc:`bag`
* :doc:`dataframe`
* :doc:`delayed`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Collections

   array.rst
   bag.rst
   dataframe.rst
   delayed.rst

**Graphs**

Dask graphs encode algorithms in a simple format involving Python dicts,
tuples, and functions.  This graph format can be used in isolation from the
dask collections.  Working directly with dask graphs is an excellent way to
implement and test new algorithms in fields such as linear algebra,
optimization, and machine learning.  If you are a *developer*, you should start
here.

* :doc:`graphs`
* :doc:`spec`
* :doc:`custom-graphs`
* :doc:`optimize`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Graphs

   graphs.rst
   spec.rst
   custom-graphs.rst
   optimize.rst

**Scheduling**

Schedulers execute task graphs.  Dask currently has two main schedulers, one
for single machine processing using threads or processes, and one for
distributed memory clusters.

* :doc:`scheduler-overview`
* :doc:`Single machine scheduler<shared>`
* `Distributed scheduler`_  (separate webpage)
* :doc:`scheduling-policy`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Scheduling

   scheduler-overview.rst
   shared.rst
   scheduling-policy.rst

**Inspecting and Diagnosing Graphs**

Parallel code can be tricky to debug and profile. Dask provides a few tools to
help make debugging and profiling graph execution easier.

* :doc:`inspect`
* :doc:`diagnostics`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Diagnostics

   inspect.rst
   diagnostics.rst

**Help & reference**

* :doc:`support`
* :doc:`faq`
* :doc:`spark`
* :doc:`caching`
* :doc:`cite`
* :doc:`glossary`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Help & reference

   support.rst
   faq.rst
   spark.rst
   caching.rst
   cite.rst
   glossary.rst

**Contact**

* For user questions please tag StackOverflow questions with the `#dask tag`_.
* For bug reports and feature requests please use the `GitHub issue tracker`_
* For community discussion please use `blaze-dev@continuum.io`_
* For chat, see `gitter chat room`_

Dask is part of the Blaze_ project supported and offered by
`Continuum Analytics`_ and contributors under a `3-clause BSD
license`_.

.. _Blaze: http://continuum.io/open-source/blaze
.. _`Continuum Analytics`: http://continuum.io
.. _`3-clause BSD license`: https://github.com/dask/dask/blob/master/LICENSE.txt

.. _`#dask tag`: http://stackoverflow.com/questions/tagged/dask
.. _`GitHub issue tracker`: https://github.com/dask/dask/issues
.. _`blaze-dev@continuum.io`: https://groups.google.com/a/continuum.io/forum/#!forum/blaze-dev
.. _`gitter chat room`: https://gitter.im/dask/dask
.. _`xarray`: http://xray.readthedocs.org/en/stable/
.. _`scikit-image`: http://scikit-image.org/docs/stable/
.. _`scikit-allel`: https://scikits.appspot.com/scikit-allel
.. _`pandas`: http://pandas.pydata.org/pandas-docs/version/0.17.0/
.. _`distributed scheduler`: http://distributed.readthedocs.org/en/latest/
.. _`Distributed scheduler`: http://distributed.readthedocs.org/en/latest/
