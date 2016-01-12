====
Dask
====

Dask is a simple task scheduling system that uses directed acyclic graphs (DAGs) 
of tasks to break up large computations into many small ones.

**Dask enables parallel computing** through task scheduling and blocked algorithms.
This allows developers to write complex parallel algorithms and execute them
in parallel either on a modern multi-core machine or on a distributed cluster.

On a single machine dask increases the scale of comfortable data from
*fits-in-memory* to *fits-on-disk* by intelligently streaming data from disk
and by leveraging all the cores of a modern CPU.

Dask use cases
==============

* **Scientific computing libraries:** Projects like `xray`_ and `scikit-image`_ 
  use dask.array for fast interactive analysis of large numeric datasets.
* **Large unstructured or semi-structured data:** Dask.bag excels at 
  processing large JSON blobs and log files.
* **Scaling pandas-like data analysis to large datasets:** Dask.dataframe 
  implements a part of the pandas interface, and can process data files larger than 
  available memory.
* **Algorithm development:** Working directly with dask graphs or dask.imperative 
  is an excellent way to implement and test new algorithms in fields such as linear 
  algebra, optimization, and machine learning.

Dask user intro
===============

Users interact with dask either by making graphs directly, or through the 
*dask collections* which provide larger-than-memory counterparts to existing 
popular libraries:

* ``dask.array`` = ``numpy`` + ``threading``
* ``dask.bag`` = ``map, filter, toolz`` + ``multiprocessing``
* ``dask.dataframe`` = ``pandas`` + ``threading``

Dask primarily targets parallel computations that run on a single machine. It
integrates nicely with the existing PyData ecosystem and is trivial to setup
and use::

    conda install dask
    or
    pip install dask

Operations on dask collections (array, bag, dataframe) produce task graphs that
encode blocked algorithms. Task schedulers execute these task graphs in
parallel in a variety of contexts.

.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 80%
   :align: center

**Collections**

Dask collections are the main interaction point for users. They look like
NumPy and pandas but generate dask graphs internally. If you are a dask *user*
then you should start here.

* :doc:`array`
* :doc:`bag`
* :doc:`dataframe`
* :doc:`imperative`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Collections

   array.rst
   bag.rst
   dataframe.rst
   imperative.rst

**Graphs**

Dask graphs encode algorithms in a simple format involving Python dicts,
tuples, and functions.  This graph format can be used in isolation from the
dask collections.  If you are a *developer*, you should start here.

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

Schedulers execute task graphs.  After a collection produces a graph we execute
this graph in parallel, either using all of the cores on a single workstation
or using a distributed cluster.

* :doc:`scheduler-overview`
* :doc:`shared`
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

* :doc:`install`
* :doc:`glossary`
* :doc:`faq`
* :doc:`spark`
* :doc:`caching`
* :doc:`get-involved`

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Help & reference

   install.rst
   glossary.rst
   faq.rst
   spark.rst
   caching.rst
   get-involved.rst

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
.. _`3-clause BSD license`: https://github.com/blaze/dask/blob/master/LICENSE.txt

.. _`#dask tag`: http://stackoverflow.com/questions/tagged/dask
.. _`GitHub issue tracker`: https://github.com/blaze/dask/issues
.. _`blaze-dev@continuum.io`: https://groups.google.com/a/continuum.io/forum/#!forum/blaze-dev
.. _`gitter chat room`: https://gitter.im/blaze/dask
.. _`xray`: http://xray.readthedocs.org/en/stable/dask.html
.. _`scikit-image`: http://scikit-image.org/docs/stable/
.. _`pandas`: http://pandas.pydata.org/pandas-docs/version/0.17.0/
