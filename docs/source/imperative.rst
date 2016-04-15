Imperative
==========

As discussed in the :ref:`custom graphs <custom-graph-example>` section,
sometimes a problem doesn't fit into one of the collections like ``dask.bag`` or
``dask.array``. Instead of creating a dask directly using a dictionary, one can
use the ``dask.imperative`` interface. This allows one to create graphs
directly with a light annotation of normal python code.

.. toctree::
   :maxdepth: 1

   imperative-overview.rst
   imperative-examples.rst
   imperative-api.rst
   imperative-collections.rst
