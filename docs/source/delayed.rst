Delayed
=======

As discussed in the :ref:`custom graphs <custom-graph-example>` section,
sometimes a problem doesn't fit into one of the collections like ``dask.bag`` or
``dask.array``. Instead of creating a dask directly using a dictionary, one can
use the ``dask.delayed`` interface. This allows one to create graphs
directly with a light annotation of normal python code.

.. toctree::
   :maxdepth: 1

   delayed-overview.rst
   delayed-api.rst
   delayed-collections.rst
