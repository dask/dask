Delayed
=======

Sometimes problems don't fit into one of the collections like ``dask.array`` or
``dask.dataframe``. In these cases, users can parallelize custom algorithms
using the simpler ``dask.delayed`` interface. This allows one to create graphs
directly with a light annotation of normal python code.

.. code-block:: python

   >>> x = dask.delayed(inc)(1)
   >>> y = dask.delayed(inc)(2)
   >>> z = dask.delayed(add)(x, y)
   >>> z.compute()
   7
   >>> z.vizualize()

.. image:: images/inc-add.svg
   :alt: simple task graph created with dask.delayed

.. toctree::
   :maxdepth: 1

   delayed-overview.rst
   delayed-api.rst
   delayed-collections.rst
