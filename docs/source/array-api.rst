API
---

.. currentmodule:: dask.array.core

Create and Store Arrays
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   from_array
   store
   Array.to_hdf5

Specialized Functions for Dask.Array
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   Array.map_blocks
   Array.map_overlap
   topk
   coarsen
   stack
   concatenate

.. currentmodule:: dask.array.rechunk
   rechunk

Array Methods
~~~~~~~~~~~~~

.. currentmodule:: dask.array.core

.. autoclass:: Array
   :members:


Other functions
~~~~~~~~~~~~~~~

.. autofunction:: from_array
.. autofunction:: store
.. autofunction:: Array.to_hdf5
.. autofunction:: Array.map_blocks
.. autofunction:: Array.map_overlap
.. autofunction:: topk
.. autofunction:: coarsen
.. autofunction:: stack
.. autofunction:: concatenate
