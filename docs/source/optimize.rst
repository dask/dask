Optimization
------------

Small optimizations of the task dependency graph can significantly improve
performance in different contexts.  The ``dask.optimize`` module contains
several utility functions to transform graphs in a variety of useful ways.

.. currentmodule:: dask.optimize

**Top level optimizations**

.. autosummary::
   cull
   dealias
   fuse
   inline
   inline_functions

**Utility functions**

.. autosummary::
   dependency_dict
   equivalent
   functions_of
   merge_sync
   sync_keys

Definitions
~~~~~~~~~~~

.. autofunction:: cull
.. autofunction:: dealias
.. autofunction:: fuse
.. autofunction:: inline
.. autofunction:: inline_functions


.. autofunction:: functions_of
.. autofunction:: equivalent
.. autofunction:: dependency_dict
.. autofunction:: sync_keys
.. autofunction:: merge_sync




