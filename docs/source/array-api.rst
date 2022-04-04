API
---

.. currentmodule:: dask.array


Array
~~~~~

.. autoclass:: Array
   :noindex:

.. autosummary::
    :toctree: generated
    :recursive:
    :template: custom-class-template.rst

    Array

Fast Fourier Transforms
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
    :toctree: generated
    :recursive:
    :template: custom-module-template.rst

    fft


Linear Algebra
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated
   :template: custom-module-template.rst
   :recursive:

   linalg

Masked Arrays
~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   ma

Random
~~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   random

Stats
~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   stats

Image Support
~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   image 
    

Slightly Overlapping Computations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   overlap
   lib.stride_tricks

Create and Store Arrays
~~~~~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   from_array
   from_delayed
   from_npy_stack
   from_zarr
   from_tiledb
   store
   to_hdf5
   to_zarr
   to_npy_stack
   to_tiledb

Generalized Ufuncs
~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/
   :template: custom-module-template.rst
   :recursive:

   gufunc

Internal functions
~~~~~~~~~~~~~~~~~~

.. currentmodule:: dask.array.core

.. autosummary::
   :toctree: generated/

   blockwise
   normalize_chunks
   unify_chunks
