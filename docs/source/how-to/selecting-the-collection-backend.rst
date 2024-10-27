Selecting the collection backend
================================


**Warning**: Backend-library disptaching at the collection level is still an experimental feature. Both the ``DaskBackendEntrypoint`` API and the set of "dispatchable" functions are expected to change.


Changing the default backend library
------------------------------------

The Dask-Dataframe and Dask-Array modules were originally designed with the Pandas and Numpy backend libraries in mind, respectively. However, other dataframe and array libraries can take advantage of the same collection APIs for out-of-core and parallel processing. For example, users with `cupy <https://cupy.dev/>`_ installed can change their default Dask-Array backend to ``cupy`` with the ``"array.backend"`` configuration option:

.. code-block:: python

   >>> import dask
   >>> import dask.array as da
   >>> with dask.config.set({"array.backend": "cupy"}):
   ...     darr = da.ones(10, chunks=(5,))  # Get cupy-backed collection

This code opts out of the default (``"numpy"``) backend for dispatchable Dask-Array creation functions, and uses the creation functions registered for ``"cupy"`` instead. The current set of dispatchable creation functions for Dask-Array is:

- ``ones``
- ``zeros``
- ``empty``
- ``full``
- ``arange``

The Dask-Array API can also dispatch the backend ``RandomState`` class to be used for random-number generation. This means all creation functions in ``dask.array.random`` are also dispatchable.

The current set of dispatchable creation functions for Dask-Dataframe is:

- ``from_dict``
- ``read_parquet``
- ``read_json``
- ``read_orc``
- ``read_csv``
- ``read_hdf``

As the backend-library disptaching system becomes more mature, this set of dispatchable creation functions is likely to grow.

For an existing collection, the underlying data can be forcibly moved to a desired backend using the ``to_backend`` method:

.. code-block:: python

   >>> import dask
   >>> import dask.array as da
   >>> darr = da.ones(10, chunks=(5,))  # Creates numpy-backed collection
   >>> with dask.config.set({"array.backend": "cupy"}):
   ...     darr = darr.to_backend()  # Moves numpy data to cupy


Defining a new collection backend
---------------------------------

**Warning**: Defining a custom backend is **not** yet recommended for most users and down-stream libraries. The backend-entrypoint system should still be treated as experimental.


Dask currently exposes an `entrypoint <https://packaging.python.org/specifications/entry-points/>`_ under the group ``dask.array.backends`` and ``dask.dataframe.backends`` to enable users and third-party libraries to develop and maintain backend implementations for Dask-Array and Dask-Dataframe. A custom Dask-Array backend should define a subclass of ``DaskArrayBackendEntrypoint`` (defined in ``dask.array.backends``), while a custom Dask-DataFrame backend should define a subclass of ``DataFrameBackendEntrypoint`` (defined in ``dask.dataframe.backends``).

For example, a cudf-based backend definition for Dask-Dataframe would look something like the `CudfBackendEntrypoint` definition below:


.. code-block:: python

   from dask.dataframe.backends import DataFrameBackendEntrypoint
   from dask.dataframe.dispatch import (
      ...
      make_meta_dispatch,
      ...
   )
   ...

   def make_meta_cudf(x, index=None):
      return x.head(0)
   ...

   class CudfBackendEntrypoint(DataFrameBackendEntrypoint):

      def __init__(self):
         # Register compute-based dispatch functions
         # (e.g. make_meta_dispatch, sizeof_dispatch)
         ...
         make_meta_dispatch.register(
            (cudf.Series, cudf.DataFrame),
            func=make_meta_cudf,
         )
         # NOTE: Registration may also be outside __init__
         # if it is in the same module as this class
      ...

      @staticmethod
      def read_orc(*args, **kwargs):
         from .io import read_orc

         # Use dask_cudf version of read_orc
         return read_orc(*args, **kwargs)
      ...

In order to support pandas-to-cudf conversion with ``DataFrame.to_backend``, this class also needs to implement the proper ``to_backend`` and ``to_backend_dispatch`` methods.

To expose this class as a ``dask.dataframe.backends`` entrypoint, the necessary ``setup.cfg`` configuration in ``cudf`` (or ``dask_cudf``) would be as follows:

.. code-block:: ini

   [options.entry_points]
   dask.dataframe.backends =
      cudf = <module-path>:CudfBackendEntrypoint


Compute dispatch
~~~~~~~~~~~~~~~~


.. note::

   The primary dispatching mechanism for array-like compute operations in both Dask-Array and Dask-DataFrame is the ``__array_function__`` protocol defined in `NEP-18 <https://numpy.org/neps/nep-0018-array-function-protocol.html>`_. For a custom collection backend to be functional, this protocol **must** cover many common numpy functions for the desired array backend. For example, the ``cudf`` backend for Dask-DataFrame depends on the ``__array_function__`` protocol being defined for both ``cudf`` and its complementary array backend (``cupy``). The compute-based dispatch functions discussed in this section correspond to functionality that is not already captured by NEP-18.


Notice that the ``CudfBackendEntrypoint`` definition must define a distinct method definition for each dispatchable creation routine, and register all non-creation (compute-based) dispatch functions within the ``__init__`` logic. These compute dispatch functions do not operate at the collection-API level, but at computation time (within a task). The list of all current "compute" dispatch functions are listed below.

Dask-Array compute-based dispatch functions (as defined in ``dask.array.dispatch``, and defined for Numpy in ``dask.array.backends``):

   - concatenate_lookup
   - divide_lookup
   - einsum_lookup
   - empty_lookup
   - nannumel_lookup
   - numel_lookup
   - percentile_lookup
   - tensordot_lookup
   - take_lookup

Dask-Dataframe compute-based dispatch functions (as defined in ``dask.dataframe.dispatch``, and defined for Pandas in ``dask.dataframe.backends``):

   - categorical_dtype_dispatch
   - concat_dispatch
   - get_collection_type
   - group_split_dispatch
   - grouper_dispatch
   - hash_object_dispatch
   - is_categorical_dtype_dispatch
   - make_meta_dispatch
   - make_meta_obj
   - meta_lib_from_array
   - meta_nonempty
   - pyarrow_schema_dispatch
   - tolist_dispatch
   - union_categoricals_dispatch

Note that the compute-based dispatching system is subject to change. Implementing a complete backend is still expected to require significant effort. However, the long-term goal is to bring further simplicity to this process.
