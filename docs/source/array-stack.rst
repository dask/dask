Stack and Concatenate
=====================

Often we have many arrays stored on disk that we want to stack together and
think of as one large array.  This is common with geospatial data in which we
might have many HDF5/NetCDF files on disk, one for every day, but we want to do
operations that span multiple days.

To solve this problem we use the functions ``da.stack`` and ``da.concatenate``.

Stack
-----

We stack many existing Dask arrays into a new array, creating a new dimension
as we go.

.. code-block:: python

   >>> import dask.array as da

   >>> arr0 = da.from_array(np.zeros((3, 4)), chunks=(1, 2))
   >>> arr1 = da.from_array(np.ones((3, 4)), chunks=(1, 2))

   >>> data = [arr0, arr1]

   >>> x = da.stack(data, axis=0)
   >>> x.shape
   (2, 3, 4)

   >>> da.stack(data, axis=1).shape
   (3, 2, 4)

   >>> da.stack(data, axis=-1).shape
   (3, 4, 2)

This creates a new dimension with length equal to the number of slices

Concatenate
-----------

We concatenate existing arrays into a new array, extending them along an
existing dimension

.. code-block:: python

   >>> import dask.array as da
   >>> import numpy as np

   >>> arr0 = da.from_array(np.zeros((3, 4)), chunks=(1, 2))
   >>> arr1 = da.from_array(np.ones((3, 4)), chunks=(1, 2))

   >>> data = [arr0, arr1]

   >>> x = da.concatenate(data, axis=0)
   >>> x.shape
   (6, 4)

   >>> da.concatenate(data, axis=1).shape
   (3, 8)
