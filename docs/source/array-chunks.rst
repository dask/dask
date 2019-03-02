Chunks
======

Dask arrays are composed of many NumPy arrays.  How these arrays are arranged
can significantly affect performance.  For example, for a square array you might
arrange your chunks along rows, along columns, or in a more square-like
fashion.  Different arrangements of NumPy arrays will be faster or slower for
different algorithms.

Thinking about and controlling chunking is important to optimize advanced
algorithms.

Specifying Chunk shapes
-----------------------

We always specify a ``chunks`` argument to tell dask.array how to break up the
underlying array into chunks.  We can specify ``chunks`` in a variety of ways:

1.  A uniform dimension size like ``1000``, meaning chunks of size ``1000`` in each dimension
2.  A uniform chunk shape like ``(1000, 2000, 3000)``, meaning chunks of size ``1000`` in the
    first axis, ``2000`` in the second axis, and ``3000`` in the third
3.  Fully explicit sizes of all blocks along all dimensions,
    like ``((1000, 1000, 500), (400, 400), (5, 5, 5, 5, 5))``
4.  A dictionary specifying chunk size per dimension like ``{0: 1000, 1: 2000,
    2: 3000}``.  This is just another way of writing the forms 2 and 3 above

Your chunks input will be normalized and stored in the third and most explicit
form.  Note that ``chunks`` stands for "chunk shape" rather than "number of
chunks", so specifying ``chunks=1`` means that you will have many chunks,
each with exactly one element.

For performance, a good choice of ``chunks`` follows the following rules:

1.  A chunk should be small enough to fit comfortably in memory.  We'll
    have many chunks in memory at once
2.  A chunk must be large enough so that computations on that chunk take
    significantly longer than the 1ms overhead per task that Dask scheduling
    incurs.  A task should take longer than 100ms
3.  Chunk sizes between 10MB-1GB are common, depending on the availability of
    RAM and the duration of computations
4.  Chunks should align with the computation that you want to do.

    For example, if you plan to frequently slice along a particular dimension,
    then it's more efficient if your chunks are aligned so that you have to
    touch fewer chunks.  If you want to add two arrays, then its convenient if
    those arrays have matching chunks patterns

5.  Chunks should align with your storage, if applicable.

    Array data formats are often chunked as well.  When loading or saving data,
    if is useful to have Dask array chunks that are aligned with the chunking
    of your storage, often an even multiple times larger in each direction


Unknown Chunks
--------------

Some arrays have unknown chunk sizes.  This arises whenever the size of an
array depends on lazy computations that we haven't yet performed like the
following:

.. code-block:: python

   x = x[x > 100]  # don't know how many values are greater than 100 ahead of time

Operations like the above result in arrays with unknown shapes and unknown
chunk sizes.  Unknown values within shape or chunks are designated using
``np.nan`` rather than an integer.  These arrays support many (but not all)
operations.  In particular, operations like slicing are not possible and will
result in an error.

.. code-block:: python

   >>> x.shape
   (np.nan, np.nan)

   >>> x[100]
   ValueError: Array chunk sizes unknown

This can also happen when creating a Dask array from a Dask DataFrame:

.. code-block:: python

   >>> ddf = dask.dataframe.from_pandas(...)
   >>> ddf.to_dask_array()
   ... dask.array<values, shape=(nan, 2), dtype=float64, chunksize=(nan, 2)>

For details on how to avoid unknown chunk sizes, look at how to create a Dask
array from a Dask DataFrame in the :doc:`documentation on Dask array creation
<array-creation>`.

Chunks Examples
---------------

In this example we show how different inputs for ``chunks=`` cut up the following array::

   1 2 3 4 5 6
   7 8 9 0 1 2
   3 4 5 6 7 8
   9 0 1 2 3 4
   5 6 7 8 9 0
   1 2 3 4 5 6

Here, we show how different ``chunks=`` arguments split the array into different blocks

**chunks=3**: Symmetric blocks of size 3::

   1 2 3  4 5 6
   7 8 9  0 1 2
   3 4 5  6 7 8

   9 0 1  2 3 4
   5 6 7  8 9 0
   1 2 3  4 5 6

**chunks=2**: Symmetric blocks of size 2::

   1 2  3 4  5 6
   7 8  9 0  1 2

   3 4  5 6  7 8
   9 0  1 2  3 4

   5 6  7 8  9 0
   1 2  3 4  5 6

**chunks=(3, 2)**: Asymmetric but repeated blocks of size ``(3, 2)``::

   1 2  3 4  5 6
   7 8  9 0  1 2
   3 4  5 6  7 8

   9 0  1 2  3 4
   5 6  7 8  9 0
   1 2  3 4  5 6

**chunks=(1, 6)**: Asymmetric but repeated blocks of size ``(1, 6)``::

   1 2 3 4 5 6

   7 8 9 0 1 2

   3 4 5 6 7 8

   9 0 1 2 3 4

   5 6 7 8 9 0

   1 2 3 4 5 6

**chunks=((2, 4), (3, 3))**: Asymmetric and non-repeated blocks::

   1 2 3  4 5 6
   7 8 9  0 1 2

   3 4 5  6 7 8
   9 0 1  2 3 4
   5 6 7  8 9 0
   1 2 3  4 5 6

**chunks=((2, 2, 1, 1), (3, 2, 1))**: Asymmetric and non-repeated blocks::

   1 2 3  4 5  6
   7 8 9  0 1  2

   3 4 5  6 7  8
   9 0 1  2 3  4

   5 6 7  8 9  0

   1 2 3  4 5  6

**Discussion**

The latter examples are rarely provided by users on original data but arise from complex slicing and broadcasting operations.  Generally people use the simplest form until they need more complex forms.  The choice of chunks should align with the computations you want to do.

For example, if you plan to take out thin slices along the first dimension, then you might want to make that dimension skinnier than the others.  If you plan to do linear algebra, then you might want more symmetric blocks.


Loading Chunked Data
--------------------

Modern NDArray storage formats like HDF5, NetCDF, TIFF, and Zarr, allow arrays
to be stored in chunks or tiles so that blocks of data can be pulled out
efficiently without having to seek through a linear data stream.  It is best to
align the chunks of your Dask array with the chunks of your underlying data
store.

However, data stores often chunk more finely than is ideal for Dask array, so
it is common to choose a chunking that is a multiple of your storage chunk
size, otherwise you might incur high overhead.

For example, if you are loading a data store that is chunked in blocks of
``(100, 100)``, then you might choose a chunking more like ``(1000, 2000)`` that
is larger, but still evenly divisible by ``(100, 100)``.  Data storage
technologies will be able to tell you how their data is chunked.


Rechunking
----------

.. currentmodule:: dask.array

.. autosummary:: rechunk

Sometimes you need to change the chunking layout of your data.  For example,
perhaps it comes to you chunked row-wise, but you need to do an operation that
is much faster if done across columns.  You can change the chunking with the
``rechunk`` method.

.. code-block:: python

   x = x.rechunk((50, 1000))

Rechunking across axes can be expensive and incur a lot of communication, but
Dask array has fairly efficient algorithms to accomplish this.

You can pass rechunk any valid chunking form:

.. code-block:: python

   x = x.rechunk(1000)
   x = x.rechunk((50, 1000))
   x = x.rechunk({0: 50, 1: 1000})


Automatic Chunking
------------------

Chunks also includes three special values:

1.  ``-1``: no chunking along this dimension
2.  ``None``: no change to the chunking along this dimension (useful for rechunk)
3.  ``"auto"``: allow the chunking in this dimension to accomodate ideal chunk sizes

So, for example, one could rechunk a 3D array to have no chunking along the zeroth
dimension, but still have sensible chunk sizes as follows:

.. code-block:: python

   x = x.rechunk({0: -1, 1: 'auto', 2: 'auto'})

Or one can allow *all* dimensions to be auto-scaled to get to a good chunk
size:

.. code-block:: python

   x = x.rechunk('auto')

Automatic chunking expands or contracts all dimensions marked with ``"auto"``
to try to reach chunk sizes with a number of bytes equal to the config value
``array.chunk-size``, which is set to 128MiB by default, but which you can
change in your :doc:`configuration <configuration>`.

.. code-block:: python

   >>> dask.config.get('array.chunk-size')
   '128MiB'

Automatic rechunking tries to respect the median chunk shape of the
auto-rescaled dimensions, but will modify this to accomodate the shape of the
full array (can't have larger chunks than the array itself) and to find
chunk shapes that nicely divide the shape.

These values can also be used when creating arrays with operations like
``dask.array.ones`` or ``dask.array.from_array``

.. code-block:: python

   >>> dask.array.ones((10000, 10000), chunks=(-1, 'auto'))
   dask.array<wrapped, shape=(10000, 10000), dtype=float64, chunksize=(10000, 1250)>
