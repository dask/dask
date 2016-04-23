Build Custom Arrays
===================

Here we have a serial blocked computation for computing the mean of all
positive elements in a large, on disk array:

.. code-block:: python

    x = h5py.File('myfile.hdf5')['/x']          # Trillion element array on disk

    sums = []
    counts = []
    for i in range(1000000):                    # One million times
        chunk = x[1000000*i:1000000*(i + 1)]    # Pull out chunk
        positive = chunk[chunk > 0]             # Filter out negative elements
        sums.append(positive.sum())             # Sum chunk
        counts.append(positive.size)            # Count chunk

    result = sum(sums) / sum(counts)            # Aggregate results


Below is the same code, parallelized using ``dask.delayed``:

.. code-block:: python

    x = delayed(h5py.File('myfile.hdf5')['/x'])   # Trillion element array on disk

    sums = []
    counts = []
    for i in range(1000000):                    # One million times
        chunk = x[1000000*i:1000000*(i + 1)]    # Pull out chunk
        positive = chunk[chunk > 0]             # Filter out negative elements
        sums.append(positive.sum())             # Sum chunk
        counts.append(positive.size)            # Count chunk

    result = delayed(sum)(sums) / delayed(sum)(counts)    # Aggregate results

    result.compute()                            # Perform the computation


Only 3 lines had to change to make this computation parallel instead of serial.

- Wrap the original array in ``delayed``. This makes all the slices on it return
  ``Delayed`` objects.
- Wrap both calls to ``sum`` with ``delayed``.
- Call the ``compute`` method on the result.

While the for loop above still iterates fully, it's just building up a graph of
the computation that needs to happen, without actually doing any computing.
