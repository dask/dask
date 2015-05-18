Create Dask DataFrames
======================

You can manually create DataFrames from NumPy array's with record dtype.

.. code-block:: Python

    >>> import numpy as np
    >>> import dask.dataframe as dd
    >>> from dask.dataframe.io import from_array
    >>> x = np.array([(i, i*10) for i in range(10)],
                     dtype=[('a', 'i4'), ('b', ('i4')]))
    >>> dd.from_array(x, chunksize=4)


From a CSV file.
-----------------

beep


From bcolz??
------------

boop
