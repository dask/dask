Dataframes from HDF5 files
===========================

This section provides working examples of ``dask.dataframe`` methods to read HDF5 files. HDF5 is a unique technology suite that makes possible the management of large and complex data collections. To learn more about HDF5, visit the `HDF Group Tutorial page <https://www.hdfgroup.org/HDF5/whatishdf5.html>`_.  For an overview of ``dask.dataframe``, its limitations, scope, and use, see the :doc:`DataFrame overview section<../dataframe-overview>`.  

**Important Note** -- ``dask.dataframe.read_hdf`` uses ``pandas.read_hdf``, thereby inheriting its abilities and limitations.  See `pandas HDF5 documentation <http://pandas.pydata.org/pandas-docs/stable/io.html#hdf5-pytables>`_ for more information. 
    
Examples Covered
----------------------------------------------

*  Use ``dask.dataframe`` to:

1.  Create dask DataFrame by loading a specific dataset (key) from a single HDF5 file
2.  Create dask DataFrame from a single HDF5 file with multiple datasets (keys)  
3.  Create dask DataFrame by loading multiple HDF5 files with different datasets (keys)

  
Generate Example Data
----------------------------------------------

Here is some code to generate sample HDF5 files.

.. code-block:: python

    import string, json, random
    import pandas as pd
    import numpy as np

    # dict to keep track of hdf5 filename and each key
    fileKeys = {}

    for i in range(10):
        # randomly pick letter as dataset key
        groupkey = random.choice(list(string.ascii_lowercase))

        # randomly pick a number as hdf5 filename
        filename = 'my' + str(np.random.randint(100)) + '.h5'

        # Make a dataframe; 26 rows, 2 columns
        df = pd.DataFrame({'x': np.random.randint(1, 1000, 26),
                           'y': np.random.randint(1, 1000, 26)},
                           index=list(string.ascii_lowercase))

        # Write hdf5 to current directory
        df.to_hdf(filename, key='/' + groupkey, format='table')
        fileKeys[filename] = groupkey
    
    print(fileKeys) # prints hdf5 filenames and keys for each


Read single dataset from HDF5
--------------------------------------------

The first order of ``dask.dataframe`` business is creating a dask DataFrame using a single HDF5 file's dataset.  The code to accomplish this task is:

.. code-block:: python

    import dask.dataframe as dd
    df = dd.read_hdf('my86.h5', key='/c')
    
Load multiple datasets from single HDF5 file
------------------------------------------------- 

Loading multiple datasets from a single file requires a small tweak and use of the wildcard character:

.. code-block:: python

    import dask.dataframe as dd
    df = dd.read_hdf('my86.h5', key='/*')
    
Learn more about ``dask.dataframe`` methods by visiting the :doc:`API documentation<../dataframe-api>`.

Create dask DataFrame from multiple HDF5 files
--------------------------------------------------    

The next example is a natural progression from the previous example (e.g. using a wildcard). Add a wildcard for the `key` and `path` parameters to read multiple files and multiple keys:

.. code-block:: python

    import dask.dataframe as dd
    df = dd.read_hdf('./*.h5', key='/*')
    
These exercises cover the basics of using ``dask.dataframe`` to work with HDF5 data.  For more information on the user functions to manipulate and explore dataframes (visualize, describe, compute, etc.) see :doc:`API documentation<../dataframe-api>`.  To explore the other data formats supported by ``dask.dataframe``, visit the :doc:`section on creating dataframes<../dataframe-create>` .
