Dataframes from HDF5 files
=========================

This section provides working examples of ``dask.dataframe`` methods to read HDF5 files. HDF5 is a unique technology suite that makes possible the management of extremely large and complex data collections. To learn more about HDF5, visit the `HDF Group Tutorial page <https://www.hdfgroup.org/HDF5/whatishdf5.html>`_.  For an overview of ``dask.dataframe``, its limitations, scope, and use, see the `DataFrame Overview section <http://dask.pydata.org/en/latest/dataframe-overview.html>`_.  

**Core DataFrame Functions in this Tutorial:**
----------------------------------------------

    *  Use ``dask.dataframe`` to:
        
        1.  Create dask DataFrame by loading a specific dataset (key) from a single HDF5 file
        2.  Read all datasets from a single HDF5 into a dask DataFrame 
        3.  Create a dask DataFrame by loading multiple HDF5 files with different datasets (keys)


**Generate Synthetic HDF5 files:**
--------------------------------------------

These exercises require multiple HDF5 files with multiple dataset keys.  The `Python pandas library <http://pandas.pydata.org/pandas-docs/stable/>`_ has a convenient `I/O API to create an HDF5 file from a dataframe <http://pandas.pydata.org/pandas-docs/stable/io.html#io-hdf5>`_. Combining this method with the `Python numpy random integer generator <http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.randint.html>`_ and several standard library modules provide all the tools necessary to create a synthetic dataset.

.. code-block:: python
   :emphasize-lines: 40,45
   
    
    def create_hdf5s(iters):
           
       import random
       import pandas as pd
       import numpy as np
       import string 
       import json
    
       # Intitalize counter to build specific number of hdf5 files
       count = 0
       setseed=1
       np.random.seed(seed=setseed)
       random.seed(setseed)
    
       # Key to store group keys and file names for generated hdf5 files
       fileKeys = {}
    
       while count <= iters:
            
           # randomly pick letter as dataset key
           groupkey = random.choice(list(string.ascii_lowercase))
        
           # randomly pick a number as hdf5 filename
           saver = 'my'+str(np.random.randint(100))+'.h5'
        
           # Make a dataframe; 26 rows, 2 columns
           df = pd.DataFrame(
           {'x': np.random.randint(1,1000,26),
           'y': np.random.randint(1,1000,26)},
           index=list(string.ascii_lowercase)
           )
        
           # Write synthetic hdf5 to current directory
           df.to_hdf(saver, key='/'+groupkey, format='table')
           fileKeys[saver]=groupkey
           count += 1
           setseed+=1
        
       # Return the dictionary with file key and filename for testing
       with open('hdf5Keys.json','w') as f:
           f.write(json.dumps(fileKeys))

    return fileKeys

    # Create the HDF5 files and keys to access
    hdf5Keys = create_hdf5s(keys)


.. doctest:: 
    
    def create_hdf5s(iters):
        import random
        import pandas as pd
        import numpy as np
        import string 
        import json

        # Intitalize counter to build specific number of hdf5 files
        count = 0

        # Key to store group keys and file names for generated hdf5 files
        fileKeys = {}

        while count <= iters:
            # randomly pick letter as dataset key
            groupkey = random.choice(list(string.ascii_lowercase))

            # randomly pick a number as hdf5 filename
            saver = 'my'+str(np.random.randint(100))+'.h5'

            # Make a dataframe; 26 rows, 2 columns
            df = pd.DataFrame(
            {'x': np.random.randint(1,1000,26),
            'y': np.random.randint(1,1000,26)},
            index=list(string.ascii_lowercase)
            )

            # Write synthetic hdf5 to current directory
            df.to_hdf(saver, key='/'+groupkey, format='table')

            fileKeys[saver]=groupkey

            count += 1
        # Return the dictionary with file key and filename for testing
        with open('hdf5Keys.json','w') as f:
            f.write(json.dumps(hdf5Keys))
        return fileKeys
    
    # Build the exercise data
    hdf5Keys = create_hdf5s(keys)
        
**Read single dataset from HDF5:**
--------------------------------------------

The first order of ``data.dataframe`` business is creating a dask DataFrame using a single HDF5 file's dataset.  The code to accomplish this task is::

    import dask.dataframe as dd
    df = dd.read_hdf('my10.h5','/u')
    df['x'].compute() 
    
    
**Load multiple datasets from single HDF5 file:**
-------------------------------------------- 

Loading multiple datasets from a single file requires a small tweak and use of the wildcard charater.  After reading the HDF5 file in, use the ``dask.dataframe.visualize`` method to `render the computation of this dataframe object’s task graph using graphviz <http://dask.pydata.org/en/latest/dataframe-api.html?highlight=visualize#dask.dataframe.DataFrame.visualize>`_ ::

    import dask.dataframe as dd
    df = dd.read_hdf('my83.h5','/*')
    df.visualize(format='png')

Learn more about user functions by visiting the `API documentation <http://dask.pydata.org/en/latest/dataframe-api.html>`_ .

**Create dask DataFrame from multiple HDF5 files:**
--------------------------------------------    

The next example is a natural progression from the previous example (e.g. using wildcard). In this case, HDF5 filenames are serialized (e.g. "my1.h5","my2.h5","my3.h5",etc).   Adding a wildcard to the file name for the serialized numbers takes care of this task::

    import dask.dataframe as dd
    df = dd.read_hdf('my*.h5','/*')
    df.describe().compute()

These exercises cover the basics of using ``dask.dataframe`` to work with HDF5 data.  For more information on the user functions to manipulate and explore within the dataframe (visualize, describe, compute, etc.) see `dask API documentation <http://dask.pydata.org/en/latest/dataframe-api.html>`_.  To explore the other data formats supported by ``dask.dataframe``, visit the `Create DataFrame <http://dask.pydata.org/en/latest/dataframe-create.html>`_ section.