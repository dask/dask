Quickstart
==========

**Dask makes it easy to parallelize your code**. Dask can help you in the following cases:

- Speed up computations that are currently just using one of your cores.
- Compute on data that doesn't fit in memory, but does fit on disk.
- Perform computations in a distributed cluster of computers.

**Install:**

Dask is easy installable through conda and pip:

.. code-block:: bash

   conda install dask
   #or
   pip install dask

**Collections:**

There are three default collections, or interfaces, to dask: ``dask.array``, ``dask.dataframe`` and ``dask.bag``. Users coming from the PyData and SciPy ecosystem are probably already familiar with ``numpy.array`` and ``pandas.Dataframe``. Their dask collections equivalents provide a very similar interface while allowing parallel computations. The third one, ``dask.bag``, is primarly used when dealing with semi-structured data, like JSON blobs or log files.

**dask.array:**

If you are familiar with ``numpy.array``, you'll notice that ``dask.array`` is very similar. When creating an array with dask, one of the differences with numpy is that you'll to specify the block size for your chunks. For more information on chunks read the FAQ_: *In ``dask.array`` what is ``chunks``?* and *How do I select a good value for ``chunks``?*.

.. code-block:: python

    >>> import dask.array as da
    >>> import numpy as np

    # Create a numpy array of ones
    >>> np_ones = np.ones((5000, 1000))

    # Create a dask array of ones
    >>> da_ones = da.ones((5000, 1000), chunks=(1000, 1000))


You can interact with a ``dask.array`` similarly to a ``numpy.array``:

.. code-block:: python

    # Interact with a numpy array
    >>> np_y = np.log(np_ones + 1)[:5].sum(axis=1)

    # Interact with a dask array
    >>> da_y = da.log(da_ones + 1)[:5].sum(axis=1)

If the result fits in memory, you can return it as a ``numpy.array``:

.. code-block:: python

    # Return the result in a numpy.array (fits in memory)
    >>> np_da_y = np.array(da_y)
    # or
    >>> da_y.compute()

If it doesn't, you can store the result to a file:

.. code-block:: python

    # Store the result to disk
    >>> import h5py

    >>> da_y.to_hdf5('myfile.hdf5', 'result')

You can also create arrays from files:

.. code-block:: python

    # Read results from file
    >>> f = h5py.File('myfile.hdf5')
    >>> dset = f['/result']
    >>> da_result = da.from_array(dset, chunks=(1000, 1000))


For more information on ``dask.array`` visit the array_ section. The reference guide in the API_ section provides a detailed description of all the available methods and their arguments.

.. _FAQ: faq.rst
.. _array: array-overview.rst
.. _api: array-api.rst


**dask.dataframe:**

Equivalently, a ``dask.dataframe`` is similar to a ``pandas.Dataframe`` with some slight alterations due to the parallel nature of dask.

.. code-block:: python

    >>> import pandas as pd
    >>> import dask.dataframe as dd
    
    >>> df = pd.read_csv('iris.csv')
    >>> da_df = dd.read_csv('iris.csv')

You can then interact with the dataframe in a similar manner.

.. code-block:: python

    # In pandas
    >>> df.head()

    # In dask
    >>> ddf.head()

With all dask collections (e.g. Array, Bag, DataFrame) one triggers computation by calling the ``.compute()`` method.

.. code-block:: python

    # In pandas
    >>> max_sepal_length_setosa = df[df.species == 'setosa'].sepal_length.max()
    >>> max_sepal_length_setosa
    5.7999999999999998

    # In dask
    >>> d_max_sepal_length_setosa = ddf[ddf.species == 'setosa'].sepal_length.max()
    >>> d_max_sepal_length_setosa.compute()
    5.7999999999999998


The ``pandas.DataFrame`` API is huge and dask currently doesn't support it entirely, but implements a subset in ``dask.dataframe``. For a list of the current supported API, take a look at the *What definetely works* section_. The reference guide for the dask.dataframe API provides a detailed description of all the available methods and their arguments.

.. _section: dataframe.rst
.. _API: dataframe-api.rst


**dask.bag:**

If you are working with semi-structure data, like JSON blobs or log files, ``dask.bag`` will provide a nice interface to perform parallel computations on it.

You can create a bag from a Python sequence (``db.from_sequence``), from lines in many files (``db.from_filenames``) and from HDFS files (``db.from_hdfs``).

.. code-block:: python

    >>> import dask.bag as db
    >>> import json

    # Get tweets as a dask.bag from compressed json files
    >>> b = db.from_filenames('*.json.gz').map(json.loads)

    # Take two items in dask.bag
    >>> b.take(2)

    # Count the frequencies of user locations
    >>> freq = b.pluck('user').pluck('location').frequencies()

    # Transform to a dask.dataframe
    >>> df = freq.to_dataframe()

For more information on ``dask.bag``, visit the Bag_ section, where you'll also finde the full API reference guide.

.. _bag: bag.rst


**dask.distributed:**

If you'd like to run your computations with a distributed cluster, instead of a single workstation, you can use ``dask.distributed``. There are many possible options for configuring your cluster. A good full example on how to use dask.distributed with Anaconda Cluster can be found in this blogpost_.

Once you have a cluster setup, you'll need to set up the ``dask.distributed.Client`` and pass it to ``compute()``. The following is taken from the blogpost.

.. code-block:: python

    >>> import dask
    >>> from dask.distributed import Client
    
    # client connected to 50 nodes, 2 workers per node.
    >>> dc = Client('tcp://localhost:9000') 

    >>> b = db.from_s3('githubarchive-data', '2015-*.json.gz').map(json.loads)

    # use single node scheduler
    >>> top_commits.compute()

    # use client with distributed cluster              
    >>> top_commits.compute(get=dc.get)
    [(u'mirror-updates', 1463019),
     (u'KenanSulayman', 235300),
     (u'greatfirebot', 167558),
     (u'rydnr', 133323),
     (u'markkcc', 127625)]

.. _blogpost: http://continuum.io/blog/dask-distributed-cluster

**Final remarks**

This 5-min quickstart guide is aimed at dask **users** and just provides a very general introduction. Dask is more than a set of collections. Under the hood, operations on those collections are represented as a task graph and dask's schedulers know how to execute them. More advanced users might be interested in writing their own custom_ graphs, understading in more detail the shared memory scheduler_ or the distribuded_ one, and profile the execution of the graph through dask's diagnostics_ utilities.


.. _custom: custom-graphs.rst
.. _scheduler: shared.rst
.. _distributed: distributed.rst
.. _diagnostics: diagnostics.rst

