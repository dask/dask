Dask Arrays Quickstart
======================

Dask Array implements a subset of the NumPy ndarray interface and can cut large 
arrays into many small pieces. This lets you work with arrays larger than 
memory and use all of your cores without having to learn a new syntax.

Installation Testing
--------------------

Because ``dask.array`` is a Python library, rather than a graphical program or
command line program, to test that dask.array is working you can use the Python 
interpreter. 

In this sample test, install ``dask.array``, run python, and then run a brief
set of commands to display an array of random numbers:

.. code::

   python

.. code:: Python

   import dask.array
   import numpy
   a=dask.array.random.normal(6,.1,size=(4,4),chunks=(2,2))
   numpy.array(a)
   quit()

If dask is installed correctly, this will print out a 4x4 array of random 
numbers close to 6, similar to:

.. code::

   >>>array([[ 6.11636086,  6.02888376,  6.02715362,  6.09789198],
             [ 6.17750019,  6.10241887,  6.0984278 ,  5.80852248],
             [ 6.00780242,  6.03833159,  6.0115587 ,  6.06790745],
             [ 5.9882688 ,  5.91056546,  5.9793473 ,  5.93219086]])

Array Creation
--------------
Dask Arrays can be created from functions that return NumPy-like objects.  NumPy
arrays themselves and other iterable objects like those from reading HDF5 and
NetCDF can be easily converted into Dask Arrays.

For example, if you create a HDF5 data file with some random numbers:

.. code-block:: Python

  import h5py
  import numpy as np

  m = np.random.random(size = (10000, 1000))

  with h5py.File('data.hdf5', 'w') as wf:
      g = wf.create_group('group1')
      g.create_dataset('dataset', data = m)

You can load the data into a Dask Array by means of the function
``from_array``:

.. code-block:: Python

  import h5py
  import dask.array as da

  f = h5py.File('data.hdf5', 'r')
  d = f['group1/dataset']
  dsk = da.from_array(d, chunks = (100, 100))

The ``chunks`` argument tells ``dask.array`` how to break up the underlying
array into chunks. For recommendations on how to choose chunk sizes see
:doc:`array-creation`.

You can also use ``from_array`` with netCDF datasets:

.. code-block:: Python

  from netCDF4 import Dataset
  import dask.array as da

  rootgp = Dataset('data.nc', 'r')
  nds = rootgp.variables['data']
  d = da.from_array(nds, chunks=(100, 100))
  rootgp.close()

There are more advanced methods to create a ``dask.array``, for example you can
:doc:`stack and concatenate </stack>` two existing arrays into a new one.


Basics
------

Working with Dask Arrays is straightforward since they inherit most of the
functionalities from Numpy's ndarrays. The one special thing about a
``dask.array`` is that you have to call ``compute()`` to trigger the computation
process.

The example below shows some of the features supported by Dask Arrays:

.. code-block:: Python

  import dask.array as da
  
  # Probability distributions
  a = da.random.normal(6, .1, size=(8,8), chunks=(2,2))
  
  # Element-wise and scalar arithmetic
  b = 3*a + a**2 - da.log(a)
  resb = b.compute()
  
  # Axes reordering
  c = a.transpose()
  
  # Matrix multiplication
  d = da.dot(a, b).compute()

Fancy indexing and :doc:`slicing` are also supported. For array operations that
require communication between adjacent blocks see :doc:`ghost`.
