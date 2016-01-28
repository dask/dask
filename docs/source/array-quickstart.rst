Array Quickstart
=================

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

For more information on how to create Dask Arrays see: :doc:`array-creation`.

Basics
------

Working with Dask Arrays is straightforward since they inherit most of the
functionalities from Numpy's ndarrays. The one special thing about a
``dask.array`` is that you have to call ``compute()`` to trigger the
computation process.

A Dask Array is a :doc:`Dask Graph </spec>` with a special structure. You can
do operations with Dask Arrays, but the result of calling ``compute`` is not a
Dask Array. In most cases it will return a Numpy ndarray or a numeric type.
A consequence is that even tough the data involved in the computations
can be larger than memory, you have to ensure that your result will fit in
memory.

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

Fancy indexing and :doc:`slicing` are also supported. 
