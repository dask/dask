beginning ideas for dask.array quickstart:

Because dask.array is a Python library, rather than a graphical program or command 
line program, to test that dask.array is working you can run a small Python program. 

In this sample test, install dask.array, run python, and then run a brief set of 
commands to display an array of random numbers:

.. code::

   python

.. code::

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
