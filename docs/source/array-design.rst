Internal Design
===============

.. image:: images/array.png
   :width: 40 %
   :align: right
   :alt: A dask array

Dask arrays define a large array with a grid of blocks of smaller arrays.
These arrays may be concrete, or functions that produce arrays.  We define a
dask array with from the following components

*  A dask with a special set of keys designating blocks
   e.g. ``('x', 0, 0), ('x', 0, 1), ...``
*  A sequence of block sizes along each dimension
   e.g. ``((5, 5, 5, 5), (8, 8, 8))``
*  A name to identify which keys in the dask refer to this array, e.g. ``'x'``

By special convention we refer to each block of the array with a tuple of the
form ``(name, i, j, k)`` for ``i, j, k`` being the indices of the block,
ranging from ``0`` to the number of blocks in that dimension.  The dask must
hold key-value pairs referring to these keys.  It likely also holds other
key-value pairs required to eventually compute the desired values, e.g.

.. code-block:: python

   {
    ('x', 0, 0): (add, 1, ('y', 0, 0)),
    ('x', 0, 1): (add, 1, ('y', 0, 1)),
    ...
    ('y', 0, 0): (getitem, dataset, (slice(0, 1000), slice(0, 1000))),
    ('y', 0, 1): (getitem, dataset, (slice(0, 1000), slice(1000, 2000)))
    ...
   }

We also store the size of each block along each axis.  This is a tuple of
tuples such that the length of the outer tuple is equal to the dimension and
the lengths of the inner tuples are equal to the number of blocks along each
dimension.  In the example illustrated above this value is as follows::

    blockdims = ((5, 5, 5, 5), (8, 8, 8))

Note that these numbers do not necessarily need to be regular.  We often create
regularly sized grids but blocks change shape after complex slicing.  Beware
that some operations do expect certain symmetries in the block-shapes.  For
example matrix multiplication requires that blocks on each side have
anti-symmetric shapes.

Create an Array Object
----------------------

So to create an ``da.Array`` object we need a dictionary with these special
keys ::

    dsk = {('x', 0, 0): ...}

a name specifying to which keys this array refers ::

    name = 'x'

and a blockdims tuple::

    blockdims = ((5, 5, 5, 5), (8, 8, 8))

Then one can construct an array::

    x = da.Array(dsk, name, blockdims=blockdims)

So ``dask.array`` operations update dask dictionaries and track blockdims
shapes.


Example - ``eye`` function
--------------------------

As an example lets build the ``np.eye`` function for ``dask.array`` to make the
identity matrix

.. code-block:: python

   names = ('eye-%d' % i for i in itertools.count(1))  # sequence of names

   def eye(n, blocksize):
       blockdims = ((blocksize,) * n // blocksize,
                    (blocksize,) * n // blocksize)

       name = next(names)

       dsk = {(name, i, j): np.eye(blocksize)
                            if i == j else
                            np.zeros((blocksize, blocksize))
                for i in range(n // blocksize)
                for j in range(n // blocksize)}

       return da.Array(dsk, name, blockdims=blockdims)
