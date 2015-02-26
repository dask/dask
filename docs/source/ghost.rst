Overlapping Blocks with Ghost Cells
===================================

Some array operations require communication of borders between neighboring
blocks.  Example operations include the following:

*  Convolve a filter across an image
*  Rolling sum/mean/max, ...
*  Search for image motifs like a Gaussian blob that might span the border of a
   block
*  Evalute a partial derivative
*  Play the game of Life_

Dask.array supports these operations by creating a new dask.array where each
block is slightly expanded by the borders of its neighbors.  This costs an
excess copy and the communication of many small chunks but allows localized
functions to evaluate in an embarrassing manner.  We call this process
*ghosting*.

Ghosting
--------

Consider two neighboring blocks in a dask array.

.. image:: images/unghosted-neighbors.png
   :width: 30%
   :alt: un-ghosted neighbors

We extend each block by trading thin nearby slices betweeen arrays

.. image:: images/ghosted-neighbors.png
   :width: 30%
   :alt: ghosted neighbors

We do this in all directions, including also diagonal interactions with the
ghost function:

.. image:: images/ghosted-blocks.png
   :width: 40%
   :alt: ghosted blocks

.. code-block:: python

   >>> import dask.array as da
   >>> x = da.ones((25, 25), blockshape=(5, 5))
   >>> x.blockdims
   ((5, 5, 5, 5, 5), (5, 5, 5, 5, 5))

   >>> g = da.ghost.ghost(g, axes={0: 2, 1: 2})
   >>> g.blockdims
   ((7, 9, 9, 9, 7), (7, 9, 9, 9, 7))


Map a function across blocks
----------------------------

We can now map a function across each of these larger blocks.  That function
can use the additional information from the neighbors that is stored locally in
each block

.. code-block:: python

   >>> from scipy.ndimage.filters import gaussian_filter
   >>> def func(block):
   ...    return gaussian_filter(block, sigma=2)

   >>> filt = g.map_blocks(func)

While we used a scipy function above this could have been any arbitrary
function.  This is a good interaction point with Numba_.

If your function does not preserve the shape of the block then you will need to
provide either a ``blockshape`` if your block sizes are regular or
``blockdims`` keyword argument if your block sizes are irregular

.. code-block:: python

   >>> g.map_blocks(myfunc, blockshape=(5, 5))

If your function needs to know the location of the block on which it operates
you can give your function a keyword argument ``block_id``

.. code-block:: python

   def func(block, block_id=None):
       ...

This extra keyword argument will be given a tuple that provides the block
location like ``(0, 0)`` for the upper right block or ``(0, 1)`` for the block
just to the right of that block.


Trim Excess
-----------

After mapping a blocked function you may want to trim off the borders from each
block by the same amount by which it was expanded.  The function
``trim_internal`` is useful here and takes the same ``axes`` keyword argument
given to ``ghost``.

.. code-block:: python

   >>> x.blockdims
   ((10, 10, 10, 10), (10, 10, 10, 10))

   >>> da.ghost.trim_internal(x, axes={0: 2, 1: 2})
   ((6, 6, 6, 6), (6, 6, 6, 6))


*Note: at the moment ``trim`` cuts indiscriminately from the boundaries as
well.  This does not match ``ghost`` and may not be desired.*

Boundaries
----------

Before ghosting you may want to extend your array in order to provide a
buffer on the boundary.  The following functions may be useful here:

*  ``periodic(x, axis=0, depth=2)`` - Pad the left and right borders with the
   right and left slices of depth 2
*  ``constant(x, axis=1, depth=2, value=-1)``  Pad the top and bottom borders
   with the value -1

Alternatively you can use functions like ``da.fromfunction`` and
``da.concatenate`` to pad arbitrarily.


.. _Life: http://en.wikipedia.org/wiki/Conway%27s_Game_of_Life
.. _Numba: http://numba.pydata.org/
