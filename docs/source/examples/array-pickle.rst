Loading pickle files
====================

As an example, we might load a grid of pickle files known to contain 1000 by
1000 NumPy arrays.

.. code-block:: python

   def load(fn):
       with open(fn) as f:
           result = pickle.load(f)
        return result

   dsk = {('x', 0, 0): (load, 'block-0-0.pkl'),
          ('x', 0, 1): (load, 'block-0-1.pkl'),
          ('x', 0, 2): (load, 'block-0-2.pkl'),
          ('x', 1, 0): (load, 'block-1-0.pkl'),
          ('x', 1, 1): (load, 'block-1-1.pkl'),
          ('x', 1, 2): (load, 'block-1-2.pkl')}

    chunks = ((1000, 1000), (1000, 1000, 1000))

    x = da.Array(dsk, 'x', chunks)
