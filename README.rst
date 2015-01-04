Dask
====

A minimal task scheduling abstraction.

See Dask documentation at http://dask.readthedocs.org

LICENSE
-------

New BSD. See `License File <https://github.com/ContinuumIO/dask/blob/master/LICENSE.txt>`__.

Install
-------

``dask`` is on the Python Package Index (PyPI):

::

    pip install dask

Example
-------

.. image:: docs/source/_static/dask-simple.png
   :height: 400px
   :alt: A simple dask dictionary
   :align: right


Consider the following simple program

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)

We encode this as a dictionary in the following way

.. code-block:: python

   d = {'x': 1,
        'y': (inc, 'x'),
        'z': (add, 'y', 10)}

Dependencies
------------

``dask.core`` supports Python 2.6+ and Python 3.2+ with a common codebase.  It
is pure Python and requires no dependencies beyond the standard library.

It is, in short, a light weight dependency.
