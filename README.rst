|Build Status| |Coverage| |Doc Status| |Gitter| |Version Status|


Dask
====

*Dask is a flexible parallel computing library for analytic computing.*  

See documentation_ for more information.

|

.. image:: https://github.com/dask/dask/blob/master/docs/source/images/collections-schedulers.png
   :alt: Dask collections and schedulers
 

Use Cases
---------

* bigger-than-memory datasets
* single machine parallelism
* distributed cluster parallelism 
* on disk arrays
* directory of tabular files
* computations with complex dependencies


Sneak Peek
----------

A simple computation

.. code-block:: python

   def inc(i):
       return i + 1

   def add(a, b):
       return a + b

   x = 1
   y = inc(x)
   z = add(y, 10)
   

is representable like:


.. code-block:: python

   dsk = {'x': 1,
          'y': (inc, 'x'),
          'z': (add, 'y', 10)}
        

which happens to be a graph:

.. image:: https://github.com/dask/dask/blob/master/docs/source/_static/dask-simple.png
   :height: 400px
   :alt: A simple dask dictionary
   :align: right


where the computation of `z` is parallelizable


.. code-block:: python

   from dask import get
   
   z = get(dsk, 'z')


Do You want to handle more complex structures with a variaty of schedulers?


Collections and Schedulers
==========================

To handle more complex cases there are a variaty of abstractions and schedulers available.


Bag
---

Parallel python iterators, lists.

.. code-block:: python

   import dask.bag as db
   b = db.read_text('2015-*-*.json.gz').map(json.loads)
   b.pluck('name').frequencies().topk(10, lambda pair: pair[1]).compute()


Delayed
-------

Parallely executed functions.

.. code-block:: python

   from dask import delayed
   L = []
   for fn in filenames:                  # Use for loops to build up computation
       data = delayed(load)(fn)          # Delay execution of function
       L.append(delayed(process)(data))  # Build connections between variables

   result = delayed(summarize)(L)
   result.compute()


DataFrame
---------

Parallel pandas.

.. code-block:: python

    import dask.dataframe as dd
    df = dd.read_csv('2015-*-*.csv')
    df.groupby(df.user_id).value.mean().compute()


Array
-----

Parallel numpy

.. code-block:: python

   import dask.array as da
   f = h5py.File('myfile.hdf5')
   x = da.from_array(f['/big-data'], chunks=(1000, 1000))
   x - x.mean(axis=1).compute()


Distributed 
-----------

The **concurrent.futures** interface provides general submission of custom
tasks:

.. code-block:: python

   from dask.distributed import Client
   client = Client('scheduler:port')

   futures = []
   for fn in filenames:
       future = client.submit(load, fn)
       futures.append(future)

   summary = client.submit(summarize, futures)
   summary.result()
   

Install Dask
============

Conda
-----

To install the latest version of Dask from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.continuum.io/downloads>`_::

    conda install dask -c conda-forge

This installs dask and all common dependencies, including Pandas and NumPy.

Pip
---

To install Dask with ``pip`` there are a few options, depending on which
dependencies you would like to keep up to date:

*   ``pip install dask[complete]``: Install everything
*   ``pip install dask[array]``: Install dask and numpy
*   ``pip install dask[bag]``: Install dask and cloudpickle
*   ``pip install dask[dataframe]``: Install dask, numpy, and pandas
*   ``pip install dask``: Install only dask, which depends only on the standard
    library.  This is appropriate if you only want the task schedulers.


Documentation
-------------

See the comprehensive documentation_.


License
-------

New BSD. See `License File <https://github.com/dask/dask/blob/master/LICENSE.txt>`__.

.. _documentation: http://dask.pydata.org/en/latest/
.. |Build Status| image:: https://travis-ci.org/dask/dask.svg?branch=master
   :target: https://travis-ci.org/dask/dask
.. |Coverage| image:: https://coveralls.io/repos/dask/dask/badge.svg
   :target: https://coveralls.io/r/dask/dask
   :alt: Coverage status
.. |Doc Status| image:: http://readthedocs.org/projects/dask/badge/?version=latest
   :target: http://dask.pydata.org/en/latest/
   :alt: Documentation Status
.. |Gitter| image:: https://badges.gitter.im/Join%20Chat.svg
   :alt: Join the chat at https://gitter.im/dask/dask
   :target: https://gitter.im/dask/dask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge
.. |Version Status| image:: https://img.shields.io/pypi/v/dask.svg
   :target: https://pypi.python.org/pypi/dask/

