Dask
====

|Build Status| |Coverage| |Doc Status| |Gitter| |Version Status|


*Dask is a flexible parallel computing library for analytic computing.*

See documentation_ for more information.


Dask can scale to a cluster of 100s of machines. It is resilient, elastic, data
local, and low latency.


Dask is composed of two components:

1.  **Dynamic task scheduling** optimized for computation.  This is similar to
    *Airflow, Luigi, Celery, or Make*, but optimized for interactive
    computational workloads.
2.  **"Big Data" collections** like parallel arrays, dataframes, and lists that
    extend common interfaces like *NumPy, Pandas, or Python iterators* to
    larger-than-memory or distributed environments.  These parallel collections
    run on top of the dynamic task schedulers.

.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 80%
   :align: center

See the `dask.distributed documentation (separate website)
<https://distributed.readthedocs.io/en/latest/>`_ for more technical information
on Dask's distributed scheduler,


**Parallelized Lists**

.. code-block:: python

   import dask.bag as db
   b = db.read_text('2015-*-*.json.gz').map(json.loads)
   b.pluck('name').frequencies().topk(10, lambda pair: pair[1]).compute()


**Parallelized Functions**

.. code-block:: python

   from dask import delayed
   L = []
   for fn in filenames:                  # Use for loops to build up computation
       data = delayed(load)(fn)          # Delay execution of function
       L.append(delayed(process)(data))  # Build connections between variables

   result = delayed(summarize)(L)
   result.compute()


**Parallelized DataFrames** 

.. code-block:: python

    import dask.dataframe as dd
    df = dd.read_csv('2015-*-*.csv')
    df.groupby(df.user_id).value.mean().compute()

**Parallelized Arrays**

.. code-block:: python

   import dask.array as da
   f = h5py.File('myfile.hdf5')
   x = da.from_array(f['/big-data'], chunks=(1000, 1000))
   x - x.mean(axis=1).compute()


**Distributed among machines**

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
