Install Dask
============

You can install dask using ``conda``::

    conda install dask

You can install dask using ``pip``::

    pip install dask

Dask also ships with Anaconda so you may have it already.

Install parts of dask
---------------------

Different components of dask have different dependencies that are only relevant for that component.

* ``dask.array``: numpy
* ``dask.bag``: dill
* ``dask.dataframe``: pandas, bcolz (in development)
* ``dask.distributed``: pyzmq (in development)

The base ``pip`` install of dask is fairly minimal.  This is to protect
lightweight ``dask.bag`` users from having to install heavyweight dependencies
like ``bcolz`` or ``pandas``.  You can either install these dependencies
separately or, when installing with ``pip``  you can specify which set of
dependencies you would like as a parameter::

   pip install dask[array]
   pip install dask[bag]
   pip install dask[dataframe]
   pip install dask[complete]
