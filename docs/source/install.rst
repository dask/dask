Install Dask
============

Dask can be installed from source, or can be installed with a single command
with pip or with `conda <https://www.continuum.io/downloads>`_.

To install Dask with ``conda``::

    conda install dask

And to install Dask with ``pip``::

    pip install dask


Install parts of dask
---------------------

Different components of dask have different dependencies that are only relevant for that component.

* ``dask.array``: numpy
* ``dask.bag``: cloudpickle
* ``dask.dataframe``: pandas, bcolz (in development)

The base ``pip`` install of dask is fairly minimal.  This is to protect
lightweight ``dask.bag`` users from having to install heavyweight dependencies
like ``bcolz`` or ``pandas``.  You can either install these dependencies
separately or, when installing with ``pip``  you can specify which set of
dependencies you would like as a parameter::

   pip install dask[array]
   pip install dask[bag]
   pip install dask[dataframe]
   pip install dask[complete]
