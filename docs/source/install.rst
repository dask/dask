Install Dask
============

You can install dask with ``conda``, with ``pip``, or by installing from source.

Conda
-----

Dask is installed by default in `Anaconda <https://www.anaconda.com/download/>`_.
You can update Dask using the `conda <https://www.anaconda.com/download/>`_ command::

   conda install dask

This installs Dask and **all** common dependencies, including Pandas and NumPy.
Dask packages are maintained both on the default channel and on `conda-forge <https://conda-forge.github.io/>`_.
Optionally, you can obtain a minimal Dask installation using the following command::

   conda install dask-core

This will install a minimal set of dependencies required to run Dask similar to (but not exactly the same as) ``python -m pip install dask`` below.

Pip
---

You can install everything required for most common uses of Dask (arrays,
dataframes, ...)  This installs both Dask and dependencies like NumPy, Pandas,
and so on that are necessary for different workloads.  This is often the right
choice for Dask users::

   python -m pip install "dask[complete]"    # Install everything

You can also install only the Dask library.  Modules like ``dask.array``,
``dask.dataframe``, or ``dask.distributed`` won't work until you also install NumPy,
Pandas, or Tornado, respectively.  This is common for downstream library
maintainers::

   python -m pip install dask                # Install only core parts of dask

We also maintain other dependency sets for different subsets of functionality::

   python -m pip install "dask[array]"       # Install requirements for dask array
   python -m pip install "dask[dataframe]"   # Install requirements for dask dataframe
   python -m pip install "dask[diagnostics]" # Install requirements for dask diagnostics
   python -m pip install "dask[distributed]" # Install requirements for distributed dask

We have these options so that users of the lightweight core Dask scheduler
aren't required to download the more exotic dependencies of the collections
(Numpy, Pandas, Tornado, etc.).


Install from Source
-------------------

To install Dask from source, clone the repository from `github
<https://github.com/dask/dask>`_::

    git clone https://github.com/dask/dask.git
    cd dask
    python -m pip install .

You can also install all dependencies as well::

    python -m pip install ".[complete]"

You can view the list of all dependencies within the ``extras_require`` field
of ``setup.py``.


Or do a developer install by using the ``-e`` flag::

    python -m pip install -e .

Anaconda
--------

Dask is included by default in the `Anaconda distribution <https://www.anaconda.com/download>`_.

Optional dependencies
---------------------

Specific functionality in Dask may require additional optional dependencies.
For example, reading from Amazon S3 requires ``s3fs``.
These optional dependencies and their minimum supported versions are listed below.

+---------------+----------+--------------------------------------------------------------+
| Dependency    | Version  |                          Description                         |
+===============+==========+==============================================================+
|     bokeh     | >=2.4.2  |                Visualizing dask diagnostics                  |
+---------------+----------+--------------------------------------------------------------+
|   cityhash    |          |                  Faster hashing of arrays                    |
+---------------+----------+--------------------------------------------------------------+
|  distributed  | >=2.0    |               Distributed computing in Python                |
+---------------+----------+--------------------------------------------------------------+
|  fastparquet  |          |         Storing and reading data from parquet files          |
+---------------+----------+--------------------------------------------------------------+
|     gcsfs     | >=0.4.0  |        File-system interface to Google Cloud Storage         |
+---------------+----------+--------------------------------------------------------------+
|   murmurhash  |          |                   Faster hashing of arrays                   |
+---------------+----------+--------------------------------------------------------------+
|     numpy     | >=1.18   |                   Required for dask.array                    |
+---------------+----------+--------------------------------------------------------------+
|     pandas    | >=1.0    |                  Required for dask.dataframe                 |
+---------------+----------+--------------------------------------------------------------+
|     psutil    |          |             Enables a more accurate CPU count                |
+---------------+----------+--------------------------------------------------------------+
|     pyarrow   | >=1.0    |               Python library for Apache Arrow                |
+---------------+----------+--------------------------------------------------------------+
|     s3fs      | >=0.4.0  |                    Reading from Amazon S3                    |
+---------------+----------+--------------------------------------------------------------+
|     scipy     |          |                  Required for dask.array.stats               |
+---------------+----------+--------------------------------------------------------------+
|   sqlalchemy  |          |            Writing and reading from SQL databases            |
+---------------+----------+--------------------------------------------------------------+
|    cytoolz*   | >=0.8.2  | Utility functions for iterators, functions, and dictionaries |
+---------------+----------+--------------------------------------------------------------+
|    xxhash     |          |                  Faster hashing of arrays                    |
+---------------+----------+--------------------------------------------------------------+

\* Note that ``toolz`` is a mandatory dependency but it can be transparently replaced with
``cytoolz``.


Test
----

Test Dask with ``py.test``::

    cd dask
    py.test dask

Please be aware that installing Dask naively may not install all
requirements by default. Please read the ``pip`` section above which discusses
requirements.  You may choose to install the ``dask[complete]`` version which includes
all dependencies for all collections.  Alternatively, you may choose to test
only certain submodules depending on the libraries within your environment.
For example, to test only Dask core and Dask array we would run tests as
follows::

    py.test dask/tests dask/array/tests
