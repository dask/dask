Dask Installation
=================

.. meta::
    :description: Dask Installation | You can install Dask with conda, pip install Dask, or install from source.

.. _Anaconda distribution: https://www.anaconda.com/download/

Dask currently supports Linux, macOS, and Windows. See the :doc:`changelog`
for comprehensive release notes for each Dask version.

How to Install Dask
-------------------

Dask installation can happen in a few different ways. You can install Dask with ``conda``, with ``pip``, or install from source.

Conda
-----

If you use the `Anaconda distribution`_, Dask installation will occur by default.
You can also install or upgrade Dask using the
`conda install <https://docs.conda.io/projects/conda/en/latest/commands/install.html>`_ command::

   conda install dask

This installs Dask and **all** common dependencies, including pandas and NumPy.
Dask packages are maintained both on the default channel and on `conda-forge <https://conda-forge.github.io/>`_.
You can select the channel with the ``-c`` flag::

    conda install dask -c conda-forge

Optionally, you can obtain a minimal Dask installation using the following command::

   conda install dask-core

This will install a minimal set of dependencies required to run Dask similar to (but not exactly the same as) ``python -m pip install dask`` below.

Pip
---

You can use pip to install everything required for most common uses of Dask
(e.g. Dask Array, Dask DataFrame, etc.).
This installs both Dask and dependencies, like NumPy and pandas,
that are necessary for different workloads. This is often the right
choice for Dask users::

   python -m pip install "dask[complete]"    # Install everything

You can also install only the Dask library.  Modules like ``dask.array``,
``dask.dataframe``, or ``dask.distributed`` won't work until you also install NumPy,
pandas, or Tornado, respectively.  This is common for downstream library
maintainers::

   python -m pip install dask                # Install only core parts of dask

We also maintain other dependency sets for different subsets of functionality::

   python -m pip install "dask[array]"       # Install requirements for dask array
   python -m pip install "dask[dataframe]"   # Install requirements for dask dataframe
   python -m pip install "dask[diagnostics]" # Install requirements for dask diagnostics
   python -m pip install "dask[distributed]" # Install requirements for distributed dask

We have these options so that users of the lightweight core Dask scheduler
aren't required to download the more exotic dependencies of the collections
(Numpy, pandas, Tornado, etc.).

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

Or do a developer install by using the ``-e`` flag
(see the :ref:`Install section <develop-install>` in the Development Guidelines)::

    python -m pip install -e .

Anaconda
--------

Dask is included by default in the `Anaconda distribution`_.

Optional dependencies
---------------------

Specific functionality in Dask may require additional optional dependencies.
For example, reading from Amazon S3 requires `s3fs <https://s3fs.readthedocs.io/en/latest/>`_.
These optional dependencies and their minimum supported versions are listed below.

+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| Dependency      | Version     | Description                                                                                            |
+=================+=============+========================================================================================================+
| bokeh           | >=2.4.2     | Visualizing dask diagnostics                                                                           |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| cachey          | >=0.1.1     | Use caching for computation                                                                            |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| cityhash        | >=0.2.4     | Faster hashing of arrays                                                                               |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| crick           | >=0.0.3     | Use ``tdigest`` internal method for percentile computation                                             |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| cytoolz         | >=0.10.1    | Faster cythonized implementation of internal iterators, functions, and dictionaries                    |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| dask-ml         | >=1.4.0     | Common machine learning functions scaled with Dask                                                     |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| fastavro        | >=0.22.6    | Storing and reading data from Apache Avro files                                                        |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| fastparquet     | >=0.8.2     | Storing and reading data from Apache Parquet files                                                     |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| gcsfs           | >=0.4.0     | Storing and reading data located in Google Cloud Storage                                               |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| graphviz        | >=0.8.4     | Graph visualization using the graphviz engine                                                          |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| h5py            | >=2.10.0    | Storing array data in hdf5 files                                                                       |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| ipycytoscape    | >=1.0.1     | Graph visualization using the cytoscape engine                                                         |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| IPython         | >=7.9.0     | Write graph visualizations made with graphviz engine to file                                           |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| jinja2          | >=2.10.3    | HTML representations of Dask objects in Jupyter notebooks                                              |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| lz4             | >=4.3.2     | Transparent use of lz4 compression algorithm?                                                          |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| matplotlib      | >=3.1.1     | Color map support for graph visualization                                                              |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| mimesis         | >=5.3.0     | Random bag data generation with ``dask.datasets.make_people()``                                        |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| mmh3            | >=2.4       | Use of murmurhash algorithm for array hashing                                                          |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| numpy           | >=1.21      | Required for dask.array                                                                                |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| pandas          | >=1.3       | Required for dask.dataframe                                                                            |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| psutil          | >=5.7.0     | Factor CPU affinity into CPU count, intelligently infer blocksize for ``dask.dataframe.read_pandas()`` |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| pyarrow         | >=7.0       | Support for Apache Arrow datatypes & engine when storing/reading Apache ORC or Parquet files           |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| python-snappy   | >=0.5.4     | Snappy compression to bs used when storing/reading Avro or Parquet files                               |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| s3fs            | >=2021.9.0  | Storing and reading data located in Amazon S3                                                          |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| scipy           | >=1.3.2     | Required for ``dask.array.stats``, ``dask.array.fft``, ``dask.array.linalg.lu()``                      |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| sparse          | >=0.12.0    | Use sparse arrays as backend for dask arrays                                                           |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| sqlalchemy      | >=1.4.16,<2 | Writing and reading from SQL databases                                                                 |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| tblib           | >=1.6.0     | Serialization of worker traceback objects                                                              |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| tiledb-py       | >=0.8.1     | Storing and reading data from TileDB files                                                             |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| xxhash          | >=1.4.1     | Faster hashing of arrays                                                                               |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+
| zarr            | >=2.12.0    | Storing and reading data from Zarr files                                                               |
+-----------------+-------------+--------------------------------------------------------------------------------------------------------+

Test
----

Test Dask with ``py.test``::

    cd dask
    py.test dask

Installing Dask naively may not install all requirements by default (see the `Pip`_ section above).
You may choose to install the ``dask[complete]`` version which includes
all dependencies for all collections::

    pip install "dask[complete]"

Alternatively, you may choose to test
only certain submodules depending on the libraries within your environment.
For example, to test only Dask core and Dask array we would run tests as
follows::

    py.test dask/tests dask/array/tests

See the :ref:`section on testing <develop-test>` in the Development Guidelines for more details.

.. _Anaconda distribution: https://www.anaconda.com/download/
