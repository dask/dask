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
For example, reading from Amazon S3 requires `s3fs`_.
These optional dependencies and their minimum supported versions are listed below.

+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| Dependency       | Version         | Description                                                                                             |
+==================+=================+=========================================================================================================+
| `bokeh`_         | ``>=2.4.2``     | Generate profiles of Dask execution (required for ``dask.diagnostics``)                                 |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `cachey`_        | ``>=0.1.1``     | Use caching for computation                                                                             |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `cityhash`_      | ``>=0.2.4``     | Use CityHash and FarmHash hash functions for array hashing (~2x faster than MurmurHash)                 |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `crick`_         | ``>=0.0.3``     | Use ``tdigest`` internal method for dataframe statistics computation                                    |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `cytoolz`_       | ``>=0.10.1``    | Faster cythonized implementation of internal iterators, functions, and dictionaries                     |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `dask-ml`_       | ``>=1.4.0``     | Common machine learning functions scaled with Dask                                                      |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `fastavro`_      | ``>=0.22.6``    | Storing and reading data from Apache Avro files                                                         |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `fastparquet`_   | ``>=0.8.2``     | Storing and reading data from Apache Parquet files                                                      |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `gcsfs`_         | ``>=2021.9.0``  | Storing and reading data located in Google Cloud Storage                                                |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `graphviz`_      | ``>=0.8.4``     | Graph visualization using the graphviz engine                                                           |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `h5py`_          | ``>=2.10.0``    | Storing array data in hdf5 files                                                                        |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `ipycytoscape`_  | ``>=1.0.1``     | Graph visualization using the cytoscape engine                                                          |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `IPython`_       | ``>=7.9.0``     | Write graph visualizations made with graphviz engine to file                                            |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `jinja2`_        | ``>=2.10.3``    | HTML representations of Dask objects in Jupyter notebooks (required for ``dask.diagnostics``)           |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `lz4`_           | ``>=4.3.2``     | Transparent use of lz4 compression algorithm                                                            |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `matplotlib`_    | ``>=3.1.1``     | Color map support for graph visualization                                                               |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `mimesis`_       | ``>=5.3.0``     | Random bag data generation with :func:`dask.datasets.make_people`                                       |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `mmh3`_          | ``>=2.5.1``     | Use MurmurHash hash functions for array hashing (~8x faster than SHA1)                                  |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `numpy`_         | ``>=1.21``      | Required for ``dask.array``                                                                             |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `pandas`_        | ``>=1.3``       | Required for ``dask.dataframe``                                                                         |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `psutil`_        | ``>=5.7.0``     | Factor CPU affinity into CPU count, intelligently infer blocksize when reading CSV files                |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `pyarrow`_       | ``>=7.0``       | Support for Apache Arrow datatypes & engine when storing/reading Apache ORC or Parquet files            |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `python-snappy`_ | ``>=0.5.4``     | Snappy compression to bs used when storing/reading Avro or Parquet files                                |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `s3fs`_          | ``>=2021.9.0``  | Storing and reading data located in Amazon S3                                                           |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `scipy`_         | ``>=1.3.2``     | Required for ``dask.array.stats``, ``dask.array.fft``, and :func:`dask.array.linalg.lu`                 |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `sparse`_        | ``>=0.12.0``    | Use sparse arrays as backend for dask arrays                                                            |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `sqlalchemy`_    | ``>=1.4.16``    | Writing and reading from SQL databases                                                                  |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `tblib`_         | ``>=1.6.0``     | Serialization of worker traceback objects                                                               |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `tiledb`_        | ``>=0.8.1``     | Storing and reading data from TileDB files                                                              |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `xxhash`_        | ``>=1.4.1``     | Use xxHash hash functions for array hashing (~2x faster than MurmurHash, slightly slower than CityHash) |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+
| `zarr`_          | ``>=2.12.0``    | Storing and reading data from Zarr files                                                                |
+------------------+-----------------+---------------------------------------------------------------------------------------------------------+

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
.. _s3fs: https://s3fs.readthedocs.io/en/latest/
.. _bokeh: https://bokeh.org/
.. _cachey: https://github.com/dask/cachey
.. _cityhash: https://github.com/escherba/python-cityhash
.. _crick: https://github.com/dask/crick
.. _cytoolz: https://github.com/pytoolz/cytoolz
.. _dask-ml: https://ml.dask.org/
.. _fastavro: https://fastavro.readthedocs.io/en/latest/
.. _fastparquet: https://github.com/dask/fastparquet
.. _graphviz: https://graphviz.readthedocs.io/en/stable/
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _h5py: https://www.h5py.org/
.. _ipycytoscape: https://ipycytoscape.readthedocs.io/en/master/index.html
.. _IPython: https://ipython.org/
.. _jinja2: https://jinja.palletsprojects.com/
.. _lz4: https://python-lz4.readthedocs.io/en/stable/index.html
.. _matplotlib: https://matplotlib.org/
.. _mimesis: https://mimesis.name/en/master/
.. _mmh3: https://github.com/hajimes/mmh3
.. _numpy: https://numpy.org/
.. _pandas: https://pandas.pydata.org/
.. _psutil: https://psutil.readthedocs.io/en/latest/
.. _pyarrow: https://arrow.apache.org/docs/python/index.html
.. _python-snappy: https://github.com/andrix/python-snappy
.. _scikit-image: https://scikit-image.org/
.. _scipy: https://scipy.org/
.. _sparse: https://sparse.pydata.org/en/stable/
.. _sqlalchemy: https://www.sqlalchemy.org/
.. _tblib: https://python-tblib.readthedocs.io/en/latest/readme.html
.. _tiledb: https://github.com/TileDB-Inc/TileDB-Py
.. _xxhash: https://github.com/ifduyue/python-xxhash
.. _zarr: https://zarr.readthedocs.io/en/stable/index.html
