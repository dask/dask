Install Dask
============

You can install dask with ``conda``, with ``pip``, or by installing from source.

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

We do this so that users of the lightweight core dask scheduler aren't required
to download the more exotic dependencies of the collections (numpy, pandas,
etc..)

Install from Source
-------------------

To install dask from source, clone the repository from `github
<https://github.com/dask/dask>`_::

    git clone https://github.com/dask/dask.git
    cd dask
    python setup.py install

or use ``pip`` locally if you want to install all dependencies as well::

    pip install -e .[complete]

You can view the list of all dependencies within the ``extras_require`` field
of ``setup.py``.


Test
----

Test dask with ``py.test``::

    cd dask
    py.test dask

Although please aware that installing dask naively may not install all
requirements by default.  Please read the ``pip`` section above that discusses
requirements.  You may choose to install the ``dask[complete]`` which includes
all dependencies for all collections.  Alternatively you may choose to test
only certain submodules depending on the libraries within your environment.
For example to test only dask core and dask array we would run tests as
follows::

    py.test dask/tests dask/array/tests
