Install Dask
============

You can install dask with ``conda``, with ``pip``, or by installing from source.

Conda
-----

To install Dask with `conda <https://www.continuum.io/downloads>`_::

    conda install dask

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

Source
------

To install dask from source, clone the repository from `github
<https://github.com/dask/dask>`_::

    git clone https://github.com/dask/dask.git
    cd dask
    python setup.py install
