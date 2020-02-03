Install Dask.Distributed
========================

You can install dask.distributed with ``conda``, with ``pip``, or by installing
from source.

Conda
-----

To install the latest version of dask.distributed from the
`conda-forge <https://conda-forge.github.io/>`_ repository using
`conda <https://www.anaconda.com/downloads>`_::

    conda install dask distributed -c conda-forge

Pip
---

Or install distributed with ``pip``::

    python -m pip install dask distributed --upgrade

Source
------

To install distributed from source, clone the repository from `github
<https://github.com/dask/distributed>`_::

    git clone https://github.com/dask/distributed.git
    cd distributed
    python setup.py install


Notes
-----

**Note for Macports users:** There `is a known issue
<https://trac.macports.org/ticket/50058>`_.  with Python from macports that
makes executables be placed in a location that is not available by default. A
simple solution is to extend the ``PATH`` environment variable to the location
where Python from macports install the binaries. For example, for Python 3.6::

    $ export PATH=/opt/local/Library/Frameworks/Python.framework/Versions/3.6/bin:$PATH
