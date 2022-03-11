Docker Images
=============

Example docker images are maintained at https://github.com/dask/dask-docker .

Each image installs the full Dask conda environment (including the distributed
scheduler), Numpy, and Pandas on top of a Miniconda installation on top of
a Debian image.

These images are large, around 1GB.

-   ``ghcr.io/dask/dask``: This a normal debian + miniconda image with the full Dask
    conda package (including the distributed scheduler), Numpy, and Pandas.
    This image is about 1GB in size.

-   ``ghcr.io/dask/dask-notebook``: This is based on the
    `Jupyter base-notebook image <https://hub.docker.com/r/jupyter/base-notebook/>`_
    and so it is suitable for use both normally as a Jupyter server, and also as
    part of a JupyterHub deployment.  It also includes a matching Dask software
    environment described above.  This image is about 2GB in size.

Example
-------

Here is a simple example on a dedicated virtual network

.. code-block:: bash

   docker network create dask

   docker run --network dask -p 8787:8787 --name scheduler ghcr.io/dask/dask dask-scheduler  # start scheduler

   docker run --network dask ghcr.io/dask/dask dask-worker scheduler:8786 # start worker
   docker run --network dask ghcr.io/dask/dask dask-worker scheduler:8786 # start worker
   docker run --network dask ghcr.io/dask/dask dask-worker scheduler:8786 # start worker

   docker run --network dask -p 8888:8888 ghcr.io/dask/dask-notebook  # start Jupyter server

Then from within the notebook environment you can connect to the Dask cluster like this:

.. code-block:: python

   from dask.distributed import Client
   client = Client("scheduler:8786")
   client

Extensibility
-------------

Users can mildly customize the software environment by populating the
environment variables ``EXTRA_APT_PACKAGES``, ``EXTRA_CONDA_PACKAGES``, and
``EXTRA_PIP_PACKAGES``.  If these environment variables are set in the container,
they will trigger calls to the following respectively::

   apt-get install $EXTRA_APT_PACKAGES
   conda install $EXTRA_CONDA_PACKAGES
   python -m pip install $EXTRA_PIP_PACKAGES

For example, the following ``conda`` installs the ``joblib`` package into
the Dask worker software environment:

.. code-block:: bash

   docker run --network dask -e EXTRA_CONDA_PACKAGES="joblib" ghcr.io/dask/dask dask-worker scheduler:8786

Note that using these can significantly delay the container from starting,
especially when using ``apt``, or ``conda`` (``pip`` is relatively fast).

Remember that it is important for software versions to match between Dask
workers and Dask clients.  As a result, it is often useful to include the same
extra packages in both Jupyter and Worker images.

Source
------

Docker files are maintained at https://github.com/dask/dask-docker.
This repository also includes a docker-compose configuration.
