Docker Images
=============

Example docker images are maintained at https://github.com/dask/dask-docker
and https://hub.docker.com/r/daskdev/ .

Each image installs the full Dask conda package (including the distributed
scheduler), Numpy, and Pandas on top of a Miniconda installation on top of
a Debian image.

These images are large, around 1GB.

-   ``daskdev/dask``: This a normal debian + miniconda image with the full Dask
    conda package (including the distributed scheduler), Numpy, and Pandas.
    This image is about 1GB in size.

-   ``daskdev/dask-notebook``: This is based on the
    `Jupyter base-notebook image <https://hub.docker.com/r/jupyter/base-notebook/>`_
    and so it is suitable for use both normally as a Jupyter server, and also as
    part of a JupyterHub deployment.  It also includes a matching Dask software
    environment described above.  This image is about 2GB in size.

Example
-------

Here is a simple example on the local host network

.. code-block:: bash

   docker run -it --network host daskdev/dask dask-scheduler  # start scheduler

   docker run -it --network host daskdev/dask dask-worker localhost:8786 # start worker
   docker run -it --network host daskdev/dask dask-worker localhost:8786 # start worker
   docker run -it --network host daskdev/dask dask-worker localhost:8786 # start worker

   docker run -it --network host daskdev/dask-notebook  # start Jupyter server


Extensibility
-------------

Users can mildly customize the software environment by populating the
environment variables ``EXTRA_APT_PACKAGES``, ``EXTRA_CONDA_PACKAGES``, and
``EXTRA_PIP_PACKAGES``.  If these environment variables are set, they will
trigger calls to the following respectively::

   apt-get install $EXTRA_APT_PACKAGES
   conda install $EXTRA_CONDA_PACKAGES
   pip install $EXTRA_PIP_PACKAGES

Note that using these can significantly delay the container from starting,
especially when using ``apt``, or ``conda`` (``pip`` is relatively fast).

Remember that it is important for software versions to match between Dask
workers and Dask clients.  As a result, it is often useful to include the same
extra packages in both Jupyter and Worker images.

Source
------

Docker files are maintained at https://github.com/dask/dask-docker.
This repository also includes a docker-compose configuration.
