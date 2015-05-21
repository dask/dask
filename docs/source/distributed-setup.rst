Setting up a dask.distributed cluster
=====================================

*Dask.distributed has not been battle tested. It is for experimental use only*

Setting up a 

IPython.parallel
----------------

..change IPython.parallel to ipyparallel when that name catches on.

Users familiar with ``IPython.parallel`` can use an ``IPython.parallel.Client``
object, connected to a running ``ipcluster`` to bootstrap a dask distributed
cluster.

.. code-block:: python

    # Setup your IPython cluster...
    # Create a client.
    from IPython.parallel import Client
    ipclient = Client()

    # Now we use it to set up a dask.distributed cluster
    from dask.distributed import dask_client_from_ipclient
    dclient = dask_client_from_ipclient(ipclient)

    # the dask client's get method computes dask graphs on the cluster.
        dclient.get({'a': 41, 'b': (lambda x: x + 1, 'a')}, 'b')

More info about setting up an IPython cluster can be found here_.

.. _here: http://ipython.org/ipython-doc/dev/parallel/parallel_process.html

Anaconda cluster
````````````````

`Anaconda cluster`_ is a tool for launching and managing clusters. We use to
create a cluster, install dask and IPython, then launch an IPython notebook and
cluster.
