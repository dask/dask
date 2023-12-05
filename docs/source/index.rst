====
Dask
====

*Dask is a Python library for parallel and distributed computing.*  Dask is ...

-  **Easy** to use and set up (it's just a Python library)
-  **Powerful** at providing scale, and unlocking complex algorithms
   scale up
-  and **Fun** 🎉

How to Use Dask
---------------

Dask provides several APIs.  Choose one that works best for you:

.. tab-set::

    .. tab-item:: Tasks

        Dask Futures parallelize arbitrary for-loop style Python code,
        providing:

        -  **Flexible** tooling allowing you to construct custom
           pipelines and workflows
        -  **Powerful** scaling techniques, processing several thousand
           tasks per second
        -  **Responsive** feedback allowing for intuitive execution,
           and helpful dashboards

        Dask futures form the foundation for other Dask work

        Learn more at :bdg-link-primary:`Futures Documentation <futures.html>`

        .. grid:: 1 1 2 2

            .. grid-item::
                :columns: 12 12 7 7

                .. code-block:: python

                    from dask.distributed import LocalCluster
                    client = LocalCluster().get_client()

                    # Submit work to happen in parallel
                    results = []
                    for filename in filenames:
                        data = client.submit(load, filename)
                        result = client.submit(process, data)
                        results.append(result)

                    # Gather results back to local computer
                    results = client.gather(results)

            .. grid-item::
                :columns: 12 12 5 5

                .. figure:: images/futures-graph.png
                   :align: center

    .. tab-item:: DataFrames

        Dask Dataframes parallelize the popular pandas library, providing:

        -   **Larger-than-memory** execution for single machines, allowing you
            to process data that is larger than your available RAM
        -   **Parallel** execution for faster processing
        -   **Distributed** computation for terabyte-sized datasets

        Dask Dataframes are similar in this regard to Apache Spark, but use the
        familiar pandas API and memory model.  One Dask dataframe is simply a
        collection of pandas dataframes on different computers.

        Learn more at :bdg-link-primary:`DataFrame Documentation <dataframe.html>`

        .. grid:: 1 1 2 2

            .. grid-item::
                :columns: 12 12 7 7

                .. code-block:: python

                    import dask.dataframe as dd

                    # Read large datasets in parallel
                    df = dd.read_parquet("s3://mybucket/data.*.parquet")
                    df = df[df.value < 0]
                    result = df.groupby(df.name).amount.mean()

                    result = result.compute()  # Compute to get pandas result
                    result.plot()

            .. grid-item::
                :columns: 12 12 5 5

                .. figure:: images/dask-dataframe.svg
                   :align: center


    .. tab-item:: Arrays

        Dask Arrays parallelize the popular NumPy library, providing:

        -   **Larger-than-memory** execution for single machines, allowing you
            to process data that is larger than your available RAM
        -   **Parallel** execution for faster processing
        -   **Distributed** computation for terabyte-sized datasets

        Dask Arrays allow scientists and researchers to perform intuitive and
        sophisticated operations on large datasets but use the
        familiar NumPy API and memory model.  One Dask array is simply a
        collection of NumPy arrays on different computers.

        Learn more at :bdg-link-primary:`Array Documentation <array.html>`

        .. grid:: 1 1 2 2

            .. grid-item::

                .. code-block:: python

                    import dask.array as da

                    x = da.random.random((10000, 10000))
                    y = (x + x.T) - x.mean(axis=1)

                    z = y.var(axis=0).compute()

            .. grid-item::
                :columns: 12 12 5 5

                .. figure:: images/dask-array.svg
                   :align: center

        Xarray wraps Dask array and is a popular downstream project, providing
        labeled axes and simultaneously tracking many Dask arrays together,
        resulting in more intuitive analyses.  Xarray is popular and accounts
        for the majority of Dask array use today especially within geospatial
        and imaging communities.

        Learn more at :bdg-link-primary:`Xarray Documentation <https://docs.xarray.dev/en/stable/>`

        .. grid:: 1 1 2 2

            .. grid-item::

                .. code-block:: python

                    import xarray as xr

                    ds = xr.open_mfdataset("data/*.nc")
                    da.groupby('time.month').mean('time').compute()


            .. grid-item::
                :columns: 12 12 5 5

                .. figure:: https://docs.xarray.dev/en/stable/_static/dataset-diagram-logo.png
                   :align: center

How to Install Dask
-------------------

Installing Dask is easy with ``pip`` or ``conda``

Learn more at :bdg-link-primary:`Install Documentation <install.html>`

.. tab-set::

    .. tab-item:: pip

       .. code-block::

          pip install "dask[complete]"

    .. tab-item:: conda

       .. code-block::

          conda install dask

How to Deploy Dask
------------------

You can then use Dask on a single machine, or deploy it on distributed hardware

Learn more at :bdg-link-primary:`Deploy Documentation <deploying.html>`

.. tab-set::

    .. tab-item:: Local

       Dask can set itself up easily in your Python session if you create a
       ``LocalCluster`` object, which sets everything up for you.

       .. code-block:: python

          from dask.distributed import LocalCluster
          cluster = LocalCluster()
          client = cluster.get_client()

          # Normal Dask work ...

       Alternatively, you can skip this part, and Dask will operate within a
       thread pool contained entirely with your local process.

    .. tab-item:: Kubernetes

       The `dask-kubernetes project <https://kubernetes.dask.org>`_ provides
       a Dask Kubernetes Operator.

       .. code-block:: python

          from dask_kubernetes.operator import KubeCluster
          cluster = KubeCluster(
             name="my-dask-cluster",
             image='ghcr.io/dask/dask:latest'
          )
          cluster.scale(10)

       Learn more at :bdg-link-primary:`Dask Kubernetes Documentation <https://kubernetes.dask.org>`

    .. tab-item:: HPC

        The `dask-jobqueue project <https://jobqueue.dask.org>`_ interfaces
        with popular job submission projects, like SLURM, PBS, SGE, LSF,
        Torque, Condor, and others.


        .. code-block:: python

           from dask_jobqueue import SLURMCluster

           cluster = SLURMCluster()
           cluster.scale(jobs=10)


        You can also deploy Dask with MPI

        .. code-block:: python

           # myscript.py
           from dask_mpi import initialize
           initialize()

           from dask.distributed import Client
           client = Client()  # Connect this local process to remote workers

        .. code-block::

           $ mpirun -np 4 python myscript.py

        Learn more at :bdg-link-primary:`Dask Jobqueue Documentation <https://jobqueue.dask.org>` and the :bdg-link-primary:`Dask MPI Documentation <https://mpi.dask.org>`.

    .. tab-item:: Cloud

        The `dask-cloudprovider project <https://cloudprovider.dask.org>`_ interfaces
        with popular cloud platforms like AWS, GCP, Azure, and Digital Ocean.

        .. code-block:: python

           from dask_cloudprovider.aws import FargateCluster
           cluster = FargateCluster(
               # Cluster manager specific config kwargs
           )

        Learn more at :bdg-link-primary:`Dask CloudProvider Documentation <https://cloudprovider.dask.org>`

    .. tab-item:: Cloud-SaaS

        Several companies offer commercial Dask products.  These are not open
        source, but tend to be easier to set up and use, safer, cheaper, etc..
        Here is an incomplete list.
        
        .. _Coiled: https://coiled.io/?utm_source=dask-docs&utm_medium=homepage
        .. |coiled| replace:: **Coiled**
      
        -  |Coiled|_ provides a standalone Dask deployment product that works
           in AWS and GCP.  Coiled notably employs most of the active Dask
           maintainers today.

           Learn more at :bdg-link-primary:`Coiled <https://coiled.io>`

      .. _saturn: https://saturncloud.io
      .. |saturn| replace:: **Saturn Cloud**

        -  |saturn|_ provides Dask as part of their hosted platform
           including Jupyter and other products.

           Learn more at :bdg-link-primary:`Saturn Cloud <https://saturncloud.io>`

Learn with Examples
-------------------

Dask use is widespread, across all industries and scales.  Dask is used
anywhere Python is used and people experience pain due to large scale data, or
intense computing.

You can learn more about Dask applications at the following sources:

-  `Dask Examples <https://examples.dask.org>`_
-  `Dask YouTube Channel <https://youtube.com/@dask-dev>`_

Additionally, we encourage you to look through the reference documentation on
this website related to the API that most closely matches your application.

Dask was designed to be **easy to use** and **powerful**.  We hope that it's
able to help you have fun with your work.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Getting Started

   Install Dask <install.rst>
   10-minutes-to-dask.rst
   presentations.rst
   Best Practices <best-practices.rst>
   faq.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: API

   array.rst
   bag.rst
   DataFrame <dataframe.rst>
   Delayed <delayed.rst>
   futures.rst
   deploying.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Internals

   understanding-performance.rst
   scheduling.rst
   graphs.rst
   debugging-performance.rst
   internals.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Reference

   api.rst
   cli.rst
   develop.rst
   changelog.rst
   configuration.rst
   how-to/index.rst
   maintainers.rst

.. _`Anaconda Inc`: https://www.anaconda.com
.. _`3-clause BSD license`: https://github.com/dask/dask/blob/main/LICENSE.txt

.. _`#dask tag`: https://stackoverflow.com/questions/tagged/dask
.. _`GitHub issue tracker`: https://github.com/dask/dask/issues
.. _`xarray`: https://xarray.pydata.org/en/stable/
.. _`scikit-image`: https://scikit-image.org/docs/stable/
.. _`scikit-allel`: https://scikits.appspot.com/scikit-allel
.. _`pandas`: https://pandas.pydata.org/pandas-docs/version/0.17.0/
.. _`distributed scheduler`: https://distributed.dask.org/en/latest/
