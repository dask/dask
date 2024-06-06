====
Dask
====

.. grid:: 1 1 2 2

    .. grid-item::
       :columns: 12 12 6 6

       *Dask is a Python library for parallel and distributed computing.*  Dask is:

       -  **Easy** to use and set up (it's just a Python library)
       -  **Powerful** at providing scale, and unlocking complex algorithms
       -  and **Fun** 🎉

    .. grid-item::
       :columns: 12 12 6 6

       .. raw:: html

            <script src="https://fast.wistia.com/embed/medias/l9sgt2saht.jsonp" async></script><script src="https://fast.wistia.com/assets/external/E-v1.js" async></script><div class="wistia_responsive_padding" style="padding:75.0% 0 0 0;position:relative;"><div class="wistia_responsive_wrapper" style="height:100%;left:0;position:absolute;top:0;width:100%;"><div class="wistia_embed wistia_async_l9sgt2saht seo=true videoFoam=true" style="height:100%;position:relative;width:100%">&nbsp;</div></div></div>


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
        or see an example at :bdg-link-primary:`Futures Example <https://examples.dask.org/futures.html>`

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
        or see an example at :bdg-link-primary:`DataFrame Example <https://examples.dask.org/dataframe.html>`

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
        or see an example at :bdg-link-primary:`Array Example <https://examples.dask.org/array.html>`

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
        or see an example at :bdg-link-primary:`Xarray Example <https://examples.dask.org/xarray.html>`

        .. grid:: 1 1 2 2

            .. grid-item::

                .. code-block:: python

                    import xarray as xr

                    ds = xr.open_mfdataset("data/*.nc")
                    da.groupby('time.month').mean('time').compute()


            .. grid-item::
                :columns: 12 12 5 5

                .. figure:: https://docs.xarray.dev/en/stable/_static/logos/Xarray_Logo_RGB_Final.png
                   :align: center

    .. tab-item:: Bags

        Dask Bags are simple parallel Python lists, commonly used to process
        text or raw Python objects.  They are ...

        -   **Simple** offering easy map and reduce functionality
        -   **Low-memory** processing data in a streaming way that minimizes memory use
        -   **Good for preprocessing** especially for text or JSON data prior
            ingestion into dataframes

        Dask bags are similar in this regard to Spark RDDs or vanilla
        Python data structures and iterators.  One Dask bag is simply a
        collection of Python iterators processing in parallel on different computers.

        Learn more at :bdg-link-primary:`Bag Documentation <bag.html>`
        or see an example at :bdg-link-primary:`Bag Example <https://examples.dask.org/bag.html>`

        .. code-block:: python

            import dask.bag as db

            # Read large datasets in parallel
            lines = db.read_text("s3://mybucket/data.*.json")
            records = (lines
                .map(json.loads)
                .filter(lambda d: d["value"] > 0)
            )
            df = records.to_dask_dataframe()

How to Install Dask
-------------------

Installing Dask is easy with ``pip`` or ``conda``.

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

You can use Dask on a single machine, or deploy it on distributed hardware.

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

    .. tab-item:: Cloud

        `Coiled <https://docs.coiled.io/user_guide/index.html?utm_source=dask-docs&utm_medium=homepage>`_
        is a commercial SaaS product that deploys Dask clusters on cloud platforms like AWS, GCP, and Azure.

        .. code-block:: python

            import coiled
            cluster = coiled.Cluster(
               n_workers=100,
               region="us-east-2",
               worker_memory="16 GiB",
               spot_policy="spot_with_fallback",
            )
            client = cluster.get_client()

        Learn more at :bdg-link-primary:`Coiled Documentation <https://docs.coiled.io/user_guide/index.html?utm_source=dask-docs&utm_medium=homepage>`


    .. tab-item:: HPC

        The `Dask-Jobqueue project <https://jobqueue.dask.org>`_ deploys
        Dask clusters on popular HPC job submission systems like SLURM, PBS, SGE, LSF,
        Torque, Condor, and others.

        .. code-block:: python

            from dask_jobqueue import PBSCluster
            cluster = PBSCluster(
               cores=24,
               memory="100GB",
               queue="regular",
               account="my-account",
            )
            cluster.scale(jobs=100)
            client = cluster.get_client()

        Learn more at :bdg-link-primary:`Dask-Jobqueue Documentation <https://jobqueue.dask.org>`

    .. tab-item:: Kubernetes

       The `Dask Kubernetes project <https://kubernetes.dask.org>`_ provides
       a Dask Kubernetes Operator for deploying Dask on Kubernetes clusters.

       .. code-block:: python

            from dask_kubernetes.operator import KubeCluster
            cluster = KubeCluster(
               name="my-dask-cluster",
               image="ghcr.io/dask/dask:latest",
               resources={"requests": {"memory": "2Gi"}, "limits": {"memory": "64Gi"}},
            )
            cluster.scale(10)
            client = cluster.get_client()


       Learn more at :bdg-link-primary:`Dask Kubernetes Documentation <https://kubernetes.dask.org>`


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
   deploying.rst
   Best Practices <best-practices.rst>
   faq.rst

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: How to Use

   array.rst
   bag.rst
   DataFrame <dataframe.rst>
   Delayed <delayed.rst>
   futures.rst
   ml.rst

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
   presentations.rst
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
