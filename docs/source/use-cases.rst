Use Cases
=========

Dask is a versatile tool that supports a variety of workloads.  This page
contains brief and illustrative examples of how people use Dask in practice.
This page emphasizes breadth and hopefully inspires readers to find new ways
that Dask can serve them beyond their original intent.

Overview
--------

Dask use cases can be roughly divided in the following two categories:

1.  Large NumPy/Pandas/Lists with :doc:`dask.array<array>`,
    :doc:`dask.dataframe<dataframe>`, :doc:`dask.bag<bag>` to analyze large
    datasets with familiar techniques.  This is similar to Databases, Spark_,
    or big array libraries
2.  Custom task scheduling.  You submit a graph of functions that depend on
    each other for custom workloads.  This is similar to Luigi_, Airflow_,
    Celery_, or Makefiles_

Most people today approach Dask assuming it is a framework like Spark, designed
for the first use case around large collections of uniformly shaped  data.
However, many of the more productive and novel use cases fall into the second
category where Dask is used to parallelize custom workflows.

Dask compute environments can be divided into the following two categories:

1.  Single machine parallelism with threads or processes:  the Dask
    single-machine scheduler leverages the full CPU power of a laptop or a
    large workstation and changes the space limitation from "fits in memory" to
    "fits on disk".  This scheduler is simple to use and doesn't have the
    computational or conceptual overhead of most "big data" systems
2.  Distributed cluster parallelism on multiple nodes:  the Dask distributed
    scheduler coordinates the actions of multiple machines on a cluster.  It
    scales anywhere from a single machine to a thousand machines, but not
    significantly beyond

The single machine scheduler is more useful to individuals (more people have
personal laptops than have access to clusters) and probably accounts for 80+% of
the use of Dask today.  On the other hand, the distributed machine scheduler is 
more useful to larger organizations like universities, research labs, or private 
companies.

.. _Airflow: https://airflow.apache.org/ 
.. _Luigi: https://luigi.readthedocs.io/en/latest/
.. _Celery: http://www.celeryproject.org/
.. _Spark: https://spark.apache.org/
.. _Makefiles: https://en.wikipedia.org/wiki/Make_(software)

Below we give specific examples of how people use Dask.  We start with large
NumPy/Pandas/List examples because they're somewhat more familiar to people
looking at "big data" frameworks.  We then follow with custom scheduling
examples, which tend to be applicable more often and are, arguably, a bit more
interesting.

Collection Examples
-------------------

Dask contains large parallel collections for n-dimensional arrays (similar to
NumPy), DataFrames (similar to Pandas), and lists (similar to PyToolz or
PySpark).

On disk arrays
~~~~~~~~~~~~~~

Scientists studying the earth have 10GB to 100GB of regularly gridded weather
data on their laptop's hard drive stored as many individual HDF5 or NetCDF
files.  They use :doc:`dask.array<array>` to treat this stack of HDF5 or NetCDF
files as a single NumPy_ array (or a collection of NumPy arrays with the
XArray_ project).  They slice, perform reductions, compute seasonal averaging,
etc., all with straight Numpy syntax.  These computations take a few minutes to
execute (reading 100GB from disk is somewhat slow), but previously infeasible
computations become convenient from the comfort of a personal laptop.

It's not so much parallel computing that is valuable here, but rather the
ability to comfortably compute on larger-than-memory data without special
hardware.

.. code-block:: python

    import h5py
    dataset = h5py.File('myfile.hdf5')['/x']

    import dask.array as da
    x = da.from_array(dataset, chunks=dataset.chunks)

    y = x[::10] - x.mean(axis=0)
    y.compute()

.. _NumPy: https://www.numpy.org/
.. _XArray: https://xarray.pydata.org/en/stable/

Directory of CSV or tabular HDF files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Analysts studying time series data have a large directory of CSV, HDF, or
other formatted tabular files.  They usually use Pandas_ for this kind of
data but, either the volume is too large, or dealing with a large number of files
is confusing, it can be a slow process.  So, they can use :doc:`dask.dataframe<dataframe>` 
to logically wrap all of these different files into one logical DataFrame that is 
built on demand to save space.  Since most of their Pandas workflow is the same (Dask's 
DataFrame is a subset of Pandas), they can switch from Pandas to Dask and back easily
without significantly changing their code.

.. code-block:: python

   import dask.dataframe as dd

   df = dd.read_csv('data/2016-*.*.csv', parse_dates=['timestamp'])
   df.groupby(df.timestamp.dt.hour).value.mean().compute()

.. _Pandas: https://pandas.pydata.org/


Directory of CSV files on HDFS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same analysts as above use dask.dataframe with the dask.distributed_ scheduler
to analyze terabytes of data on their institution's Hadoop cluster straight
from Python.  This uses either the hdfs3_ or pyarrow_ Python libraries for HDFS management.

This solution is particularly attractive because it stays within the Python
ecosystem, and uses the speed and algorithm set of Pandas_, a tool which
the analyst is already very comfortable with.

.. code-block:: python

   from dask.distributed import Client
   client = Client('cluster-address:8786')

   import dask.dataframe as dd
   df = dd.read_csv('hdfs://data/2016-*.*.csv', parse_dates=['timestamp'])
   df.groupby(df.timestamp.dt.hour).value.mean().compute()

.. _hdfs3: https://hdfs3.readthedocs.io/en/latest/
.. _pyarrow: https://arrow.apache.org/docs/python/index.html


Directories of custom format files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same analysts also have a bunch of files of a custom format not supported by
dask.dataframe, or perhaps these files are in a directory structure that
encodes important information about their data (such as the date or other
metadata).  To work around this, they use :doc:`dask.delayed<delayed>` to teach 
dask.dataframe how to load the data and then pass it into dask.dataframe for tabular 
algorithms.

* Example Notebook: https://gist.github.com/mrocklin/e7b7b3a65f2835cda813096332ec73ca


JSON data
~~~~~~~~~

Data Engineers with click stream data from a website, or mechanical engineers
with telemetry data from mechanical instruments, have large volumes of data in
JSON or some other semi-structured format.  They use :doc:`dask.bag<bag>` to
manipulate many Python objects in parallel, either on their personal machine
where they stream the data through memory, or across a cluster.

.. code-block:: python

   import dask.bag as db
   import json

   records = db.read_text('data/2015-*-*.json').map(json.loads)
   records.filter(lambda d: d['name'] == 'Alice').pluck('id').frequencies()


Custom Examples
---------------

The large collections (array, dataframe, bag) are wonderful when they fit the
application, for example, if you want to perform a groupby on a directory of CSV
data.  However, several parallel computing applications don't fit neatly into
one of these higher level abstractions.  Fortunately, Dask provides a wide
variety of ways to parallelize more custom applications.  These use the same
machinery as the arrays and DataFrames, but allow the user to develop custom
algorithms specific to their problem.

Embarrassingly parallel computation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some programmers have a function that they want to run many times on different
inputs.  Their function and inputs might use arrays or DataFrames internally,
but conceptually their problem isn't a single large array or DataFrame.

They want to run these functions in parallel on their laptop while they prototype,
but they also intend to eventually use an in-house cluster.  To accomplish this, they 
wrap their function in :doc:`dask.delayed<delayed>` and then let the appropriate dask 
scheduler parallelize and load balance the work.

.. code-block:: python

   def process(data):
      ...
      return ...

**Normal Sequential Processing**:

.. code-block:: python

   results = [process(x) for x in inputs]

**Build Dask Computation**:

.. code-block:: python

   from dask import compute, delayed
   values = [delayed(process)(x) for x in inputs]

**Multiple Threads**:

.. code-block:: python

   import dask.threaded
   results = compute(*values, scheduler='threads')

**Multiple Processes**:

.. code-block:: python


   import dask.multiprocessing
   results = compute(*values, scheduler='processes')

**Distributed Cluster**:

.. code-block:: python


   from dask.distributed import Client
   client = Client("cluster-address:8786")
   results = compute(*values, scheduler='distributed')


Complex dependencies
~~~~~~~~~~~~~~~~~~~~

A financial analyst has many models that depend on each other in a 
complex web of computations.

.. code-block:: python

   data = [load(fn) for fn in filenames]
   reference = load_from_database(query)

   A = [model_a(x, reference) for x in data]
   B = [model_b(x, reference) for x in data]

   roll_A = [roll(A[i], A[i + 1]) for i in range(len(A) - 1)]
   roll_B = [roll(B[i], B[i + 1]) for i in range(len(B) - 1)]
   compare = [compare_ab(a, b) for a, b in zip(A, B)]

   results = summarize(compare, roll_A, roll_B)

These models are time consuming and need to be run on a variety of inputs and
situations.  Now, the analyst has his code as a collection of Python functions,
and is trying to figure out how to parallelize such a codebase.  To solve this, 
he uses dask.delayed to wrap his function calls and capture the implicit parallelism.

.. code-block:: python

   from dask import compute, delayed

   data = [delayed(load)(fn) for fn in filenames]
   reference = delayed(load_from_database)(query)

   A = [delayed(model_a)(x, reference) for x in data]
   B = [delayed(model_b)(x, reference) for x in data]

   roll_A = [delayed(roll)(A[i], A[i + 1]) for i in range(len(A) - 1)]
   roll_B = [delayed(roll)(B[i], B[i + 1]) for i in range(len(B) - 1)]
   compare = [delayed(compare_ab)(a, b) for a, b in zip(A, B)]

   lazy_results = delayed(summarize)(compare, roll_A, roll_B)

The analyst then depends on the dask schedulers to run this complex web of computations
in parallel.

.. code-block:: python

    results = compute(lazy_results)

He sees how easy it was to transition from experimental code to a
scalable parallel version.  This code is also easy enough for his
teammates to easily understand and extend it in the future.


Algorithm developer
~~~~~~~~~~~~~~~~~~~

A couple of graduate students in machine learning are prototyping novel parallel
algorithms.  They are in a situation much like the financial analyst above,
except that they need to benchmark and profile their computation heavily under
a variety of situations and scales.  The :doc:`dask profiling tools
<understanding-performance>`  provide the feedback they need to understand
their parallel performance, including how long each task takes, how intense
communication is, and their scheduling overhead.  They scale their algorithm
between 1 and 50 cores on single workstations and then scale out to a cluster
running their computation at thousands of cores.  They don't have access to an
institutional cluster, so instead they use :doc:`dask on the cloud
<setup/cloud>` to easily provision clusters of varying sizes.

Their algorithm is written in the same way in all cases. This drastically reduces 
the cognitive load, and lets the readers of their work experiment with their
system on their own machines, aiding reproducibility.


Scikit-Learn or Joblib User
~~~~~~~~~~~~~~~~~~~~~~~~~~~

A data scientist wants to scale her machine learning pipeline to run on a
cluster to accelerate parameter searches.  She already uses the ``sklearn``
``njobs=`` parameter to accelerate computations on her local computer
with Joblib_.  Now, she wraps her ``sklearn`` code with a context manager to
parallelize the exact same code across a cluster (also available with
IPyParallel_)

.. code-block:: python

   import distributed.joblib

   with joblib.parallel_backend('distributed',
                                scheduler_host=('192.168.1.100', 8786)):
       result = GridSearchCV( ... )  # normal sklearn code

.. _IPyParallel: https://ipyparallel.readthedocs.io/en/latest/


Academic Cluster Administrator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A system administrator for a university compute cluster wants to enable many
researchers to use the available cluster resources, which are currently lying
idle.  The research faculty and graduate students lack experience with job
schedulers and MPI, but are comfortable interacting with Python code through a
Jupyter notebook.

Teaching the faculty and graduate students to parallelize software has proven
to be time consuming.  Instead, the administrator sets up dask.distributed_ on a
sandbox allocation of the cluster and broadly publishes the address of the
scheduler, pointing researchers to the `dask.distributed quickstart`_.
Utilization of the cluster climbs steadily over the next week as researchers
are more easily able to parallelize their computations without having to learn
foreign interfaces.  The administrator is happy because resources are being
used without significant hand-holding.

As utilization increases, the administrator has a new problem: the shared
dask.distributed cluster is being overused.  The administrator tracks use
through Dask diagnostics to identify which users are taking most of the
resources.  He contacts these users and teaches them how to launch_ their own
dask.distributed clusters using the traditional job scheduler on their cluster,
making space for more new users in the sandbox allocation.

.. _`dask.distributed quickstart`: https://distributed.dask.org/en/latest/quickstart.html
.. _launch: https://distributed.dask.org/en/latest/setup.html


Financial Modeling Team
~~~~~~~~~~~~~~~~~~~~~~~

Similar to the case above, a team of modelers working at a financial
institution run a complex network of computational models on top of each
other.  They started using :doc:`dask.delayed<delayed>` individually, as
suggested above, but realized that they often perform highly overlapping
computations, such as always reading the same data.

Now, they decide to use the same Dask cluster collaboratively to save on these
costs.  Because Dask intelligently hashes computations in a way similar to how
Git works, they find that, when two people submit similar computations, the
overlapping part of the computation runs only once.

Ever since working collaboratively on the same cluster, they find that their
frequently running jobs run much faster because most of the work is already
done by previous users.  When they share scripts with colleagues, they find that
those repeated scripts complete immediately rather than taking several hours.

They are now able to iterate and share data as a team more effectively,
decreasing their time to result and increasing their competitive edge.

As this becomes more heavily used on the company cluster, they decide to set up
an auto-scaling system.  They use their dynamic job scheduler (perhaps SGE,
LSF, Mesos, or Marathon) to run a single ``dask-scheduler`` 24/7 and then scale
up and down the number of ``dask-workers`` running on the cluster based on
computational load.  This solution ends up being more responsive (and thus more
heavily used) than their previous attempts to provide institution-wide access
to parallel computing. But because it responds to load, it still acts as a good
citizen in the cluster.


Streaming data engineering
~~~~~~~~~~~~~~~~~~~~~~~~~~

A data engineer responsible for watching a data feed needs to scale out a
continuous process.  She `combines dask.distributed with normal Python Queues`_ to
produce a rudimentary but effective stream processing system.

Because dask.distributed is elastic, she can scale up or scale down her
cluster resources in response to demand.

.. _`combines dask.distributed with normal Python Queues`: https://distributed.dask.org/en/latest/queues.html

.. _Joblib: https://joblib.readthedocs.io/en/latest/
.. _dask.distributed: https://distributed.dask.org/en/latest/
