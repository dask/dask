Adaptive Deployments
====================

Motivation
----------

Most Dask deployments are static with a single scheduler and a fixed number of
workers.  This results in predictable behavior, but is wasteful of resources in
two situations:

1.  The user may not be using the cluster, or perhaps they are busy
    interpreting a recent result or plot, and so the workers sit idly,
    taking up valuable shared resources from other potential users
2.  The user may be very active, and is limited by their original allocation.

Particularly efficient users may learn to manually add and remove workers
during their session, but this is rare.  Instead, we would like the size of a
Dask cluster to match the computational needs at any given time.  This is the
goal of the *adaptive deployments* discussed in this document.  These are
particularly helpful for interactive workloads, which are characterized by long
periods of inactivity interrupted with short bursts of heavy activity.
Adaptive deployments can result in both faster analyses that give users much
more power, but with much less pressure on computational resources.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/dViyEqOMA8U"
           style="margin: 0 auto 20px auto; display: block;"
           frameborder="0"
           allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"
           allowfullscreen></iframe>


Adaptive
--------

To make setting up adaptive deployments easy, some Dask deployment solutions
offer an ``.adapt()`` method.  Here is an example with
`dask_kubernetes.KubeCluster
<https://kubernetes.dask.org/en/latest/api.html#dask_kubernetes.KubeCluster>`_.

.. code-block:: python

   from dask_kubernetes import KubeCluster

   cluster = KubeCluster()
   cluster.adapt(minimum=0, maximum=100)  # scale between 0 and 100 workers

For more keyword options, see the Adaptive class below:

.. currentmodule:: distributed.deploy   #doctest: +SKIP

.. autosummary::
   Adaptive


Dependence on a Resource Manager
--------------------------------

The Dask scheduler does not know how to launch workers on its own. Instead, it
relies on an external resource scheduler like Kubernetes above, or
Yarn, SGE, SLURM, Mesos, or some other in-house system (see :doc:`setup
documentation <../setup>` for options).  In order to use adaptive deployments, you
must provide some mechanism for the scheduler to launch new workers.  Typically,
this is done by using one of the solutions listed in the :doc:`setup
documentation <../setup>`, or by subclassing from the Cluster superclass and
implementing that API.

.. autosummary::
   Cluster


Scaling Heuristics
------------------

The Dask scheduler tracks a variety of information that is useful to correctly
allocate the number of workers:

1.  The historical runtime of every function and task that it has seen,
    and all of the functions that it is currently able to run for users
2.  The amount of memory used and available on each worker
3.  Which workers are idle or saturated for various reasons, like the presence
    of specialized hardware

From these, it is able to determine a target number of workers by dividing the
cumulative expected runtime of all pending tasks by the ``target_duration``
parameter (defaults to five seconds).  This number of workers serves as a
baseline request for the resource manager.  This number can be altered for a
variety of reasons:

1.  If the cluster needs more memory, then it will choose either the target
    number of workers or twice the current number of workers (whichever is
    larger)
2.  If the target is outside of the range of the minimum and maximum values,
    then it is clipped to fit within that range

Additionally, when scaling down, Dask preferentially chooses those workers that
are idle and have the least data in memory.  It moves that data to other
machines before retiring the worker.  To avoid rapid cycling of the cluster up
and down in size, we only retire a worker after a few cycles have gone by where
it has consistently been a good idea to retire it (controlled by the
``wait_count`` and ``interval`` parameters).


API
---

.. autoclass:: Adaptive
.. autoclass:: Cluster
