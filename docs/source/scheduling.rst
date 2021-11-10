Scheduling
==========

.. toctree::
   :maxdepth: 1
   :hidden:

   scheduling-distributed.rst
   scheduling-single-machine.rst

All of the large-scale Dask collections like
:doc:`Dask Array <array>`, :doc:`Dask DataFrame <dataframe>`, and :doc:`Dask Bag <bag>`
and the fine-grained APIs like :doc:`delayed <delayed>` and :doc:`futures <futures>`
generate task graphs. Each node in the graph is a normal Python function
and edges between nodes are normal Python objects
that are created by one task as outputs and used as inputs in another task.
After Dask generates these task graphs, it needs to execute them on parallel hardware.
This is the job of a *task scheduler*.
Different task schedulers exist, and each will consume a task graph and compute the
same result, but with different performance characteristics.

.. image:: images/collections-schedulers.png
   :alt: Dask collections and schedulers
   :width: 80%
   :align: center

Dask has two families of task schedulers:

1.  **Distributed scheduler**: This sophisticated scheduler offers many features
    and can run locally to distribute across CPU cores or across a cluster on many CPUs.
    It is often preferable, even on single workstations. It contains many diagnostics and
    features not found in the older single-machine scheduler
2.  **Single machine scheduler**: This scheduler provides basic features on a
    local process or thread pool.  This scheduler was made first and is the
    default if you do not specify a distributed scheduler.
    It is simple and cheap to use, although it can only be used on
    a single machine and does not scale.


