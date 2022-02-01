Dashboard Diagnostics
=====================

Profiling parallel code can be challenging, but the :doc:`Dask distributed scheduler <scheduling>` 
provides live feedback via its interactive dashboard. 

## Include dashboard picture status page

To be able to to use the dashboard diagnostics `Bokeh <https://docs.bokeh.org>`_ needs to 
be installed. A link that redirects to the dashboard will prompt whenever the scheduler is 
created, and you can access the link when you create a client.

.. code-block:: python

   from dask.distributed import Client
   client = Client()  # start distributed scheduler locally. 
   client            

.. figure:: images/dashboard_link.png
    :alt: NEEDS ALT TEXT

The address of the dashboard will be displayed if you are in a Jupyter Notebook,
or it can be queried from ``client.dashboard_link``. By default, when starting a schduler 
locally the dashboard will be served at ``http://localhost:8787/status``, but may be served 
elsewhere if this is desired or if the port is taken.

The dashboard link redirects you to the `/status` page, which is the entry point of the 
dashboard:

.. figure:: images/dashboard_status.png
    :alt: NEEDS ALT TEXT

- Bytes stored: Cluster memory. 
- Bytes per Worker: Memory per worker.
- Task Processing/CPU Utilization/Occupancy: Tasks being processed by each worker/ NEEDS INFO/ NEEDS INFO
- Task Stream: Individual task across threads.
- Progress: Progress of a set of tasks.


Bytes Stored and Bytes per Worker
---------------------------------

.. figure:: images/dashboard_memory.png
    :alt: NEEDS ALT TEXT

These two plots show a summary of the overall memory usage on the cluster,
as well as the individual usage on each worker. In these plots, the colors and 
shades have different meanings:
    - Solid Blue: Managed memory under target (default 70% of memory)
    - Solid Orange: Managed memory is close to the spilling target (default 70% of memory)

For both blue and orange we have two more shades:
    - Lighter shade: Unamanged memory
    - Even lighter shade: Unmanaged recent memory. 

    - Grey: Spilled to disk memory 

For a detailed explanation on the different types of memory refer to the Worker Memory management
documentation `Worker Memory management documentation <https://distributed.dask.org/en/latest/worker.html#memory-management>`_

Processing/CPU/Occupancy
------------------------

**Task Processing** 

.. figure:: images/dashboard_task_processing.png
    :alt: NEEDS ALT TEXT

The Processing plot, shows the tasks being processed across each worker. Ideally you want, each worker to 
process an equally amount of tasks. If one of the bars is completely white it means that 
worker is idle and waiting for tasks. This usually happens when the computations are close to finish (nothing 
to worry about), but it can also mean that the distribution of the task across workers is not optimized and you
will want to revisit your code. 

In this plot you can also find the colors red and green: **(PENDING)**

- Green : 
- Red :

In this plot on the dashboard we have two extra tabs with the following information:

**CPU Utilization**

CPU usage per-worker (this needs some love, haven't found a nice way of describing this) 

**Occupancy**

This tab shows the occupancy, in time, per worker. The total occupancy for a worker is the total expected runtime
for all tasks currently on a worker. For example, an occupancy of 10s means (it's an estimation) that it will take the 
worker 10s to execute all the tasks it has currently been assigned.

Task Stream
-----------

The task stream is a view of all the tasks across threads. Each row represents a thread and each rectangle represent 
an individual tasks. The color for each rectangle corresponds to the kind of task being performed and it matches the color 
of the Progress plot (see Progress section). This means that all the individual tasks of kind `inc` for example will have 
the same randomly assigned color. 

There are certain colors that are reserved for a specific kinds of tasks:

- Red: Transfer / communication 
- Orange: Disk spilling
- Black: Erred tasks
- Grey: Serialization and deserialization
- White: Idle thread

.. figure:: images/dashboard_taskstream_healthy.png
    :alt: NEEDS ALT TEXT

.. figure:: images/dashboard_task_stream_unhealthy.png
    :alt: NEEDS ALT TEXT

In some scenarios the dashboard will have white spaces between each rectangles, this means that the during that time the thread
is idle. Having to much white and red is an indication of not optimal use of resources, and you will want to revisit your 
computations and/or the resources allocated. 


Progress
--------

.. figure:: images/dashboard_progress.png
    :alt: NEEDS ALT TEXT

The progress bars plot shows the progress of each individual kind of task. The color of the of each bar matches the color of the 
individual tasks on the task stream that correspond to the same kind. Each horizontal bar has three different components:

- Grey : Tasks that are ready to run.
- Solid color : Tasks that have been completed and are in memory.
- Transparent color: Tasks that have been completed, been in memory and have been released.