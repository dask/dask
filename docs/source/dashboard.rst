Dashboard Diagnostics
=====================

.. meta::
   :description: The interactive Dask dashboard provides numerous diagnostic plots for live monitoring of your Dask computation. It includes information about task runtimes, communication, statistical profiling, load balancing, memory use, and much more.

.. raw:: html

    <iframe width="560"
            height="315"
            src="https://www.youtube.com/embed/N_GqzcuGLCY"
            frameborder="0"
            allow="autoplay; encrypted-media"
            style="margin: 0 auto 20px auto; display: block;"
            allowfullscreen>
    </iframe>

Profiling parallel code can be challenging, but the interactive dashboard provided with
Dask's :doc:`distributed scheduler <scheduling>` makes this easier with live monitoring
of your Dask computations. The dashboard is built with `Bokeh <https://docs.bokeh.org>`_
and will start up automatically, returning a link to the dashboard whenever the scheduler is created.

Locally, this is when you create a :class:`Client <distributed.client.Client>` and connect the scheduler:

.. code-block:: python

   from dask.distributed import Client
   client = Client()  # start distributed scheduler locally.

In a Jupyter Notebook or JupyterLab session displaying the client object will show the dashboard address:

.. figure:: images/dashboard_link.png
    :alt: Client html repr displaying the dashboard link in a JupyterLab session.

You can also query the address from ``client.dashboard_link`` (or for older versions of distributed, ``client.scheduler_info()['services']``).

By default, when starting a scheduler on your local machine the dashboard will be served at ``http://localhost:8787/status``. You can type this address into your browser to access the dashboard, but may be directed
elsewhere if port 8787 is taken. You can also configure the address using the ``dashboard_address``
parameter (see :class:`LocalCluster <distributed.deploy.local.LocalCluster>`).

There are numerous diagnostic plots available. In this guide you'll learn about some
of the most commonly used plots shown on the entry point for the dashboard:

- :ref:`dashboard.memory`: Cluster memory and Memory per worker
- :ref:`dashboard.proc-cpu-occ`:  Tasks being processed by each worker/ CPU Utilization per worker/ Expected runtime for all tasks currently on a worker.
- :ref:`dashboard.task-stream`: Individual task across threads.
- :ref:`dashboard.progress`: Progress of a set of tasks.

.. figure:: images/dashboard_status.png
    :alt: Main dashboard with five panes arranged into two columns. In the left column there are three bar charts. The top two show total bytes stored and bytes per worker. The bottom has three tabs to toggle between task processing, CPU utilization, and occupancy. In the right column, there are two bar charts with corresponding colors showing task activity over time, referred to as task stream and progress.

.. _dashboard.memory:

Bytes Stored and Bytes per Worker
---------------------------------

These two plots show a summary of the overall memory usage on the cluster (Bytes Stored),
as well as the individual usage on each worker (Bytes per Worker). The colors on these plots
indicate the following.

.. raw:: html

    <table>
        <tr>
            <td>
                <div role="img" aria-label="blue square" style="color:rgba(0, 0, 255, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Memory under target (default 60% of memory available) </td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="orange square" style="color:rgba(255, 165, 0, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td> Memory is close to the spilling to disk target (default 70% of memory available)</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="red square" style="color:rgba(255, 0, 0, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>When the worker (or at least one worker) is paused (default 80% of memory available) or retiring</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="grey square" style="color:rgba(128, 128, 128, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Memory spilled to disk</td>
        </tr>
    </table>

.. figure:: images/dashboard_memory_new.gif
    :alt: Two bar charts on memory usage. The top chart shows the total cluster memory in a single bar with mostly under target memory - changing colors according to memory usage, (blue - under target, orange - Memory is about to be spilled, red - paused or retiring, and a small part of spilled to disk in grey. The bottom chart displays the memory usage per worker, with a separate bar for each of the four workers. The four bars can be seen in various colours as in blue when under target, orange as their worker's memory are close to the spilling to disk target, with the second and fourth worker standing out with a portion in grey that correspond to the amount spilled to disk, also fourth worker in red is paused or about to retire.

The different levels of transparency on these plot is related to the type of memory
(Managed, Unmanaged and Unmanaged recent), and you can find a detailed explanation of them in the
:doc:`Worker Memory management documentation <worker-memory>`


.. _dashboard.proc-cpu-occ:

Task Processing/CPU Utilization/Occupancy/Data Transfer
-------------------------------------------------------

**Task Processing**

The *Processing* tab in the figure shows the number of tasks that have been assigned to each worker. Not all of these
tasks are necessarily *executing* at the moment: a worker only executes as many tasks at once as it has threads. Any
extra tasks assigned to the worker will wait to run, depending on their :doc:`priority <priority>` and whether their
dependencies are in memory on the worker.

The scheduler will try to ensure that the workers are processing about the same number of tasks. If one of the bars is
completely white it means that worker has no tasks and is waiting for them. This usually happens when the computations
are close to finished (nothing to worry about), but it can also mean that the distribution of the task across workers is
not optimized.

There are three different colors that can appear in this plot:

.. raw:: html

    <table>
        <tr>
            <td>
                <div role="img" aria-label="blue square" style="color:rgba(0, 0, 255, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Processing tasks.</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="green square" style="color:rgba(0, 128, 0, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Saturated: It has enough work to stay busy.</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="red square" style="color:rgba(255, 0, 0, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Idle: Does not have enough work to stay busy.</td>
        </tr>
    </table>

.. figure:: images/dashboard_task_processing.png
    :alt: Task Processing bar chart, showing a relatively even number of tasks on each worker.

In this plot on the dashboard we have two extra tabs with the following information:

**CPU Utilization**

The *CPU* tab shows the cpu usage per-worker as reported by ``psutil`` metrics.

**Occupancy**

The *Occupancy* tab shows the occupancy, in time, per worker. The total occupancy for a worker is the amount of time Dask expects it would take
to run all the tasks, and transfer any of their dependencies from other workers, *if the execution and transfers happened one-by-one*.
For example, if a worker has an occupancy of 10s, and it has 2 threads, you can expect it to take about 5s of wall-clock time for the worker
to complete all its tasks.

**Data Transfer**

The *Data Transfer* tab shows the size of open data transfers from/to other workers, per worker.

.. _dashboard.task-stream:

Task Stream
-----------

The task stream is a view of which tasks have been running on each thread of each worker. Each row represents a thread, and each rectangle represents
an individual task. The color for each rectangle corresponds to the task-prefix of the task being performed, and matches the color
of the :ref:`Progress plot <dashboard.progress>`. This means that all the individual tasks which are part of the ``inc`` task-prefix, for example, will have
the same color (which is chosen randomly from the viridis color map).

Note that when a new worker joins, it will get a new row, even if it's replacing a worker that recently left. So it's possible to temporarily
see more rows on the task stream than there are currently threads in the cluster, because both the history of the old worker and the new worker
will be displayed.

There are certain colors that are reserved for a specific kinds of operations:

.. raw:: html

    <table>
        <tr>
            <td>
                <div role="img" aria-label="light red square" style="color:rgba(255, 0, 0, 0.4); font-size: 25px ">&#9632;</div>
            </td>
            <td>Transferring data between workers.</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="light orange square" style="color: rgba(255,165,0, 0.4); font-size: 25px ">&#9632;</div>
            </td>
            <td>Reading from or writing to disk.</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="light grey square" style="color:rgba(128,128,128, 0.4); font-size: 25px ">&#9632;</div>
            </td>
            <td>Serializing/deserializing data.</td>
        </tr>
        <tr>
            <td>
                <div role="img" aria-label="black square" style="color:rgba(0, 0, 0, 1); font-size: 25px ">&#9632;</div>
            </td>
            <td>Erred tasks.</td>
        </tr>
    </table>


In some scenarios, the dashboard will have white spaces between each rectangle. During that time, the worker thread was idle.
Having too much white space is an indication of sub-optimal use of resources. Additionally, a lot of long red bars (transfers) can indicate
a performance problem, due to anything from too large of chunksizes, too complex of a graph, or even poor scheduling choices.

.. figure:: images/dashboard_taskstream_healthy.png
    :alt: The stacked bar chart, with one bar per worker-thread, has different shades of blue and green for different tasks, with occasional, very narrow red bars overlapping them.

    An example of a healthy Task Stream, with little to no white space. Transfers (red) are quick, and overlap with computation.

.. figure:: images/dashboard_task_stream_unhealthy.png
    :alt: The stacked bar chart, with one bar per worker-thread, is mostly empty (white). Each row only has a few occasional spurts of activity. There are also red and orange bars, some of which are long and don't overlap with the other colors.

    An example of an unhealthy Task Stream, with a lot of white space. Workers were idle most of the time. Additionally, there are some long transfers (red) which don't overlap with computation. We also see spilling to disk (orange).


.. _dashboard.progress:

Progress
--------

The progress bars plot shows the progress of each individual task-prefix. The color of each bar matches the color of the
individual tasks on the task stream from the same task-prefix. Each horizontal bar has four different components, from left to right:

.. raw:: html

    <ul style="list-style-type: none">
        <li>
            <span role="img" aria-label="light teal square" style="background:rgba(30,151,138, 0.6); width: 0.6em; height: 0.6em; border: 1px solid rgba(30,151,138, 0.6); display: inline-block"></span>
            <span>Tasks that have completed, are not needed anymore, and now have been released from memory.</span>
        </li>
        <li>
            <span role="img" aria-label="teal square" style="background:rgba(30,151,138, 1); width: 0.6em; height: 0.6em; border: 1px solid rgba(30,151,138, 1); display: inline-block"></span>
            <span> Tasks that have completed and are in memory.</span>
        </li>
        <li>
            <span role="img" aria-label="light grey square" style="background:rgba(128,128,128, 0.4); width: 0.6em; height: 0.6em; border: 1px solid rgba(128,128,128, 0.4); display: inline-block"></span>
            <span>Tasks that are ready to run.</span>
        </li>
        <li>
            <span role="img" aria-label="hashed light grey square" style="background-image: linear-gradient(135deg, rgba(128,128,128, 0.4) 25%, #ffffff 25%, #ffffff 50%, rgba(128,128,128, 0.4) 50%, rgba(128,128,128, 0.4) 75%, #ffffff 75%, #ffffff 100%); width: 0.6em; height: 0.6em; border: 1px solid rgba(128,128,128, 0.4); display: inline-block"></span>
            <span>Tasks that are <a href="https://distributed.dask.org/en/stable/scheduling-policies.html#queuing">queued</a>. They are ready to run, but not assigned to workers yet, so higher-priority tasks can run first.</span>
        </li>
        <li>
            <span role="img" aria-label="hashed red square" style="background-image: linear-gradient(135deg, rgba(255,0,0, 0.35) 20%, rgba(0,0,0, 0.35) 25%, rgba(0,0,0, 0.35) 50%, rgba(255,0,0, 0.35) 50%, rgba(255,0,0, 0.35) 75%, rgba(0,0,0, 0.35) 75%, rgba(0,0,0, 0.35) 100%); width: 0.6em; height: 0.6em; border: 1px solid rgba(128,128,128, 0.4); display: inline-block"></span>
            <span>Tasks that do not have a worker to run on due to <a href="https://distributed.dask.org/en/stable/locality.html#user-control">restrictions</a> or limited <a href="https://distributed.dask.org/en/stable/resources.html">resources</a>.</span>
        </li>
    </ul>

.. figure:: images/dashboard_progress.png
    :alt: Progress bar chart with one bar for each task-prefix matching with the names "add", "double", "inc", and "sum". The "double", "inc" and "add" bars have a progress of approximately one third of the total tasks, displayed in their individual color with different transparency levels. The "double" and "inc" bars have a striped grey background, and the "sum" bar is empty.


Dask JupyterLab Extension
--------------------------

The `JupyterLab Dask extension <https://github.com/dask/dask-labextension#dask-jupyterlab-extension>`__
allows you to embed Dask's dashboard plots directly into JupyterLab panes.

Once the JupyterLab Dask extension is installed you can choose any of the individual plots available and
integrated as a pane in your JupyterLab session. For example, in the figure below we selected the *Task Stream*,
*Progress*, *Workers Memory*, and *Graph* plots.

.. figure:: images/dashboard_jupyterlab.png
    :alt: Dask JupyterLab extension showing an arrangement of four panes selected from a display of plot options. The panes displayed are the Task stream, Bytes per worker, Progress and the Task Graph.
