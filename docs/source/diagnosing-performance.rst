Diagnosing Performance
======================

Understanding the performance of a distributed computation can be difficult.
This is due in part to the many components of a distributed computer that may
impact performance:

1.  Compute time
2.  Memory bandwidth
3.  Network bandwidth
4.  Disk bandwidth
5.  Scheduler overhead
6.  Serialization costs

This difficulty is compounded because the information about these costs is
spread among many machines and so there is no central place to collect data to
identify performance issues.

Fortunately, Dask collects a variety of diagnostic information during
execution.  It does this both to provide performance feedback to users, but
also for its own internal scheduling decisions.  The primary place to observe
this feedback is the :doc:`diagnostic dashboard <web>`.  This document
describes the various pieces of performance information available and how to
access them.


Task start and stop times
-------------------------

Workers capture durations associated to tasks.  For each task that passes
through a worker we record start and stop times for each of the following:

1.  Serialization (gray)
2.  Dependency gathering from peers (red)
3.  Disk I/O to collect local data (orange)
4.  Execution times (colored by task)

The main way to observe these times is with the task stream plot on the
scheduler's ``/status`` page where the colors of the bars correspond to the
colors listed above.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/bokeh-task-stream.gif
   :alt: Dask task stream
   :width: 50%


Alternatively if you want to do your own diagnostics on every task event you
might want to create a :doc:`Scheduler plugin <plugins>`.  All of this
information will be available when a task transitions from processing to
memory or erred.


Statistical Profiling
---------------------

For single-threaded profiling Python users typically depend on the CProfile
module in the standard library (Dask developers recommend the `snakeviz
<https://jiffyclub.github.io/snakeviz/>`_ tool for single-threaded profiling).
Unfortunately the standard CProfile module does not work with multi-threaded or
distributed computations.

To address this Dask implements its own distributed `statistical profiler
<https://en.wikipedia.org/wiki/Profiling_(computer_programming)#Statistical_profilers>`_.
Every 10ms each worker process checks what each of its worker threads are
doing.  It captures the call stack and adds this stack to a counting data
structure.  This counting data structure is recorded and cleared every second
in order to establish a record of performance over time.

Users typically observe this data through the ``/profile`` plot on either the
worker or scheduler diagnostic dashboards.  On the scheduler page they observe
the total profile aggregated over all workers over all threads.  Clicking on
any of the bars in the profile will zoom the user into just that section, as is
typical with most profiling tools.  There is a timeline at the bottom of the
page to allow users to select different periods in time.

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/daskboard-profile.gif
   :alt: Dask profiler
   :width: 70%

Profiles are also grouped by the task that was being run at the time.  You can
select a task name from the selection menu at the top of the page.  You can
also click on the rectangle corresponding to the task in the main task stream
plot on the ``/status`` page.

Users can also query this data directly using the :doc:`Client.profile <api>`
function.  This will deliver the raw data structure used to produce these
plots.  They can also pass a filename to save the plot as an HTML file
directly.  Note that this file will have to be served from a webserver like
``python -m http.server`` to be visible.

The 10ms and 1s parameters can be controlled by the ``profile-interval`` and
``profile-cycle-interval`` entries in the config.yaml file.


Bandwidth
---------

Dask workers track every incoming and outgoing transfer in the
``Worker.outgoing_transfer_log`` and ``Worker.incoming_transfer_log``
attributes including

1.  Total bytes transferred
2.  Compressed bytes transferred
3.  Start/stop times
4.  Keys moved
5.  Peer

These are made available to users through the ``/main`` page of the Worker's
diagnostic dashboard.  You can capture their state explicitly by running a
command on the workers:

.. code-block:: python

   client.run(lambda dask_worker: dask_worker.outgoing_transfer_log)
   client.run(lambda dask_worker: dask_worker.incoming_transfer_log)


A note about times
------------------

Different computers maintain different clocks which may not match perfectly.
To address this the Dask scheduler sends its current time in response to every
worker heartbeat.  Workers compare their local time against this time to obtain
an estimate of differences.  All times recorded in workers take this estimated
delay into account.  This helps, but still, imprecise measurements may exist.

All times are intended to be from the scheduler's perspective.
