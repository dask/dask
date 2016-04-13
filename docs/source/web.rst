Web Interface
=============

.. image:: https://raw.githubusercontent.com/dask/dask-org/master/images/web-ui.gif
   :alt: Dask web interface
   :width: 40%
   :align: right

Information about the current state of the network helps to track progress,
identify performance issues, and debug failures.

Dask.distributed includes a web interface to help deliver this information over
a normal web page in real time.  This web interface is launched by default
wherever the scheduler is launched if the scheduler machine has Bokeh_
installed (``conda install bokeh``).  The web interface is normally available
at  ``http://scheduler-address:8787/status/`` and can be viewed any normal web
browser.

The web UI shows basic statistics on all worker machines, grouped by physical
address.  This includes information like CPU/memory load, active tasks,
latency, network and disk usage, etc..  The tabular statistics are updated
about once a second.

It also shows the progress of all groups of tasks currently running on the
cluster.  Dark blue is used for tasks that are completed and in memory, light
blue for tasks that are completed and have been released, gray for not yet
completed, and black for erred.  The progress bar is updated every 100ms.

There is a resource plot showing total CPU and memory use of the cluster over
the last few minutes.

Finally there is a plot of tasks as they complete, showing their start and end
times, start and end transfer times (in red), as well as which worker they were
run on.  Hovering over any of the tasks gives the task name as well as more
precise information.  This plot can be invaluable to determine performance
issues.  It is updated every 200ms.  It only includes the most recent thousand
tasks.  For the most recent 20000 tasks visit
http://my-scheduler-address:8787/tasks , although beware that this page is not
updated in real time.

.. raw:: html

   <iframe width="560"
           height="315"
           src="https://www.youtube.com/embed/V1uwKWqI5Xg?list=PLRtz5iA93T4PQvWuoMnIyEIz1fXiJ5Pri"
           frameborder="0" allowfullscreen></iframe>

.. _Bokeh: http://bokeh.pydata.org/en/latest/
