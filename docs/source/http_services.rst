HTTP endpoints
==============

A subset of the following pages will be available from the scheduler or
workers of a running cluster. The list of currently available endpoints can
be found by examining ``/sitemap.json``.


Main dashboard links
--------------------


Dynamic bokeh pages. The root redirects to /status, and each page links to the
others via a header navbar.

- ``/status``: entry point to the dashboards, shows cluster-wide memory and tasks
- ``/workers``: currently connected workers and their CPU/memory usage
- ``/tasks``: task block view with longer look-back than on /status
- ``/system``: global stats for the cluster, CPU, memory, bandwidth, file descriptors
- ``/profile``: flamegraph statistical profiling across the cluster
- ``/graph``: currently processing graphs in a dependnecy tree view
- ``/info``: redirect to ``/info/main/workers.html``

Scheduler HTTP
--------------

Pages and JSON endpoints served by the scheduler

- ``/health``: check server is alive
- ``/info/main/workers.html`` basic info about workers and links to their dashboards and logs
- ``info/worker/(worker).html``: more detail about given worker, keyed by TCP address; links to tasks
- ``info/task/(task).html``: details about a task on the cluster, by dask key; links to worker,
            related tasks, and client
- ``/info/call-stacks/(worker).html``: tasks currently handled by given worker
- ``/info/call-stack/(task).html``: state of task (where it is running)
- ``/info/main/logs.html``: scheduler logs
- ``/info/logs/(worker).html``: logs of given worker
- ``/individual-plots.json``: map of path to description for available individual
            (i.e,. one-pane, non-dashboard) plots
- ``/eventstream``: scheduler events on a websocket
- ``/proxy/(port)/(address)/(path): proxy to worker HTTP locations
            (if you have jupyter-server-proxy)
- ``/metrics``: prometheus endpoint
- ``/json/counts.json``: cluster count stats
- ``/json/identity.json``: scheduler information
- ``/json/index.html``: link to the above two endpoints
- ``/sitemap.json``: list of available endpoints
- ``/statics/()``: static file content (CSS, etc)
- ``/stealing``: worker occupancy metrics, to evaluate task stealing

Individual bokeh plots
----------------------

- ``/individual-task-stream``
- ``/individual-progress``
- ``/individual-graph``
- ``/individual-profile``
- ``/individual-profile-server``
- ``/individual-nbytes``
- ``/individual-cpu``
- ``/individual-nprocessing``
- ``/individual-workers``
- ``/individual-bandwidth-types``
- ``/individual-bandwidth-workers``
- ``/individual-memory-by-key``
- ``/individual-gpu-memory`` (GPU only)
- ``/individual-gpu-utilization`` (GPU only)

Worker HTTP
-----------

- ``/status``: 
- ``/counters``: 
- ``/crossfilter``:
- ``/sitemap.json``: list of available endpoints
- ``/system``: 
- ``/health``: check server is alive
- ``/metrics``: prometheus endpoint
- ``/statics/()``: static file content (CSS, etc)
