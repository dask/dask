Setup Prometheus monitoring
===========================

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of systems. 
A distributed cluster offers a number of Prometheus metrics if the prometheus_client_ package is installed.
The metrics are exposed in Prometheus' text-based format at the ``/metrics`` endpoint on both schedulers and workers.

.. _Prometheus: https://prometheus.io
.. _prometheus_client: https://github.com/prometheus/client_python

Available metrics
-----------------

Apart from the metrics exposed per default by the ``prometheus_client``, schedulers and workers expose a number of Dask-specific metrics.


Scheduler metrics
^^^^^^^^^^^^^^^^^

The scheduler exposes the following metrics about itself:

+--------------------------------------------------+-------------------------------------------------------------------------+
|                   Metric name                    |                               Description                               |
+==================================================+=========================================================================+
| ``dask_scheduler_clients``                       | Number of clients connected                                             |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_desired_workers``               | Number of workers scheduler needs for task graph                        |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_workers``                       | Number of workers known by scheduler                                    |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_tasks``                         | Number of tasks known by scheduler                                      |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_tasks_suspicious_total``        | Total number of times a task has been marked suspicious                 |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_tasks_forgotten_total``         | Total number of processed tasks no longer in memory and already         |
|                                                  | removed from the scheduler job queue                                    |
|                                                  |                                                                         |
|                                                  | **Note:** Task groups on the                                            |
|                                                  | scheduler which have all tasks in the forgotten state are not included. |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_prefix_state_total``            | Accumulated count of task prefix in each state                          |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_tick_duration_maximum_seconds`` | Maximum tick duration observed since Prometheus last scraped metrics    |
+--------------------------------------------------+-------------------------------------------------------------------------+
| ``dask_scheduler_tick_count_total``              | Total number of ticks observed since the server started                 |
+--------------------------------------------------+-------------------------------------------------------------------------+


Semaphore metrics
^^^^^^^^^^^^^^^^^

The following metrics about semaphores are available on the scheduler:

+-----------------------------------------------+---------------------------------------------------------------------------------+
|                  Metric name                  |                                   Description                                   |
+===============================================+=================================================================================+
| ``dask_semaphore_max_leases``                 | Maximum leases allowed per semaphore                                            |
|                                               |                                                                                 |
|                                               | **Note:** This will be constant for each semaphore during its lifetime.         |
+-----------------------------------------------+---------------------------------------------------------------------------------+
| ``dask_semaphore_active_leases``              | Amount of currently active leases per semaphore                                 |
+-----------------------------------------------+---------------------------------------------------------------------------------+
| ``dask_semaphore_pending_leases``             | Amount of currently pending leases per semaphore                                |
+-----------------------------------------------+---------------------------------------------------------------------------------+
| ``dask_semaphore_acquire_total``              | Total number of leases acquired per semaphore                                   |
+-----------------------------------------------+---------------------------------------------------------------------------------+
| ``dask_semaphore_release_total``              | Total number of leases released per semaphore                                   |
|                                               |                                                                                 |
|                                               | **Note:** If a semaphore is closed while there are still leases active,         |
|                                               | this count will not equal ``semaphore_acquired_total`` after execution.         |
+-----------------------------------------------+---------------------------------------------------------------------------------+
| ``dask_semaphore_average_pending_lease_time`` | Exponential moving average of the time it took to acquire a lease per semaphore |
|                                               |                                                                                 |
|                                               | **Note:** This only includes time spent on scheduler side,                      |
|                                               | it does not include time spent on communication.                                |
|                                               |                                                                                 |
|                                               | **Note:** This average is calculated based on order of leases                   |
|                                               | instead of time of lease acquisition.                                           |
+-----------------------------------------------+---------------------------------------------------------------------------------+


Work-stealing metrics
^^^^^^^^^^^^^^^^^^^^^

If ``work-stealing`` is enabled, the scheduler exposes these metrics:


+---------------------------------------+-----------------------------------+
|              Metric name              |            Description            |
+=======================================+===================================+
| ``dask_stealing_request_count_total`` | Total number of stealing requests |
+---------------------------------------+-----------------------------------+
| ``dask_stealing_request_cost_total``  | Total cost of stealing requests   |
+---------------------------------------+-----------------------------------+


Worker metrics
^^^^^^^^^^^^^^

The worker exposes these metrics about itself:

+-----------------------------------------------+--------------------------------------------------------------------------------+
|                  Metric name                  |                                  Description                                   |
+===============================================+================================================================================+
| ``dask_worker_tasks``                         | Number of tasks at worker                                                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_threads``                       | Number of worker threads                                                       |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_latency_seconds``               | Latency of worker connection                                                   |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_memory_bytes``                  | Memory breakdown                                                               |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_incoming_bytes``       | Total size of open data transfers from other workers                           |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_incoming_count``       | Number of open data transfers from other workers                               |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_incoming_count_total`` | Total number of data transfers from other workers since the worker was started |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_outgoing_bytes``       | Total size of open data transfers to other workers                             |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_outgoing_count``       | Number of open data transfers to other workers                                 |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_transfer_outgoing_count_total`` | Total number of data transfers to other workers since the worker was started   |
+-----------------------------------------------+--------------------------------------------------------------------------------+
| ``dask_worker_concurrent_fetch_requests``     | **Deprecated:** This metric has been renamed to ``transfer_incoming_count``.   |
|                                               |                                                                                |
|                                               | Number of open fetch requests to other workers                                 |
+-----------------------------------------------+--------------------------------------------------------------------------------+

If the crick_ package is installed, the worker additionally exposes:

.. _crick: https://github.com/dask/crick

+-------------------------------------------------+----------------------------------+
|                   Metric name                   |           Description            |
+=================================================+==================================+
| ``dask_worker_tick_duration_median_seconds``    | Median tick duration at worker   |
+-------------------------------------------------+----------------------------------+
| ``dask_worker_task_duration_median_seconds``    | Median task runtime at worker    |
+-------------------------------------------------+----------------------------------+
| ``dask_worker_transfer_bandwidth_median_bytes`` | Bandwidth for transfer at worker |
+-------------------------------------------------+----------------------------------+
