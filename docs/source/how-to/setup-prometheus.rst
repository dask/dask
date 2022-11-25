Setup Prometheus monitoring
===========================

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of systems. Dask.distributed exposes
scheduler and worker metrics in a prometheus text based format. Metrics are available at ``http://scheduler-address:8787/metrics`` when the ``prometheus_client`` package has been installed.

.. _Prometheus: https://prometheus.io

Available metrics are as following

+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
|                   Metric name                   |                                   Description                                   | Scheduler | Worker |
+=================================================+=================================================================================+===========+========+
| ``python_gc_objects_collected_total``           | Objects collected during gc                                                     | Yes       | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``python_gc_objects_uncollectable_total``       | Uncollectable object found during GC                                            | Yes       | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``python_gc_collections_total``                 | Number of times this generation was collected                                   | Yes       | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``python_info``                                 | Python platform information                                                     | Yes       | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_clients``                      | Number of clients connected                                                     | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_desired_workers``              | Number of workers scheduler needs for task graph                                | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_workers``                      | Number of workers known by scheduler                                            | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_tasks``                        | Number of tasks known by scheduler                                              | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_tasks_suspicious``             | Total number of times a task has been marked suspicious                         | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_tasks_forgotten``              | Total number of processed tasks no longer in memory and already                 | Yes       |        |
|                                                 | removed from the scheduler job queue                                            |           |        |
|                                                 |                                                                                 |           |        |
|                                                 | **Note:** Task groups on the                                                    |           |        |
|                                                 | scheduler which have all tasks in the forgotten state are not included.         |           |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_scheduler_prefix_state_totals``          | Accumulated count of task prefix in each state                                  | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_max_leases``                   | Maximum leases allowed per semaphore                                            |           |        |
|                                                 |                                                                                 |           |        |
|                                                 | **Note:** This will be constant for each semaphore during its lifetime.         |           |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_active_leases``                | Amount of currently active leases per semaphore                                 | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_pending_leases``               | Amount of currently pending leases per semaphore                                | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_acquire_total``                | Total number of leases acquired per semaphore                                   | Yes       |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_release_total``                | Total number of leases released per semaphore                                   | Yes       |        |
|                                                 |                                                                                 |           |        |
|                                                 | **Note:** If a semaphore is closed while there are still leases active,         |           |        |
|                                                 | this count will not equal ``semaphore_acquired_total`` after execution.         |           |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_semaphore_average_pending_lease_time``   | Exponential moving average of the time it took to acquire a lease per semaphore | Yes       |        |
|                                                 |                                                                                 |           |        |
|                                                 | **Note:** This only includes time spent on scheduler side,                      |           |        |
|                                                 | it does not include time spent on communication.                                |           |        |
|                                                 |                                                                                 |           |        |
|                                                 | **Note:** This average is calculated based on order of leases                   |           |        |
|                                                 | instead of time of lease acquisition.                                           |           |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_tasks``                           | Number of tasks at worker                                                       |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_threads``                         | Number of worker threads                                                        |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_latency_seconds``                 | Latency of worker connection                                                    |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_memory_bytes``                    | Memory breakdown                                                                |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_incoming_bytes``         | Total size of open data transfers from other workers                            |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_incoming_count``         | Number of open data transfers from other workers                                |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_incoming_count_total``   | Total number of data transfers from other workers since the worker was started  |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_outgoing_bytes``         | Total size of open data transfers to other workers                              |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_outgoing_count``         | Number of open data transfers to other workers                                  |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_outgoing_count_total``   | Total number of data transfers to other workers since the worker was started    |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_tick_duration_median_seconds``    | Median tick duration at worker                                                  |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_task_duration_median_seconds``    | Median task runtime at worker                                                   |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_transfer_bandwidth_median_bytes`` | Bandwidth for transfer at worker                                                |           | Yes    |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
| ``dask_worker_concurrent_fetch_requests``       | **Deprecated:** This metric has been renamed to ``transfer_incoming_count``.    |           | Yes    |
|                                                 |                                                                                 |           |        |
|                                                 | Number of open fetch requests to other workers                                  |           |        |
+-------------------------------------------------+---------------------------------------------------------------------------------+-----------+--------+
