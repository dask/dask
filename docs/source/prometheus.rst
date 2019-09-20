Prometheus Monitoring
-----------------------

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of systems. Dask.distributed exposes
scheduler and worker metrics in a prometheus text based format. Metrics are available at ``http://scheduler-address:8787/metrics``.

.. _Prometheus: https://prometheus.io

Available metrics are as following

+---------------------------------------------+----------------------------------------------+
| Metric name                                 | Description                                  |
+=========================+===================+==============================================+
| dask_scheduler_workers                      | Number of workers connected.                 |
+---------------------------------------------+----------------------------------------------+
| dask_scheduler_clients                      | Number of clients connected.                 |
+---------------------------------------------+----------------------------------------------+
| dask_scheduler_received_tasks               | Number of tasks received at scheduler        |
+---------------------------------------------+----------------------------------------------+
| dask_scheduler_unrunnable_tasks             | Number of unrunnable tasks at scheduler      |
+---------------------------------------------+----------------------------------------------+
| dask_worker_tasks                           | Number of tasks at worker.                   |
+---------------------------------------------+----------------------------------------------+
| dask_worker_connections                     | Number of task connections to other workers. |
+---------------------------------------------+----------------------------------------------+
| dask_worker_threads                         | Number of worker threads.                    |
+---------------------------------------------+----------------------------------------------+
| dask_worker_latency_seconds                 | Latency of worker connection.                |
+---------------------------------------------+----------------------------------------------+
| dask_worker_tick_duration_median_seconds    | Median tick duration at worker.              |
+---------------------------------------------+----------------------------------------------+
| dask_worker_task_duration_median_seconds    | Median task runtime at worker.               |
+---------------------------------------------+----------------------------------------------+
| dask_worker_transfer_bandwidth_median_bytes | Bandwidth for transfer at worker in Bytes.   |
+---------------------------------------------+----------------------------------------------+

