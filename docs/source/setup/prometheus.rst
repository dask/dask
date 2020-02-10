Prometheus Monitoring
-----------------------

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of systems. Dask.distributed exposes
scheduler and worker metrics in a prometheus text based format. Metrics are available at ``http://scheduler-address:8787/metrics``.

.. _Prometheus: https://prometheus.io

Available metrics are as following

+---------------------------------------------+------------------------------------------------+-----------+--------+
| Metric name                                 | Description                                    | Scheduler | Worker |
+=========================+===================+================================================+===========+========+
| python_gc_objects_collected_total           | Objects collected during gc.                   |    Yes    |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| python_gc_objects_uncollectable_total       | Uncollectable object found during GC.          |    Yes    |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| python_gc_collections_total                 | Number of times this generation was collected. |    Yes    |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| python_info                                 | Python platform information.                   |    Yes    |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_scheduler_workers                      | Number of workers connected.                   |    Yes    |        |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_scheduler_clients                      | Number of clients connected.                   |    Yes    |        |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_scheduler_tasks                        | Number of tasks at scheduler.                  |    Yes    |        |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_tasks                           | Number of tasks at worker.                     |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_connections                     | Number of task connections to other workers.   |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_threads                         | Number of worker threads.                      |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_latency_seconds                 | Latency of worker connection.                  |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_tick_duration_median_seconds    | Median tick duration at worker.                |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_task_duration_median_seconds    | Median task runtime at worker.                 |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+
| dask_worker_transfer_bandwidth_median_bytes | Bandwidth for transfer at worker in Bytes.     |           |  Yes   |
+---------------------------------------------+------------------------------------------------+-----------+--------+

