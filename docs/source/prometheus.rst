.. When modifying the contents of this page, please adjust the corresponding page in the dask.distributed documentation accordingly.

Prometheus
==========

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of
systems. A distributed cluster offers a number of Prometheus metrics if the
prometheus_client_ package is installed. The metrics are exposed in Prometheus'
text-based format at the ``/metrics`` endpoint on both schedulers and workers.


Available metrics
-----------------

Apart from the metrics exposed per default by the prometheus_client_, schedulers and
workers expose a number of Dask-specific metrics.
See the `dask.distributed documentation
<https://distributed.dask.org/en/latest/prometheus.html>`_ for details.


.. _Prometheus: https://prometheus.io
.. _prometheus_client: https://github.com/prometheus/client_python
