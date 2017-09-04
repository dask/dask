Submitting Applications
=======================

The ``dask-submit`` cli can be used to submit an application to the dask cluster
running remotely. If your code depends on resources that can only be access
from cluster running dask, ``dask-submit`` provides a mechanism to send the script
to the cluster for execution from a different machine.

For example, S3 buckets could not be visible from your local machine and hence any
attempt to create a dask graph from local machine may not work.


Submitting dask Applications with `dask-submit`
-----------------------------------------------

In order to remotely submit scripts to the cluster from a local machine or a CI/CD
environment, we need to run a remote client on the same machine as the scheduler::

   #scheduler machine
   dask-remote --port 8788


After making sure the `dask-remote` is running, you can submit a script by::

   #local machine
   dask-submit <dask-remote-address>:<port> <script.py>


Some of the commonly used arguments are:

-  ``REMOTE_CLIENT_ADDRESS``: host name where dask-remote client is running
-  ``FILEPATH``: Local path to file containing dask application

For example, given the following dask application saved in a file called
``script.py``:

.. code-block:: python

   from distributed import Client

   def inc(x):
        return x + 1

   if __name__=='__main__':
        client = Client('127.0.0.1:8786')
        x = client.submit(inc, 10)
        print(x.result())


We can submit this application from a local machine by running::

   dask-submit <remote-client-address>:<port> script.py
