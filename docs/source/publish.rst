Publish Datasets
================

A *published dataset* is a named reference to a Dask collection or list of
futures that has been published to the cluster.  It is available for any client
to see and persists beyond the scope of an individual session.

Publishing datasets is useful in the following cases:

*  You want to share computations with colleagues
*  You want to persist results on the cluster between interactive sessions

Motivating Example
------------------

In this example we load a dask.dataframe from S3, manipulate it, and then
publish the result.

**Connect and Load**

.. code-block:: python

   from dask.distributed import Client
   client = Client('scheduler-address:8786')

   import dask.dataframe as dd
   df = dd.read_csv('s3://my-bucket/*.csv')
   df2 = df[df.balance < 0]
   df2 = client.persist(df2)

   >>> df2.head()
         name  balance
   0    Alice     -100
   1      Bob     -200
   2  Charlie     -300
   3   Dennis     -400
   4    Edith     -500

**Publish**

To share this collection with a colleague we publish it under the name
``'negative_accounts'``

.. code-block:: python

   client.publish_dataset(negative_accounts=df2)

**Load published dataset from different client**

Now any other client can connect to the scheduler and retrieve this published
dataset.

.. code-block:: python

   >>> from dask.distributed import Client
   >>> client = Client('scheduler-address:8786')

   >>> client.list_datasets()
   ['negative_accounts']

   >>> df = client.get_dataset('negative_accounts')
   >>> df.head()
         name  balance
   0    Alice     -100
   1      Bob     -200
   2  Charlie     -300
   3   Dennis     -400
   4    Edith     -500

This allows users to easily share results.  It also allows for the persistence
of important and commonly used datasets beyond a single session.  Published
datasets continue to reside in distributed memory even after all clients
requesting them have disconnected.

Notes
-----

Published collections are not automatically persisted.  If you publish an
un-persisted collection then others will still be able to get the collection
from the scheduler, but operations on that collection will start from scratch.
This allows you to publish views on data that do not permanently take up
cluster memory but can be surprising if you expect "publishing" to
automatically make a computed dataset rapidly available.

Any client can publish or unpublish a dataset.

Publishing too many large datasets can quickly consume a cluster's RAM.

API
---

.. currentmodule:: distributed.client

.. autosummary::
   Client.publish_dataset
   Client.list_datasets
   Client.get_dataset
   Client.unpublish_dataset
