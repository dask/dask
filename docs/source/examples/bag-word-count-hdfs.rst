Word count
==========

In this example, we'll use ``dask`` to count the number of words in text files
(Enron email dataset, 6.4 GB) both locally and on a cluster (along with the
`distributed`_ and `hdfs3`_ libraries).

Local computation
-----------------

Download the first text file (76 MB) in the dataset to your local machine:

.. code-block:: bash

   $ wget https://s3.amazonaws.com/blaze-data/enron-email/edrm-enron-v2_allen-p_xml.zip/merged.txt

Import ``dask.bag`` and create a ``bag`` from the single text file:

.. code-block:: python

   >>> import dask.bag as db
   >>> b = db.read_text('merged.txt', blocksize=10000000)

View the first ten lines of the text file with ``.take()``:

.. code-block:: python

   >>> b.take(10)

   ('Date: Tue, 26 Sep 2000 09:26:00 -0700 (PDT)\r\n',
    'From: Phillip K Allen\r\n',
    'To: pallen70@hotmail.com\r\n',
    'Subject: Investment Structure\r\n',
    'X-SDOC: 948896\r\n',
    'X-ZLID: zl-edrm-enron-v2-allen-p-1713.eml\r\n',
    '\r\n',
    '---------------------- Forwarded by Phillip K Allen/HOU/ECT on 09/26/2000 \r\n',
    '04:26 PM ---------------------------\r\n',
    '\r\n')

We can write a word count expression using the ``bag`` methods to split the
lines into words, concatenate the nested lists of words into a single list,
count the frequencies of each word, then list the top 10 words by their count:

.. code-block:: python

   >>> wordcount = b.str.split().concat().frequencies().topk(10, lambda x: x[1])

Note that the combined operations in the previous expression are lazy. We can
trigger the word count computation using ``.compute()``:

.. code-block:: python

   >>> wordcount.compute()

   [('P', 288093),
    ('1999', 280917),
    ('2000', 277093),
    ('FO', 255844),
    ('AC', 254962),
    ('1', 240458),
    ('0', 233198),
    ('2', 224739),
    ('O', 223927),
    ('3', 221407)]

This computation required about 7 seconds to run on a laptop with 8 cores and 16
GB RAM.

Cluster computation with HDFS
-----------------------------

Next, we'll use ``dask`` along with the `distributed`_ and `hdfs3`_ libraries
to count the number of words in all of the text files stored in a Hadoop
Distributed File System (HDFS).

Copy the text data from Amazon S3 into HDFS on the cluster:

.. code-block:: bash

   $ hadoop distcp s3n://AWS_SECRET_ID:AWS_SECRET_KEY@blaze-data/enron-email hdfs:///tmp/enron

where ``AWS_SECRET_ID`` and ``AWS_SECRET_KEY`` are valid AWS credentials.

We can now start a ``distributed`` scheduler and workers on the cluster,
replacing ``SCHEDULER_IP`` and ``SCHEDULER_PORT`` with the IP address and port of
the ``distributed`` scheduler:

.. code-block:: bash

   $ dask-scheduler  # On the head node
   $ dask-worker SCHEDULER_IP:SCHEDULER_PORT --nprocs 4 --nthreads 1  # On the compute nodes

Because our computations use pure Python rather than numeric libraries (e.g.,
NumPy, pandas), we started the workers with multiple processes rather than
with multiple threads. This helps us avoid issues with the Python Global
Interpreter Lock (GIL) and increases efficiency.

In Python, import the ``hdfs3`` and the ``distributed`` methods used in this
example:

.. code-block:: python

   >>> from dask.distributed import Client, progress

Initialize a connection to the ``distributed`` executor:

.. code-block:: python

   >>> client = Client('SCHEDULER_IP:SCHEDULER_PORT')

Create a ``bag`` from the text files stored in HDFS. This expression will not
read data from HDFS until the computation is triggered:

.. code-block:: python

   >>> import dask.bag as db
   >>> b = db.read_text('hdfs:///tmp/enron/*/*')

We can write a word count expression using the same ``bag`` methods as the
local ``dask`` example:

.. code-block:: python

   >>> wordcount = b.str.split().concat().frequencies().topk(10, lambda x: x[1])

We are ready to count the number of words in all of the text files using
``distributed`` workers. We can map the ``wordcount`` expression to a future
that triggers the computation on the cluster.

.. code-block:: python

   >>> future = clinet.compute(wordcount)

Note that the ``compute`` operation is non-blocking, and you can continue to
work in the Python shell/notebook while the computations are running.

We can check the status of the ``future`` while all of the text files are being
processed:

.. code-block:: python

   >>> print(future)
   <Future: status: pending, key: finalize-0f2f51e2350a886223f11e5a1a7bc948>

   >>> progress(future)
   [########################################] | 100% Completed |  8min  15.2s

This computation required about 8 minutes to run on a cluster with three worker
machines, each with 4 cores and 16 GB RAM. For comparison, running the same
computation locally with ``dask`` required about 20 minutes on a single machine
with the same specs.

When the ``future`` finishes reading in all of the text files and counting
words, the results will exist on each worker. To sum the word counts for all of
the text files, we need to gather the results from the ``dask.distributed``
workers:

.. code-block:: python

   >>> results = client.gather(future)

Finally, we print the top 10 words from all of the text files:

.. code-block:: python

   >>> print(results)
   [('0', 67218227),
    ('the', 19588747),
    ('-', 14126955),
    ('to', 11893912),
    ('N/A', 11814994),
    ('of', 11725144),
    ('and', 10254267),
    ('in', 6685245),
    ('a', 5470711),
    ('or', 5227787)]

The complete Python script for this example is shown below:

.. code-block:: python

   # word-count.py

   # Local computation

   import dask.bag as db
   b = db.read_text('merged.txt')
   b.take(10)
   wordcount = b.str.split().concat().frequencies().topk(10, lambda x: x[1])
   wordcount.compute()

   # Cluster computation with HDFS

   from dask.distributed import Client, progress

   client = Client('SCHEDULER_IP:SCHEDULER_PORT')

   b = db.read_text('hdfs:///tmp/enron/*/*')
   wordcount = b.str.split().concat().frequencies().topk(10, lambda x: x[1])

   future = client.compute(wordcount)
   print(future)
   progress(future)

   results = client.gather(future)
   print(results)

.. _distributed: https://distributed.readthedocs.io/en/latest/
.. _hdfs3: https://hdfs3.readthedocs.io/en/latest/
