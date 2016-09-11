Word count in HDFS
==================

Setup
-----

In this example, we'll use ``distributed`` with the ``hdfs3`` library to count
the number of words in text files (Enron email dataset, 6.4 GB) stored in HDFS.

Copy the text data from Amazon S3 into HDFS on the cluster:

.. code-block:: bash

   $ hadoop distcp s3n://AWS_SECRET_ID:AWS_SECRET_KEY@blaze-data/enron-email hdfs:///tmp/enron

where ``AWS_SECRET_ID`` and ``AWS_SECRET_KEY`` are valid AWS credentials.

Start the ``distributed`` scheduler and workers on the cluster.

Code example
------------

Import ``distributed``, ``hdfs3``, and other standard libraries used in this example:

.. code-block:: python

   >>> import hdfs3
   >>> from collections import defaultdict, Counter
   >>> from distributed import Client, progress

Initialize a connection to HDFS, replacing ``NAMENODE_HOSTNAME`` and
``NAMENODE_PORT`` with the hostname and port (default: 8020) of the HDFS
namenode.

.. code-block:: python

   >>> hdfs = hdfs3.HDFileSystem('NAMENODE_HOSTNAME', port=NAMENODE_PORT)

Initialize a connection to the ``distributed`` client, replacing
``SCHEDULER_IP`` and ``SCHEDULER_PORT`` with the IP address and port of the
``distributed`` scheduler.

.. code-block:: python

   >>> client = Client('SCHEDULER_IP:SCHEDULER_PORT')

Generate a list of filenames from the text data in HDFS:

.. code-block:: python

   >>> filenames = hdfs.glob('/tmp/enron/*/*')
   >>> print(filenames[:5])

   ['/tmp/enron/edrm-enron-v2_nemec-g_xml.zip/merged.txt',
    '/tmp/enron/edrm-enron-v2_ring-r_xml.zip/merged.txt',
    '/tmp/enron/edrm-enron-v2_bailey-s_xml.zip/merged.txt',
    '/tmp/enron/edrm-enron-v2_fischer-m_xml.zip/merged.txt',
    '/tmp/enron/edrm-enron-v2_geaccone-t_xml.zip/merged.txt']

Print the first 1024 bytes of the first text file:

.. code-block:: python

   >>> print(hdfs.head(filenames[0]))

   b'Date: Wed, 29 Nov 2000 09:33:00 -0800 (PST)\r\nFrom: Xochitl-Alexis Velasc
   o\r\nTo: Mark Knippa, Mike D Smith, Gerald Nemec, Dave S Laipple, Bo Barnwel
   l\r\nCc: Melissa Jones, Iris Waser, Pat Radford, Bonnie Shumaker\r\nSubject:
    Finalize ECS/EES Master Agreement\r\nX-SDOC: 161476\r\nX-ZLID: zl-edrm-enro
   n-v2-nemec-g-2802.eml\r\n\r\nPlease plan to attend a meeting to finalize the
    ECS/EES  Master Agreement \r\ntomorrow 11/30/00 at 1:30 pm CST.\r\n\r\nI wi
   ll email everyone tomorrow with location.\r\n\r\nDave-I will also email you
   the call in number tomorrow.\r\n\r\nThanks\r\nXochitl\r\n\r\n***********\r\n
   EDRM Enron Email Data Set has been produced in EML, PST and NSF format by ZL
    Technologies, Inc. This Data Set is licensed under a Creative Commons Attri
   bution 3.0 United States License <http://creativecommons.org/licenses/by/3.0
   /us/> . To provide attribution, please cite to "ZL Technologies, Inc. (http:
   //www.zlti.com)."\r\n***********\r\nDate: Wed, 29 Nov 2000 09:40:00 -0800 (P
   ST)\r\nFrom: Jill T Zivley\r\nTo: Robert Cook, Robert Crockett, John Handley
   , Shawna'

Create a function to count words in each file:

.. code-block:: python

   >>> def count_words(fn):
   ...     word_counts = defaultdict(int)
   ...     with hdfs.open(fn) as f:
   ...         for line in f:
   ...             for word in line.split():
   ...                 word_counts[word] += 1
   ...     return word_counts

Before we process all of the text files using the distributed workers, let's
test our function locally by counting the number of words in the first text
file:

.. code-block:: python

   >>> counts = count_words(filenames[0])
   >>> print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   [(b'the', 144873),
    (b'of', 98122),
    (b'to', 97202),
    (b'and', 90575),
    (b'or', 60305),
    (b'in', 53869),
    (b'a', 43300),
    (b'any', 31632),
    (b'by', 31515),
    (b'is', 30055)]

We can perform the same operation of counting the words in the first text file,
except we will use ``client.submit`` to execute the computation on a ``distributed``
worker:

.. code-block:: python

   >>> future = client.submit(count_words, filenames[0])
   >>> counts = future.result()
   >>> print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   [(b'the', 144873),
    (b'of', 98122),
    (b'to', 97202),
    (b'and', 90575),
    (b'or', 60305),
    (b'in', 53869),
    (b'a', 43300),
    (b'any', 31632),
    (b'by', 31515),
    (b'is', 30055)]

We are ready to count the number of words in all of the text files using
``distributed`` workers. Note that the ``map`` operation is non-blocking, and
you can continue to work in the Python shell/notebook while the computations
are running.

.. code-block:: python

   >>> futures = client.map(count_words, filenames)

We can check the status of some ``futures`` while all of the text files are
being processed:

.. code-block:: python

   >>> len(futures)

   161

   >>> futures[:5]

   [<Future: status: finished, key: count_words-5114ab5911de1b071295999c9049e941>,
    <Future: status: pending, key: count_words-d9e0d9daf6a1eab4ca1f26033d2714e7>,
    <Future: status: pending, key: count_words-d2f365a2360a075519713e9380af45c5>,
    <Future: status: pending, key: count_words-bae65a245042325b4c77fc8dde1acf1e>,
    <Future: status: pending, key: count_words-03e82a9b707c7e36eab95f4feec1b173>]

   >>> progress(futures)

   [########################################] | 100% Completed |  3min  0.2s

When the ``futures`` finish reading in all of the text files and counting
words, the results will exist on each worker. This operation required about
3 minutes to run on a cluster with three worker machines, each with 4 cores
and 16 GB RAM.

Note that because the previous computation is bound by the GIL in Python, we
can speed it up by starting the ``distributed`` workers with the ``--nprocs 4``
option.

To sum the word counts for all of the text files, we need to gather some
information from the ``distributed`` workers. To reduce the amount of data
that we gather from the workers, we can define a function that only returns the
top 10,000 words from each text file.

.. code-block:: python

   >>> def top_items(d):
   ...     items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:10000]
   ...     return dict(items)

We can then ``map`` the futures from the previous step to this culling
function. This is a convenient way to construct a pipeline of computations
using futures:

.. code-block:: python

   >>> futures2 = client.map(top_items, futures)

We can ``gather`` the resulting culled word count data for each text file to
the local process:

.. code-block:: python

   >>> results = client.gather(iter(futures2))

To sum the word counts for all of the text files, we can iterate over the
results in ``futures2`` and update a local dictionary that contains all of the
word counts.

.. code-block:: python

   >>> all_counts = Counter()
   >>> for result in results:
   ...     all_counts.update(result)

Finally, we print the total number of words in the results and the words with
the highest frequency from all of the text files:

.. code-block:: python

   >>> print(len(all_counts))

   8797842

   >>> print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   [(b'0', 67218380),
    (b'the', 19586868),
    (b'-', 14123768),
    (b'to', 11893464),
    (b'N/A', 11814665),
    (b'of', 11724827),
    (b'and', 10253753),
    (b'in', 6684937),
    (b'a', 5470371),
    (b'or', 5227805)]

The complete Python script for this example is shown below:

.. code-block:: python

   # word-count.py

   import hdfs3
   from collections import defaultdict, Counter
   from distributed import Client, progress

   hdfs = hdfs3.HDFileSystem('NAMENODE_HOSTNAME', port=NAMENODE_PORT)
   client = Client('SCHEDULER_IP:SCHEDULER:PORT')

   filenames = hdfs.glob('/tmp/enron/*/*')
   print(filenames[:5])
   print(hdfs.head(filenames[0]))


   def count_words(fn):
       word_counts = defaultdict(int)
       with hdfs.open(fn) as f:
           for line in f:
               for word in line.split():
                   word_counts[word] += 1
       return word_counts

   counts = count_words(filenames[0])
   print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   future = client.submit(count_words, filenames[0])
   counts = future.result()
   print(sorted(counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])

   futures = client.map(count_words, filenames)
   len(futures)
   futures[:5]
   progress(futures)


   def top_items(d):
       items = sorted(d.items(), key=lambda kv: kv[1], reverse=True)[:10000]
       return dict(items)

   futures2 = client.map(top_items, futures)
   results = client.gather(iter(futures2))

   all_counts = Counter()
   for result in results:
       all_counts.update(result)

   print(len(all_counts))

   print(sorted(all_counts.items(), key=lambda k_v: k_v[1], reverse=True)[:10])
