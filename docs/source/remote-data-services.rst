Remote Data
===========

Dask can read data from a variety of data stores including local file systems,
network file systems, cloud object stores, and Hadoop.  Typically this is done
by prepending a protocol like ``"s3://"`` to paths used in common data access
functions like ``dd.read_csv``:

.. code-block:: python

   import dask.dataframe as dd
   df = dd.read_csv('s3://bucket/path/to/data-*.csv')
   df = dd.read_parquet('gcs://bucket/path/to/data-*.parq')

   import dask.bag as db
   b = db.read_text('hdfs://path/to/*.json').map(json.loads)

The following remote services are well supported and tested against the main
codebase:

- **Local or Network File System**: ``file://`` - the local file system, default in the absence of any protocol

- **Hadoop File System**: ``hdfs://`` - Hadoop Distributed File System, for resilient, replicated
  files within a cluster. Can use either hdfs3_ or pyarrow_.

- **Amazon S3**: ``s3://`` - Amazon S3 remote binary store, often used with Amazon EC2,
  using the library s3fs_

- **Google Cloud Storage**: ``gcs://`` or ``gs:`` - Google Cloud Storage, typically used with Google Compute
  resource using gcsfs_ (in development)

- **HTTP(s)**: ``http://`` or ``https://`` for reading data directly from HTTP web servers

- **Azure Datalake Storage**: ``adl://``, for use with the Microsoft
  Azure platform, using azure-data-lake-store-python_. This is *experimental*, use at your own risk.

When specifying a storage location, a URL should be provided using the general
form ``protocol://path/to/data``.  If no protocol is provided, the local
file system is assumed (same as ``file://``).

.. _hdfs3: https://hdfs3.readthedocs.io/
.. _s3fs: https://s3fs.readthedocs.io/
.. _azure-data-lake-store-python: https://github.com/Azure/azure-data-lake-store-python
.. _gcsfs: https://github.com/dask/gcsfs/
.. _pyarrow: https://arrow.apache.org/docs/python/

Lower-level details on how Dask handles remote data is described in Section
:doc:`Internal Data Ingestion <bytes>`.

Optional Parameters
-------------------

Two methods exist for passing parameters to the backend file system driver:
extending the URL to include username, password, server, port, etc.; and
providing ``storage_options``, a dictionary of parameters to pass on. Examples:

.. code-block:: python

   df = dd.read_csv('hdfs://user@server:port/path/*.csv')

   df = dd.read_parquet('s3://bucket/path',
                        storage_options={'anon': True, 'use_ssl': False})

Further details on how to provide configuration for each back-end
is listed next.

Each back-end has additional installation requirements and may not be available
at runtime. The dictionary ``dask.bytes.core._filesystems`` contains the
currently available file systems. Some require appropriate imports before use.

The following list gives the protocol shorthands and the back-ends to which
they refer.


Local File System
-----------------

Local files are always accessible, and all parameters passed as part of the URL
(beyond the path itself) or with the ``storage_options``
dictionary will be ignored.

This is the default back-end, and the one used if no protocol is passed at all.

We assume here that each worker has access to the same file system - either
the workers are co-located on the same machine, or a network file system
is mounted and referenced at the same path location for every worker node.

Locations specified relative to the current working directory will, in
general, be respected (as they would be with the built-in python ``open``),
but this may fail in the case that the client and worker processes do not
necessarily have the same working directory.

Hadoop File System
------------------

The Hadoop File System (HDFS) is a widely deployed, distributed, data-local file
system written in Java. This file system backs many clusters running Hadoop and
Spark.

HDFS support can be provided by either hdfs3_ or pyarrow_, defaulting to the
first library installed in that order. To explicitly set which library to use,
set ``hdfs_driver`` using ``dask.config.set``:

.. code-block:: python

   # Use first available option in [hdfs3, pyarrow]
    dd.read_csv('hdfs:///path/to/*.csv')

   # Use hdfs3 for HDFS I/O
   with dask.config.set(hdfs_driver='hdfs3'):
       dd.read_csv('hdfs:///path/to/*.csv')

   # Use pyarrow for HDFS I/O
   with dask.config.set(hdfs_driver='pyarrow'):
       dd.read_csv('hdfs:///path/to/*.csv')

   # Set pyarrow as the global hdfs driver
   dask.config.set(hdfs_driver='pyarrow')


By default, both libraries attempt to read the default server and port from
local Hadoop configuration files on each node, so it may be that no
configuration is required. However, the server, port, and user can be passed as
part of the url: ``hdfs://user:pass@server:port/path/to/data``.

Extra Configuration for HDFS3
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following additional options may be passed to the ``hdfs3`` driver via
``storage_options``:

    - ``host``, ``port``, ``user``: Basic authentication
    - ``ticket_cache``, ``token``: Kerberos authentication
    - ``pars``: Dictionary of further parameters (e.g., for `high availability`_)

The ``hdfs3`` driver can also be affected by a few environment variables. For more
information on these, see the `hdfs3 documentation`_.

.. _high availability: https://hdfs3.readthedocs.io/en/latest/hdfs.html#high-availability-mode
.. _hdfs3 documentation: https://hdfs3.readthedocs.io/en/latest/hdfs.html#defaults

Extra Configuration for PyArrow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following additional options may be passed to the ``pyarrow`` driver via
``storage_options``:

    - ``host``, ``port``, ``user``: Basic authentication
    - ``kerb_ticket``: Path to kerberos ticket cache

PyArrow's ``libhdfs`` driver can also be affected by a few environment
variables. For more information on these, see the `pyarrow documentation`_.

.. _pyarrow documentation: https://arrow.apache.org/docs/python/filesystems.html#hadoop-file-system-hdfs


Amazon S3
---------

Amazon S3 (Simple Storage Service) is a web service offered by Amazon Web
Services.

The S3 back-end available to Dask is s3fs, and is importable when Dask is
imported.

Authentication for S3 is provided by the underlying library boto3. As described
in the `auth docs`_, this could be achieved by placing credentials files in one
of several locations on each node: ``~/.aws/credentials``, ``~/.aws/config``,
``/etc/boto.cfg``, and ``~/.boto``. Alternatively, for nodes located
within Amazon EC2, IAM roles can be set up for each node, and then no further
configuration is required. The final authentication option for user
credentials can be passed directly in the URL
(``s3://keyID:keySecret/bucket/key/name``) or using ``storage_options``. In
this case, however, the key/secret will be passed to all workers in-the-clear,
so this method is only recommended on well-secured networks.

.. _auth docs: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html

The following parameters may be passed to s3fs using ``storage_options``:

    - anon: Whether access should be anonymous (default False)

    - key, secret: For user authentication

    - token: If authentication has been done with some other S3 client

    - use_ssl: Whether connections are encrypted and secure (default True)

    - client_kwargs: Dict passed to the `boto3 client`_, with keys such
      as `region_name` or `endpoint_url`. Notice: do not pass the `config`
      option here, please pass it's content to `config_kwargs` instead.

    - config_kwargs: Dict passed to the `s3fs.S3FileSystem`_, which passes it to
      the `boto3 client's config`_ option.

    - requester_pays: Set True if the authenticated user will assume transfer
      costs, which is required by some providers of bulk data

    - default_block_size, default_fill_cache: These are not of particular
      interest to Dask users, as they concern the behaviour of the buffer
      between successive reads

    - kwargs: Other parameters are passed to the `boto3 Session`_ object,
      such as `profile_name`, to pick one of the authentication sections from
      the configuration files referred to above (see `here`_)

.. _boto3 client: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
.. _boto3 Session: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
.. _here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#shared-credentials-file
.. _s3fs.S3FileSystem: https://s3fs.readthedocs.io/en/latest/api.html#s3fs.core.S3FileSystem
.. _boto3 client's config: https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html

Using Other S3-Compatible Services
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

By using the `endpoint_url` option, you may use other s3-compatible services,
for example, using `AlibabaCloud OSS`:

.. code-block:: python

    dask_function(...,
        storage_options={
            "key": ...,
            "secret": ...,
            "client_kwargs": {
                "endpoint_url": "http://some-region.some-s3-compatible.com",
            },
            # this dict goes to boto3 client's `config`
            #   `addressing_style` is required by AlibabaCloud, other services may not
            "config_kwargs": {"s3": {"addressing_style": "virtual"}},
        })

Google Cloud Storage
--------------------

Google Cloud Storage is a RESTful online file storage web service for storing
and accessing data on Google's infrastructure.

The GCS back-end is identified by the
protocol identifiers ``gcs`` and ``gs``, which are identical in their effect.

Multiple modes of authentication are supported. These options should be
included in the ``storage_options`` dictionary as ``{'token': ..}`` submitted with your call
to a storage-based Dask function/method. See the `gcsfs`_ documentation for further
details.


General recommendations for distributed clusters, in order:

- use 'anon' for public data
- use 'cloud' if this is available
- use `gcloud`_ to generate a JSON file, and distribute this to all workers, and 
  supply the path to the file

- use gcsfs directly with the 'browser' method to generate a token cache file
  (``~/.gcs_tokens``) and distribute this with method 'cache'

.. _gcloud: https://cloud.google.com/sdk/docs/

The final suggestion may be the fastest and simplest for authenticated access (as
opposed to anonymous), since it will not require re-authentication. However, this method
is not secure since credentials will be passed directly around the cluster. This is fine if
you are certain that the cluster is itself secured. You need to create a ``GCSFileSystem`` object
using any method that works for you and then pass its credentials directly:

.. code-block:: python

    gcs = GCSFileSystem(...)
    dask_function(..., storage_options={'token': gcs.session.credentials})

Azure Datalake
--------------

.. warning::

    Support for ADL is experimental - use at own risk.

Parameters ``tenant_id``, ``client_id``, and ``client_secret`` are required for
authentication in ``storage_options=``,
and all other parameters will be passed on to the AzureDLFileSystem_ constructor
(follow the link for further information). The auth parameters are passed directly to
workers, so this should only be used within a secure cluster.

.. _AzureDLFileSystem: https://azure-datalake-store.readthedocs.io/en/latest/api.html#azure.datalake.store.core.AzureDLFileSystem


HTTP(s)
-------

Direct file-like access to arbitrary URLs is available over HTTP and HTTPS. However,
there is no such thing as ``glob`` functionality over HTTP, so only explicit lists
of files can be used.

Server implementations differ in the information they provide - they may or may
not specify the size of a file via a HEAD request or at the start of a download -
and some servers may not respect byte range requests. The HTTPFileSystem therefore
offers best-effort behaviour: the download is streamed but, if more data is seen
than the configured block-size, an error will be raised. To be able to access such
data you must read the whole file in one shot (and it must fit in memory).

Note that, currently, ``http://`` and ``https://`` are treated as separate protocols,
and cannot be mixed.

Developer API
-------------

The prototype for any file system back-end can be found in
``bytes.local.LocalFileSystem``. Any new implementation should provide the
same API and make itself available as a protocol to Dask. For example, the
following would register the protocol "myproto", described by the implementation
class ``MyProtoFileSystem``. URLs of the form ``myproto://`` would thereafter
be dispatched to the methods of this class:

.. code-block:: python

   dask.bytes.core._filesystems['myproto'] = MyProtoFileSystem

For a more complicated example, users may wish to also see
``dask.bytes.s3.DaskS3FileSystem``.

.. currentmodule:: dask.bytes.local

.. autoclass:: LocalFileSystem
   :members:
