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

Dask uses `fsspec`_ for local, cluster and remote data IO. Other file interaction, such
as loading of configuration, is done using ordinary python method.

The following remote services are well supported and tested against the main
codebase:

- **Local or Network File System**: ``file://`` - the local file system, default in the absence of any protocol.

- **Hadoop File System**: ``hdfs://`` - Hadoop Distributed File System, for resilient, replicated
  files within a cluster. This uses PyArrow_ as the backend.

- **Amazon S3**: ``s3://`` - Amazon S3 remote binary store, often used with Amazon EC2,
  using the library s3fs_.

- **Google Cloud Storage**: ``gcs://`` or ``gs://`` - Google Cloud Storage, typically used with Google Compute
  resource using gcsfs_.

- **Microsoft Azure Storage**: ``adl://``, ``abfs://`` or ``az://`` - Microsoft Azure Storage using adlfs_.

- **HTTP(s)**: ``http://`` or ``https://`` for reading data directly from HTTP web servers.

`fsspec`_ also provides other file systems that may be of interest to Dask users, such as
ssh, ftp, webhdfs and dropbox. See the documentation for more information.

When specifying a storage location, a URL should be provided using the general
form ``protocol://path/to/data``.  If no protocol is provided, the local
file system is assumed (same as ``file://``).

.. _fsspec: https://filesystem-spec.readthedocs.io/
.. _s3fs: https://s3fs.readthedocs.io/
.. _adlfs: https://github.com/dask/adlfs
.. _gcsfs: https://gcsfs.readthedocs.io/en/latest/
.. _PyArrow: https://arrow.apache.org/docs/python/

Lower-level details on how Dask handles remote data is described
below in the Internals section

Optional Parameters
-------------------

Two methods exist for passing parameters to the backend file system driver:
extending the URL to include username, password, server, port, etc.; and
providing ``storage_options``, a dictionary of parameters to pass on. The
second form is more general, as any number of file system-specific options
can be passed.

Examples:

.. code-block:: python

   df = dd.read_csv('hdfs://user@server:port/path/*.csv')

   df = dd.read_parquet('s3://bucket/path',
                        storage_options={'anon': True, 'use_ssl': False})

Details on how to provide configuration for the main back-ends
are listed next, but further details can be found in the documentation pages of the
relevant back-end.

Each back-end has additional installation requirements and may not be available
at runtime. The dictionary ``fsspec.registry`` contains the
currently imported file systems. To see which backends ``fsspec`` knows how
to import, you can do

.. code-block:: python

    from fsspec.registry import known_implementations
    known_implementations

Note that some backends appear twice, if they can be referenced with multiple
protocol strings, like "http" and "https".

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
Spark. HDFS support can be provided by PyArrow_.

By default, the back-end attempts to read the default server and port from
local Hadoop configuration files on each node, so it may be that no
configuration is required. However, the server, port, and user can be passed as
part of the url: ``hdfs://user:pass@server:port/path/to/data``, or using the
``storage_options=`` kwarg.


Extra Configuration for PyArrow
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The following additional options may be passed to the ``PyArrow`` driver via
``storage_options``:

    - ``host``, ``port``, ``user``: Basic authentication
    - ``kerb_ticket``: Path to kerberos ticket cache

PyArrow's ``libhdfs`` driver can also be affected by a few environment
variables. For more information on these, see the `PyArrow documentation`_.

.. _PyArrow documentation: https://arrow.apache.org/docs/python/filesystems_deprecated.html#hadoop-file-system-hdfs


Amazon S3
---------

Amazon S3 (Simple Storage Service) is a web service offered by Amazon Web
Services.

The S3 back-end available to Dask is s3fs_, and is importable when Dask is
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
.. _here: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#shared-credentials-file
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

- use ``anon`` for public data
- use ``cloud`` if this is available
- use `gcloud`_ to generate a JSON file, and distribute this to all workers, and
  supply the path to the file

- use gcsfs directly with the ``browser`` method to generate a token cache file
  (``~/.gcs_tokens``) and distribute this to all workers, thereafter using method ``cache``

.. _gcloud: https://cloud.google.com/sdk/docs/

A final suggestion is shown below, which may be the fastest and simplest for authenticated access (as
opposed to anonymous), since it will not require re-authentication. However, this method
is not secure since credentials will be passed directly around the cluster. This is fine if
you are certain that the cluster is itself secured. You need to create a ``GCSFileSystem`` object
using any method that works for you and then pass its credentials directly:

.. code-block:: python

    gcs = GCSFileSystem(...)
    dask_function(..., storage_options={'token': gcs.session.credentials})

Microsoft Azure Storage
-----------------------

Microsoft Azure Storage is comprised of Data Lake Storage (Gen1) and Blob Storage (Gen2).
These are identified by the protocol identifiers ``adl`` and ``abfs``, respectively,
provided by the adlfs_ back-end.

Authentication for ``adl`` requires ``tenant_id``, ``client_id`` and ``client_secret``
in the ``storage_options`` dictionary.

Authentication for ``abfs`` requires ``account_name`` and ``account_key`` in ``storage_options``.

HTTP(S)
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

Using a block size of 0 will return normal ``requests`` streaming file-like objects,
which are stable, but provide no random access.

Developer API
-------------

The prototype for any file system back-end can be found in
``fsspec.spec.AbstractFileSystem``. Any new implementation should provide the
same API, or directly subclass, and make itself available as a protocol to Dask. For example, the
following would register the protocol "myproto", described by the implementation
class ``MyProtoFileSystem``. URLs of the form ``myproto://`` would thereafter
be dispatched to the methods of this class:

.. code-block:: python

   fsspec.registry['myproto'] = MyProtoFileSystem

However, it would be better to submit a PR to ``fsspec`` to include the class in
the ``known_implementations``.

Internals
---------

Dask contains internal tools for extensible data ingestion in the
``dask.bytes`` package and using `fsspec`_.
.  *These functions are developer-focused rather than for
direct consumption by users.  These functions power user facing functions like*
``dd.read_csv`` *and* ``db.read_text`` *which are probably more useful for most
users.*


.. currentmodule:: dask.bytes

.. autosummary::
   read_bytes
   open_files

These functions are extensible in their output formats (bytes, file objects),
their input locations (file system, S3, HDFS), line delimiters, and compression
formats.

Both functions are *lazy*, returning either
point to blocks of bytes (``read_bytes``) or open file objects
(``open_files``).  They can handle different storage backends by prepending
protocols like ``s3://`` or ``hdfs://`` (see below). They handle compression formats
listed in ``fsspec.compression``, some of which may require additional packages
to be installed.

These functions are not used for all data sources.  Some data sources like HDF5
are quite particular and receive custom treatment.

Delimiters
^^^^^^^^^^

The ``read_bytes`` function takes a path (or globstring of paths) and produces
a sample of the first file and a list of delayed objects for each of the other
files.  If passed a delimiter such as ``delimiter=b'\n'``, it will ensure that
the blocks of bytes start directly after a delimiter and end directly before a
delimiter.  This allows other functions, like ``pd.read_csv``, to operate on
these delayed values with expected behavior.

These delimiters are useful both for typical line-based formats (log files,
CSV, JSON) as well as other delimited formats like Avro, which may separate
logical chunks by a complex sentinel string. Note that the delimiter finding
algorithm is simple, and will not account for characters that are escaped,
part of a UTF-8 code sequence or within the quote marks of a string.

Compression
^^^^^^^^^^^

These functions support widely available compression technologies like ``gzip``,
``bz2``, ``xz``, ``snappy``, and ``lz4``.  More compressions can be easily
added by inserting functions into dictionaries available in the
``fsspec.compression`` module.  This can be done at runtime and need not be
added directly to the codebase.

However, most compression technologies like ``gzip`` do not support efficient
random access, and so are useful for streaming ``open_files`` but not useful for
``read_bytes`` which splits files at various points.

API
^^^

.. autofunction:: read_bytes
.. autofunction:: open_files
