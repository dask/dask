.. _tls:


TLS/SSL
=======

Currently dask distributed has experimental support for TLS/SSL communication,
providing mutual authentication and encryption of communications between cluster
endpoints (Clients, Schedulers and Workers).

TLS is enabled by using a ``tls`` address such as ``tls://`` (the default
being ``tcp``, which sends data unauthenticated and unencrypted).  In
TLS mode, all cluster endpoints must present a valid TLS certificate
signed by a given Certificate Authority (CA).  It is generally recommended
to use a custom CA for your organization, as it will allow signing
certificates for internal hostnames or IP addresses.


Parameters
----------

When using TLS, one has to provide additional parameters:

* a *CA certificate(s) file*, which allows TLS to decide whether an
  endpoint's certificate has been signed by the correct authority;
* a *certificate file* for each endpoint, which is presented to other
  endpoints so as to achieve mutual authentication;
* a *private key file*, which is the cryptographic means to prove to
  other endpoints that you are the authorized user of a given certificate.

.. note::
   As per OpenSSL's requirements, all those files should be in PEM format.
   Also, it is allowed to concatenate the certificate and private key into
   a single file (you can then just specify the *certificate* parameter and
   leave the *private key* parameter absent).

It is up to you whether each endpoint uses a different certificate and
private key, or whether all endpoints share the same, or whether each
endpoint kind (Client, Scheduler, Worker) gets its own certificate / key pair.
Unless you have extraordinary requirements, however, the CA certificate
should probably be the same for all endpoints.

One can also pass additional parameters:

* a set of allowed *ciphers*, if you have strong requirements as to which
  algorithms are considered secure;  this setting's value should be an
  `OpenSSL cipher string <https://www.openssl.org/docs/man1.1.0/apps/ciphers.html>`_;
* whether to *require encryption*, to avoid using plain TCP communications
  by mistake.

All those parameters can be passed in several ways:

* through the Dask :ref:`configuration file <configuration>`;
* if using the command line, through options to ``dask-scheduler`` and
  ``dask-worker``;
* if using the API, through a ``Security`` object.  For example, here is
  how you might configure a ``Security`` object for client use:

  .. code-block:: python

     from distributed import Client
     from distributed.security import Security

     sec = Security(tls_ca_file='cluster_ca.pem',
                    tls_client_cert='cli_cert.pem',
                    tls_client_key='cli_key.pem',
                    require_encryption=True)

     client = Client(..., security=sec)


Security policy
---------------

Dask always verifies the certificate presented by a remote endpoint
against the configured CA certificate(s).  Certificates are verified
for both "client" and "server" endpoints (in the TCP sense), ensuring
the endpoints are mutually authenticated.  The hostname or IP address for
which a certificate has been issued is not checked; this should not be an
issue if you are using your own internal Certificate Authority.

It is not possible to disable certificate verification, as it would render
the communications vulnerable to Man-in-the-Middle attacks.


Performance implications
------------------------

Encryption is fast on recent CPUs, most of which have hardware acceleration
for AES-based encryption.  AES is normally selected by the TLS layer
unless you have forced the *ciphers* parameter to something else.  However,
encryption may still have a non-negligible overhead if you are transferring
very large data over very high speed network links.

.. seealso::
   `A study of AES-NI acceleration <https://calomel.org/aesni_ssl_performance.html>`_
   shows recent x86 CPUs can AES-encrypt more than 1 GB per second
   on each CPU core.
