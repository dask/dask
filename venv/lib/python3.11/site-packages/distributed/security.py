from __future__ import annotations

import datetime
import os
import ssl
import tempfile
import warnings

import dask
from dask.widgets import get_template

__all__ = ("Security",)


if ssl.OPENSSL_VERSION_INFO >= (1, 1, 0, 7):
    # The OP_NO_SSL* and OP_NO_TLS* become deprecated in favor of
    # 'SSLContext.minimum_version' from Python 3.7 onwards, however
    # this attribute is not available unless the ssl module is compiled
    # with OpenSSL 1.1.0g or newer.
    # https://docs.python.org/3.10/library/ssl.html#ssl.SSLContext.minimum_version
    # https://docs.python.org/3.7/library/ssl.html#ssl.SSLContext.minimum_version

    # these _set_mimimun_version and _set_maximum_version depend on the validation
    # already performed in `Security._set_tls_version_field`,
    # and that they only apply to freshly created ssl.SSLContext instances in
    # _get_tls_context
    def _set_minimum_version(ctx: ssl.SSLContext, version: ssl.TLSVersion) -> None:
        ctx.minimum_version = version

    def _set_maximum_version(ctx: ssl.SSLContext, version: ssl.TLSVersion) -> None:
        ctx.maximum_version = version

else:

    def _set_minimum_version(ctx: ssl.SSLContext, version: ssl.TLSVersion) -> None:
        # if the ctx.maximum_version attribute is unsupported then we can infer
        # that TLS 1.3 is not supported.
        # _set_tls_version_field enforces that version is TLSVersion.TLSv1_2,
        # or TLSVersion.TLSv1_3
        if version is not ssl.TLSVersion.TLSv1_2:
            raise ValueError(f"Unsupported TLS/SSL version: {version!r}")
        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

    def _set_maximum_version(ctx: ssl.SSLContext, version: ssl.TLSVersion) -> None:
        # if the ctx.maximum_version attribute is unsupported then we can infer
        # that TLSv1_3 is not supported.
        # _set_tls_version_field enforces that version is TLSVersion.TLSv1_2,
        # TLSVersion.TLSv1_3, or None
        # _get_tls_context enforces that version is not None
        if version is not ssl.TLSVersion.TLSv1_2:
            raise ValueError(f"Unsupported TLS/SSL version: {version!r}")


class Security:
    """Security configuration for a Dask cluster.

    Default values are loaded from Dask's configuration files, and can be
    overridden in the constructor.

    Parameters
    ----------
    require_encryption : bool, optional
        Whether TLS encryption is required for all connections.
    tls_ca_file : str, optional
        Path to a CA certificate file encoded in PEM format.
    tls_ciphers : str, optional
        An OpenSSL cipher string of allowed ciphers. If not provided, the
        system defaults will be used.
    tls_min_version : ssl.TLSVersion, optional
        The minimum TLS version to support. Defaults to TLS 1.2.
    tls_max_version : ssl.TLSVersion, optional
        The maximum TLS version to support. Defaults to the maximum version
        supported.
    tls_client_cert : str, optional
        Path to a certificate file for the client, encoded in PEM format.
    tls_client_key : str, optional
        Path to a key file for the client, encoded in PEM format.
        Alternatively, the key may be appended to the cert file, and this
        parameter be omitted.
    tls_scheduler_cert : str, optional
        Path to a certificate file for the scheduler, encoded in PEM format.
    tls_scheduler_key : str, optional
        Path to a key file for the scheduler, encoded in PEM format.
        Alternatively, the key may be appended to the cert file, and this
        parameter be omitted.
    tls_worker_cert : str, optional
        Path to a certificate file for a worker, encoded in PEM format.
    tls_worker_key : str, optional
        Path to a key file for a worker, encoded in PEM format.
        Alternatively, the key may be appended to the cert file, and this
        parameter be omitted.
    extra_conn_args : mapping, optional
        Mapping with keyword arguments to pass down to connections.
    """

    __slots__ = (
        "require_encryption",
        "tls_ca_file",
        "tls_ciphers",
        "tls_min_version",
        "tls_max_version",
        "tls_client_key",
        "tls_client_cert",
        "tls_scheduler_key",
        "tls_scheduler_cert",
        "tls_worker_key",
        "tls_worker_cert",
        "extra_conn_args",
    )

    def __init__(self, require_encryption=None, **kwargs):
        if ssl.OPENSSL_VERSION_INFO < (1, 1, 1):
            warnings.warn(
                f"support for {ssl.OPENSSL_VERSION} is deprecated,"
                " and will be removed in a future release",
                DeprecationWarning,
            )
        extra = set(kwargs).difference(self.__slots__)
        if extra:
            raise TypeError("Unknown parameters: %r" % sorted(extra))
        self.extra_conn_args = kwargs.pop("extra_conn_args", {})
        if require_encryption is None:
            require_encryption = dask.config.get("distributed.comm.require-encryption")
        if require_encryption is None:
            require_encryption = bool(kwargs)
        self.require_encryption = require_encryption
        self._set_field(kwargs, "tls_ciphers", "distributed.comm.tls.ciphers")
        self._set_tls_version_field(
            kwargs,
            "tls_min_version",
            "distributed.comm.tls.min-version",
            ssl.TLSVersion.TLSv1_2,
        )
        self._set_tls_version_field(
            kwargs,
            "tls_max_version",
            "distributed.comm.tls.max-version",
        )
        self._set_field(kwargs, "tls_ca_file", "distributed.comm.tls.ca-file")
        self._set_field(kwargs, "tls_client_key", "distributed.comm.tls.client.key")
        self._set_field(kwargs, "tls_client_cert", "distributed.comm.tls.client.cert")
        self._set_field(
            kwargs, "tls_scheduler_key", "distributed.comm.tls.scheduler.key"
        )
        self._set_field(
            kwargs, "tls_scheduler_cert", "distributed.comm.tls.scheduler.cert"
        )
        self._set_field(kwargs, "tls_worker_key", "distributed.comm.tls.worker.key")
        self._set_field(kwargs, "tls_worker_cert", "distributed.comm.tls.worker.cert")

    @classmethod
    def temporary(cls, **kwargs):
        """Create a new temporary Security object.

        This creates a new self-signed key/cert pair suitable for securing
        communication for all roles in a Dask cluster. These keys/certs exist
        only in memory, and are stored in this object.

        This method requires the library ``cryptography`` be installed.
        """
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import rsa
            from cryptography.x509.oid import NameOID
        except ImportError:
            raise ImportError(
                "Using `Security.temporary` requires `cryptography`, please "
                "install it using either pip or conda"
            )
        key = rsa.generate_private_key(
            public_exponent=65537, key_size=2048, backend=default_backend()
        )
        key_contents = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

        dask_internal = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, "dask-internal")]
        )
        altnames = x509.SubjectAlternativeName([x509.DNSName("dask-internal")])
        now = datetime.datetime.now(tz=datetime.timezone.utc).replace(tzinfo=None)
        cert = (
            x509.CertificateBuilder()
            .subject_name(dask_internal)
            .issuer_name(dask_internal)
            .add_extension(altnames, critical=False)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=365))
            .sign(key, hashes.SHA256(), default_backend())
        )

        cert_contents = cert.public_bytes(serialization.Encoding.PEM).decode()

        return cls(
            require_encryption=True,
            tls_ca_file=cert_contents,
            tls_client_key=key_contents,
            tls_client_cert=cert_contents,
            tls_scheduler_key=key_contents,
            tls_scheduler_cert=cert_contents,
            tls_worker_key=key_contents,
            tls_worker_cert=cert_contents,
            **kwargs,
        )

    def _set_field(self, kwargs, field, config_name):
        if field in kwargs:
            val = kwargs[field]
        else:
            val = dask.config.get(config_name)
        setattr(self, field, val)

    def _set_tls_version_field(self, kwargs, field, config_name, default=None):
        if field in kwargs:
            val = kwargs[field]
            valid = {None, ssl.TLSVersion.TLSv1_2, ssl.TLSVersion.TLSv1_3}
            if val not in valid:
                raise ValueError(
                    f"{field}={val!r} is not supported, expected one of {list(valid)}"
                )
            if val is None:
                val = default
        else:
            valid = {
                None: default,
                1.2: ssl.TLSVersion.TLSv1_2,
                1.3: ssl.TLSVersion.TLSv1_3,
            }
            val = dask.config.get(config_name)
            if val in valid:
                val = valid[val]
            else:
                raise ValueError(
                    f"{config_name}={val!r} is not supported, expected one of {list(valid)}"
                )

        setattr(self, field, val)

    def _attr_to_dict(self):
        keys = sorted(self.__slots__)
        keys.remove("extra_conn_args")

        attr = {}

        for k in keys:
            val = getattr(self, k)
            if val is not None:
                if isinstance(val, str) and "\n" in val:
                    attr[k] = "Temporary (In-memory)"
                elif isinstance(val, str):
                    attr[k] = f"Local ({os.path.abspath(val)})"
                else:
                    attr[k] = val

        return attr

    def __repr__(self):
        attr = self._attr_to_dict()
        return (
            "Security("
            + ", ".join(f"{key}={value}" for key, value in attr.items())
            + ")"
        )

    def _repr_html_(self):
        return get_template("security.html.j2").render(security=self._attr_to_dict())

    def get_tls_config_for_role(self, role):
        """
        Return the TLS configuration for the given role, as a flat dict.
        """
        if role not in {"client", "scheduler", "worker"}:
            raise ValueError(f"unknown role {role!r}")
        return {
            "ca_file": self.tls_ca_file,
            "ciphers": self.tls_ciphers,
            "cert": getattr(self, "tls_%s_cert" % role),
            "key": getattr(self, "tls_%s_key" % role),
        }

    def _get_tls_context(self, tls, purpose):
        if tls.get("ca_file") and tls.get("cert"):
            ca = tls["ca_file"]
            cert_path = cert = tls["cert"]
            key_path = key = tls.get("key")

            if "\n" in ca:
                ctx = ssl.create_default_context(purpose=purpose, cadata=ca)
            else:
                ctx = ssl.create_default_context(purpose=purpose, cafile=ca)

            # the _set_tls_version_field method enforces that
            # self.tls_min_version is TLSv1_2, or TLSv1_3
            _set_minimum_version(ctx, self.tls_min_version)
            if self.tls_max_version is not None:
                _set_maximum_version(ctx, self.tls_max_version)

            cert_in_memory = "\n" in cert
            key_in_memory = key is not None and "\n" in key
            if cert_in_memory or key_in_memory:
                with tempfile.TemporaryDirectory() as tempdir:
                    if cert_in_memory:
                        cert_path = os.path.join(tempdir, "dask.crt")
                        with open(cert_path, "w") as f:
                            f.write(cert)
                    if key_in_memory:
                        key_path = os.path.join(tempdir, "dask.pem")
                        with open(key_path, "w") as f:
                            f.write(key)
                    ctx.load_cert_chain(cert_path, key_path)
            else:
                ctx.load_cert_chain(cert_path, key_path)

            # Bidirectional authentication
            ctx.verify_mode = ssl.CERT_REQUIRED

            # We expect a dedicated CA for the cluster and people using
            # IP addresses rather than hostnames
            ctx.check_hostname = False

            ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT

            if tls.get("ciphers"):
                ctx.set_ciphers(tls.get("ciphers"))

            return ctx

    def get_connection_args(self, role):
        """
        Get the *connection_args* argument for a connect() call with
        the given *role*.
        """
        tls = self.get_tls_config_for_role(role)
        return {
            "ssl_context": self._get_tls_context(tls, ssl.Purpose.SERVER_AUTH),
            "require_encryption": self.require_encryption,
            "extra_conn_args": self.extra_conn_args,
        }

    def get_listen_args(self, role):
        """
        Get the *connection_args* argument for a listen() call with
        the given *role*.
        """
        tls = self.get_tls_config_for_role(role)
        return {
            "ssl_context": self._get_tls_context(tls, ssl.Purpose.CLIENT_AUTH),
            "require_encryption": self.require_encryption,
        }
