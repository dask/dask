import datetime
import tempfile
import os

try:
    import ssl
except ImportError:
    ssl = None

import dask


__all__ = ("Security",)


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
    """

    __slots__ = (
        "require_encryption",
        "tls_ca_file",
        "tls_ciphers",
        "tls_client_key",
        "tls_client_cert",
        "tls_scheduler_key",
        "tls_scheduler_cert",
        "tls_worker_key",
        "tls_worker_cert",
    )

    def __init__(self, require_encryption=None, **kwargs):
        extra = set(kwargs).difference(self.__slots__)
        if extra:
            raise TypeError("Unknown parameters: %r" % sorted(extra))
        if require_encryption is None:
            require_encryption = dask.config.get("distributed.comm.require-encryption")
        if require_encryption is None:
            require_encryption = not not kwargs
        self.require_encryption = require_encryption
        self._set_field(kwargs, "tls_ciphers", "distributed.comm.tls.ciphers")
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
    def temporary(cls):
        """Create a new temporary Security object.

        This creates a new self-signed key/cert pair suitable for securing
        communication for all roles in a Dask cluster. These keys/certs exist
        only in memory, and are stored in this object.

        This method requires the library ``cryptography`` be installed.
        """
        try:
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import hashes
            from cryptography.hazmat.primitives import serialization
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
        now = datetime.datetime.utcnow()
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
        )

    def _set_field(self, kwargs, field, config_name):
        if field in kwargs:
            out = kwargs[field]
        else:
            out = dask.config.get(config_name)
        setattr(self, field, out)

    def __repr__(self):
        keys = sorted(self.__slots__)
        items = []
        for k in keys:
            val = getattr(self, k)
            if val is not None:
                if isinstance(val, str) and "\n" in val:
                    items.append((k, "..."))
                else:
                    items.append((k, repr(val)))
        return "Security(" + ", ".join("%s=%s" % (k, v) for k, v in items) + ")"

    def get_tls_config_for_role(self, role):
        """
        Return the TLS configuration for the given role, as a flat dict.
        """
        if role not in {"client", "scheduler", "worker"}:
            raise ValueError("unknown role %r" % (role,))
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
