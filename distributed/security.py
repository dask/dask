try:
    import ssl
except ImportError:
    ssl = None

import dask


__all__ = ("Security",)


class Security(object):
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

    def __init__(self, **kwargs):
        extra = set(kwargs).difference(self.__slots__)
        if extra:
            raise TypeError("Unknown parameters: %r" % sorted(extra))
        self._set_field(
            kwargs, "require_encryption", "distributed.comm.require-encryption"
        )
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

    def _set_field(self, kwargs, field, config_name):
        if field in kwargs:
            out = kwargs[field]
        else:
            out = dask.config.get(config_name)
        setattr(self, field, out)

    def __repr__(self):
        items = sorted((k, getattr(self, k)) for k in self.__slots__)
        return (
            "Security("
            + ", ".join("%s=%r" % (k, v) for k, v in items if v is not None)
            + ")"
        )

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
            ctx = ssl.create_default_context(purpose=purpose, cafile=tls["ca_file"])
            ctx.verify_mode = ssl.CERT_REQUIRED
            # We expect a dedicated CA for the cluster and people using
            # IP addresses rather than hostnames
            ctx.check_hostname = False
            ctx.load_cert_chain(tls["cert"], tls.get("key"))
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
