from contextlib import contextmanager

try:
    import ssl
except ImportError:
    ssl = None

import pytest

from distributed.comm import connect, listen
from distributed.security import Security
from distributed.utils_test import get_cert

import dask

ca_file = get_cert("tls-ca-cert.pem")

cert1 = get_cert("tls-cert.pem")
key1 = get_cert("tls-key.pem")
keycert1 = get_cert("tls-key-cert.pem")

# Note this cipher uses RSA auth as this matches our test certs
FORCED_CIPHER = "ECDHE-RSA-AES128-GCM-SHA256"

TLS_13_CIPHERS = [
    "TLS_AES_128_GCM_SHA256",
    "TLS_AES_256_GCM_SHA384",
    "TLS_CHACHA20_POLY1305_SHA256",
    "TLS_AES_128_CCM_SHA256",
    "TLS_AES_128_CCM_8_SHA256",
]


def test_defaults():
    sec = Security()
    assert sec.require_encryption in (None, False)
    assert sec.tls_ca_file is None
    assert sec.tls_ciphers is None
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key is None
    assert sec.tls_scheduler_cert is None
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert is None


def test_constructor_errors():
    with pytest.raises(TypeError) as exc:
        Security(unknown_keyword="bar")

    assert "unknown_keyword" in str(exc.value)


def test_attribute_error():
    sec = Security()
    assert hasattr(sec, "tls_ca_file")
    with pytest.raises(AttributeError):
        sec.tls_foobar
    with pytest.raises(AttributeError):
        sec.tls_foobar = ""


def test_from_config():
    c = {
        "distributed.comm.tls.ca-file": "ca.pem",
        "distributed.comm.tls.scheduler.key": "skey.pem",
        "distributed.comm.tls.scheduler.cert": "scert.pem",
        "distributed.comm.tls.worker.cert": "wcert.pem",
        "distributed.comm.tls.ciphers": FORCED_CIPHER,
        "distributed.comm.require-encryption": True,
    }

    with dask.config.set(c):
        sec = Security()

    assert sec.require_encryption is True
    assert sec.tls_ca_file == "ca.pem"
    assert sec.tls_ciphers == FORCED_CIPHER
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == "skey.pem"
    assert sec.tls_scheduler_cert == "scert.pem"
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert == "wcert.pem"


def test_kwargs():
    c = {
        "distributed.comm.tls.ca-file": "ca.pem",
        "distributed.comm.tls.scheduler.key": "skey.pem",
        "distributed.comm.tls.scheduler.cert": "scert.pem",
    }
    with dask.config.set(c):
        sec = Security(
            tls_scheduler_cert="newcert.pem", require_encryption=True, tls_ca_file=None
        )
    assert sec.require_encryption is True
    assert sec.tls_ca_file is None
    assert sec.tls_ciphers is None
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == "skey.pem"
    assert sec.tls_scheduler_cert == "newcert.pem"
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert is None


def test_repr():
    sec = Security(tls_ca_file="ca.pem", tls_scheduler_cert="scert.pem")
    assert (
        repr(sec)
        == "Security(require_encryption=True, tls_ca_file='ca.pem', tls_scheduler_cert='scert.pem')"
    )


def test_tls_config_for_role():
    c = {
        "distributed.comm.tls.ca-file": "ca.pem",
        "distributed.comm.tls.scheduler.key": "skey.pem",
        "distributed.comm.tls.scheduler.cert": "scert.pem",
        "distributed.comm.tls.worker.cert": "wcert.pem",
        "distributed.comm.tls.ciphers": FORCED_CIPHER,
    }
    with dask.config.set(c):
        sec = Security()
    t = sec.get_tls_config_for_role("scheduler")
    assert t == {
        "ca_file": "ca.pem",
        "key": "skey.pem",
        "cert": "scert.pem",
        "ciphers": FORCED_CIPHER,
    }
    t = sec.get_tls_config_for_role("worker")
    assert t == {
        "ca_file": "ca.pem",
        "key": None,
        "cert": "wcert.pem",
        "ciphers": FORCED_CIPHER,
    }
    t = sec.get_tls_config_for_role("client")
    assert t == {
        "ca_file": "ca.pem",
        "key": None,
        "cert": None,
        "ciphers": FORCED_CIPHER,
    }
    with pytest.raises(ValueError):
        sec.get_tls_config_for_role("supervisor")


def assert_many_ciphers(ctx):
    assert len(ctx.get_ciphers()) > 2  # Most likely


def test_connection_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is False

    c = {
        "distributed.comm.tls.ca-file": ca_file,
        "distributed.comm.tls.scheduler.key": key1,
        "distributed.comm.tls.scheduler.cert": cert1,
        "distributed.comm.tls.worker.cert": keycert1,
    }
    with dask.config.set(c):
        sec = Security()

    d = sec.get_connection_args("scheduler")
    assert not d["require_encryption"]
    ctx = d["ssl_context"]
    basic_checks(ctx)
    assert_many_ciphers(ctx)

    d = sec.get_connection_args("worker")
    ctx = d["ssl_context"]
    basic_checks(ctx)
    assert_many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_connection_args("client")
    assert d.get("ssl_context") is None

    # With more settings
    c["distributed.comm.tls.ciphers"] = FORCED_CIPHER
    c["distributed.comm.require-encryption"] = True

    with dask.config.set(c):
        sec = Security()

    d = sec.get_listen_args("scheduler")
    assert d["require_encryption"]
    ctx = d["ssl_context"]
    basic_checks(ctx)

    supported_ciphers = ctx.get_ciphers()
    tls_12_ciphers = [c for c in supported_ciphers if "TLSv1.2" in c["description"]]
    assert len(tls_12_ciphers) == 1
    tls_13_ciphers = [c for c in supported_ciphers if "TLSv1.3" in c["description"]]
    assert len(tls_13_ciphers) in (0, 3)


def test_listen_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is False

    c = {
        "distributed.comm.tls.ca-file": ca_file,
        "distributed.comm.tls.scheduler.key": key1,
        "distributed.comm.tls.scheduler.cert": cert1,
        "distributed.comm.tls.worker.cert": keycert1,
    }
    with dask.config.set(c):
        sec = Security()

    d = sec.get_listen_args("scheduler")
    assert not d["require_encryption"]
    ctx = d["ssl_context"]
    basic_checks(ctx)
    assert_many_ciphers(ctx)

    d = sec.get_listen_args("worker")
    ctx = d["ssl_context"]
    basic_checks(ctx)
    assert_many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_listen_args("client")
    assert d.get("ssl_context") is None

    # With more settings
    c["distributed.comm.tls.ciphers"] = FORCED_CIPHER
    c["distributed.comm.require-encryption"] = True

    with dask.config.set(c):
        sec = Security()

    d = sec.get_listen_args("scheduler")
    assert d["require_encryption"]
    ctx = d["ssl_context"]
    basic_checks(ctx)

    supported_ciphers = ctx.get_ciphers()
    tls_12_ciphers = [c for c in supported_ciphers if "TLSv1.2" in c["description"]]
    assert len(tls_12_ciphers) == 1
    tls_13_ciphers = [c for c in supported_ciphers if "TLSv1.3" in c["description"]]
    assert len(tls_13_ciphers) in (0, 3)


@pytest.mark.asyncio
async def test_tls_listen_connect():
    """
    Functional test for TLS connection args.
    """

    async def handle_comm(comm):
        peer_addr = comm.peer_address
        assert peer_addr.startswith("tls://")
        await comm.write("hello")
        await comm.close()

    c = {
        "distributed.comm.tls.ca-file": ca_file,
        "distributed.comm.tls.scheduler.key": key1,
        "distributed.comm.tls.scheduler.cert": cert1,
        "distributed.comm.tls.worker.cert": keycert1,
    }
    with dask.config.set(c):
        sec = Security()

    c["distributed.comm.tls.ciphers"] = FORCED_CIPHER
    with dask.config.set(c):
        forced_cipher_sec = Security()

    async with listen(
        "tls://", handle_comm, **sec.get_listen_args("scheduler")
    ) as listener:
        comm = await connect(
            listener.contact_address, **sec.get_connection_args("worker")
        )
        msg = await comm.read()
        assert msg == "hello"
        comm.abort()

        # No SSL context for client
        with pytest.raises(TypeError):
            await connect(listener.contact_address, **sec.get_connection_args("client"))

        # Check forced cipher
        comm = await connect(
            listener.contact_address, **forced_cipher_sec.get_connection_args("worker")
        )
        cipher, _, _ = comm.extra_info["cipher"]
        assert cipher in [FORCED_CIPHER] + TLS_13_CIPHERS
        comm.abort()


@pytest.mark.asyncio
async def test_require_encryption():
    """
    Functional test for "require_encryption" setting.
    """

    async def handle_comm(comm):
        comm.abort()

    c = {
        "distributed.comm.tls.ca-file": ca_file,
        "distributed.comm.tls.scheduler.key": key1,
        "distributed.comm.tls.scheduler.cert": cert1,
        "distributed.comm.tls.worker.cert": keycert1,
    }
    with dask.config.set(c):
        sec = Security()

    c["distributed.comm.require-encryption"] = True
    with dask.config.set(c):
        sec2 = Security()

    for listen_addr in ["inproc://", "tls://"]:
        async with listen(
            listen_addr, handle_comm, **sec.get_listen_args("scheduler")
        ) as listener:
            comm = await connect(
                listener.contact_address, **sec2.get_connection_args("worker")
            )
            comm.abort()

        async with listen(
            listen_addr, handle_comm, **sec2.get_listen_args("scheduler")
        ) as listener:
            comm = await connect(
                listener.contact_address, **sec2.get_connection_args("worker")
            )
            comm.abort()

    @contextmanager
    def check_encryption_error():
        with pytest.raises(RuntimeError) as excinfo:
            yield
        assert "encryption required" in str(excinfo.value)

    for listen_addr in ["tcp://"]:
        async with listen(
            listen_addr, handle_comm, **sec.get_listen_args("scheduler")
        ) as listener:
            comm = await connect(
                listener.contact_address, **sec.get_connection_args("worker")
            )
            comm.abort()

            with pytest.raises(RuntimeError):
                await connect(
                    listener.contact_address, **sec2.get_connection_args("worker")
                )

        with pytest.raises(RuntimeError):
            listen(listen_addr, handle_comm, **sec2.get_listen_args("scheduler"))


def test_temporary_credentials():
    pytest.importorskip("cryptography")

    sec = Security.temporary()
    sec_repr = repr(sec)
    fields = ["tls_ca_file"]
    fields.extend(
        "tls_%s_%s" % (role, kind)
        for role in ["client", "scheduler", "worker"]
        for kind in ["key", "cert"]
    )
    for f in fields:
        val = getattr(sec, f)
        assert "\n" in val
        assert val not in sec_repr


@pytest.mark.asyncio
async def test_tls_temporary_credentials_functional():
    pytest.importorskip("cryptography")

    async def handle_comm(comm):
        peer_addr = comm.peer_address
        assert peer_addr.startswith("tls://")
        await comm.write("hello")
        await comm.close()

    sec = Security.temporary()

    async with listen(
        "tls://", handle_comm, **sec.get_listen_args("scheduler")
    ) as listener:
        comm = await connect(
            listener.contact_address, **sec.get_connection_args("worker")
        )
        msg = await comm.read()
        assert msg == "hello"
        comm.abort()
