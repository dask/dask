from __future__ import print_function, division, absolute_import

from contextlib import contextmanager
import sys

try:
    import ssl
except ImportError:
    ssl = None

import pytest
from tornado import gen

from distributed.comm import connect, listen
from distributed.security import Security
from distributed.utils_test import new_config, get_cert, gen_test


ca_file = get_cert('tls-ca-cert.pem')

cert1 = get_cert('tls-cert.pem')
key1 = get_cert('tls-key.pem')
keycert1 = get_cert('tls-key-cert.pem')

# Note this cipher uses RSA auth as this matches our test certs
FORCED_CIPHER = 'ECDHE-RSA-AES128-GCM-SHA256'


def test_defaults():
    with new_config({}):
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


def test_attribute_error():
    sec = Security()
    assert hasattr(sec, 'tls_ca_file')
    with pytest.raises(AttributeError):
        sec.tls_foobar
    with pytest.raises(AttributeError):
        sec.tls_foobar = ""


def test_from_config():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
            },
            'worker': {
                'cert': 'wcert.pem',
            },
            'ciphers': FORCED_CIPHER,
        },
        'require-encryption': True,
    }
    with new_config(c):
        sec = Security()
    assert sec.require_encryption is True
    assert sec.tls_ca_file == 'ca.pem'
    assert sec.tls_ciphers == FORCED_CIPHER
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == 'skey.pem'
    assert sec.tls_scheduler_cert == 'scert.pem'
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert == 'wcert.pem'


def test_kwargs():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
            },
        },
    }
    with new_config(c):
        sec = Security(tls_scheduler_cert='newcert.pem',
                       require_encryption=True,
                       tls_ca_file=None)
    assert sec.require_encryption is True
    # None value didn't override default
    assert sec.tls_ca_file == 'ca.pem'
    assert sec.tls_ciphers is None
    assert sec.tls_client_key is None
    assert sec.tls_client_cert is None
    assert sec.tls_scheduler_key == 'skey.pem'
    assert sec.tls_scheduler_cert == 'newcert.pem'
    assert sec.tls_worker_key is None
    assert sec.tls_worker_cert is None


def test_repr():
    with new_config({}):
        sec = Security(tls_ca_file='ca.pem', tls_scheduler_cert='scert.pem')
        assert repr(sec) == "Security(tls_ca_file='ca.pem', tls_scheduler_cert='scert.pem')"


def test_tls_config_for_role():
    c = {
        'tls': {
            'ca-file': 'ca.pem',
            'scheduler': {
                'key': 'skey.pem',
                'cert': 'scert.pem',
            },
            'worker': {
                'cert': 'wcert.pem',
            },
            'ciphers': FORCED_CIPHER,
        },
    }
    with new_config(c):
        sec = Security()
    t = sec.get_tls_config_for_role('scheduler')
    assert t == {
        'ca_file': 'ca.pem',
        'key': 'skey.pem',
        'cert': 'scert.pem',
        'ciphers': FORCED_CIPHER,
    }
    t = sec.get_tls_config_for_role('worker')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': 'wcert.pem',
        'ciphers': FORCED_CIPHER,
    }
    t = sec.get_tls_config_for_role('client')
    assert t == {
        'ca_file': 'ca.pem',
        'key': None,
        'cert': None,
        'ciphers': FORCED_CIPHER,
    }
    with pytest.raises(ValueError):
        sec.get_tls_config_for_role('supervisor')


def test_connection_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is False

    def many_ciphers(ctx):
        if sys.version_info >= (3, 6):
            assert len(ctx.get_ciphers()) > 2  # Most likely

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
            },
            'worker': {
                'cert': keycert1,
            },
        },
    }
    with new_config(c):
        sec = Security()

    d = sec.get_connection_args('scheduler')
    assert not d['require_encryption']
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    d = sec.get_connection_args('worker')
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_connection_args('client')
    assert d.get('ssl_context') is None

    # With more settings
    c['tls']['ciphers'] = FORCED_CIPHER
    c['require-encryption'] = True

    with new_config(c):
        sec = Security()

    d = sec.get_listen_args('scheduler')
    assert d['require_encryption']
    ctx = d['ssl_context']
    basic_checks(ctx)
    if sys.version_info >= (3, 6):
        assert len(ctx.get_ciphers()) == 1


def test_listen_args():
    def basic_checks(ctx):
        assert ctx.verify_mode == ssl.CERT_REQUIRED
        assert ctx.check_hostname is False

    def many_ciphers(ctx):
        if sys.version_info >= (3, 6):
            assert len(ctx.get_ciphers()) > 2  # Most likely

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
            },
            'worker': {
                'cert': keycert1,
            },
        },
    }
    with new_config(c):
        sec = Security()

    d = sec.get_listen_args('scheduler')
    assert not d['require_encryption']
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    d = sec.get_listen_args('worker')
    ctx = d['ssl_context']
    basic_checks(ctx)
    many_ciphers(ctx)

    # No cert defined => no TLS
    d = sec.get_listen_args('client')
    assert d.get('ssl_context') is None

    # With more settings
    c['tls']['ciphers'] = FORCED_CIPHER
    c['require-encryption'] = True

    with new_config(c):
        sec = Security()

    d = sec.get_listen_args('scheduler')
    assert d['require_encryption']
    ctx = d['ssl_context']
    basic_checks(ctx)
    if sys.version_info >= (3, 6):
        assert len(ctx.get_ciphers()) == 1


@gen_test()
def test_tls_listen_connect():
    """
    Functional test for TLS connection args.
    """
    @gen.coroutine
    def handle_comm(comm):
        peer_addr = comm.peer_address
        assert peer_addr.startswith('tls://')
        yield comm.write('hello')
        yield comm.close()

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
            },
            'worker': {
                'cert': keycert1,
            },
        },
    }
    with new_config(c):
        sec = Security()

    c['tls']['ciphers'] = FORCED_CIPHER
    with new_config(c):
        forced_cipher_sec = Security()

    with listen('tls://', handle_comm,
                connection_args=sec.get_listen_args('scheduler')) as listener:
        comm = yield connect(listener.contact_address,
                             connection_args=sec.get_connection_args('worker'))
        msg = yield comm.read()
        assert msg == 'hello'
        comm.abort()

        # No SSL context for client
        with pytest.raises(TypeError):
            yield connect(listener.contact_address,
                          connection_args=sec.get_connection_args('client'))

        # Check forced cipher
        comm = yield connect(listener.contact_address,
                             connection_args=forced_cipher_sec.get_connection_args('worker'))
        cipher, _, _, = comm.extra_info['cipher']
        assert cipher == FORCED_CIPHER
        comm.abort()


@gen_test()
def test_require_encryption():
    """
    Functional test for "require_encryption" setting.
    """
    @gen.coroutine
    def handle_comm(comm):
        comm.abort()

    c = {
        'tls': {
            'ca-file': ca_file,
            'scheduler': {
                'key': key1,
                'cert': cert1,
            },
            'worker': {
                'cert': keycert1,
            },
        },
    }
    with new_config(c):
        sec = Security()
    c['require-encryption'] = True
    with new_config(c):
        sec2 = Security()

    for listen_addr in ['inproc://', 'tls://']:
        with listen(listen_addr, handle_comm,
                    connection_args=sec.get_listen_args('scheduler')) as listener:
            comm = yield connect(listener.contact_address,
                                 connection_args=sec2.get_connection_args('worker'))
            comm.abort()

        with listen(listen_addr, handle_comm,
                    connection_args=sec2.get_listen_args('scheduler')) as listener:
            comm = yield connect(listener.contact_address,
                                 connection_args=sec2.get_connection_args('worker'))
            comm.abort()

    @contextmanager
    def check_encryption_error():
        with pytest.raises(RuntimeError) as excinfo:
            yield
        assert "encryption required" in str(excinfo.value)

    for listen_addr in ['tcp://']:
        with listen(listen_addr, handle_comm,
                    connection_args=sec.get_listen_args('scheduler')) as listener:
            comm = yield connect(listener.contact_address,
                                 connection_args=sec.get_connection_args('worker'))
            comm.abort()

            with pytest.raises(RuntimeError):
                yield connect(listener.contact_address,
                              connection_args=sec2.get_connection_args('worker'))

        with pytest.raises(RuntimeError):
            listen(listen_addr, handle_comm,
                   connection_args=sec2.get_listen_args('scheduler'))
