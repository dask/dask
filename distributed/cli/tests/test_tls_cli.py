from __future__ import print_function, division, absolute_import

from time import sleep


from distributed import Client
from distributed.utils_test import (popen, get_cert, new_config_file,
                                    tls_security, tls_only_config)
from distributed.utils_test import loop  # noqa: F401
from distributed.metrics import time


ca_file = get_cert('tls-ca-cert.pem')
cert = get_cert('tls-cert.pem')
key = get_cert('tls-key.pem')
keycert = get_cert('tls-key-cert.pem')


tls_args = ['--tls-ca-file', ca_file, '--tls-cert', keycert]
tls_args_2 = ['--tls-ca-file', ca_file, '--tls-cert', cert, '--tls-key', key]


def wait_for_cores(c, ncores=1):
    start = time()
    while len(c.ncores()) < 1:
        sleep(0.1)
        assert time() < start + 10


def test_basic(loop):
    with popen(['dask-scheduler', '--no-bokeh'] + tls_args) as s:
        with popen(['dask-worker', '--no-bokeh', 'tls://127.0.0.1:8786'] + tls_args) as w:
            with Client('tls://127.0.0.1:8786', loop=loop,
                        security=tls_security()) as c:
                wait_for_cores(c)


def test_nanny(loop):
    with popen(['dask-scheduler', '--no-bokeh'] + tls_args) as s:
        with popen(['dask-worker', '--no-bokeh', '--nanny', 'tls://127.0.0.1:8786'] + tls_args) as w:
            with Client('tls://127.0.0.1:8786', loop=loop,
                        security=tls_security()) as c:
                wait_for_cores(c)


def test_separate_key_cert(loop):
    with popen(['dask-scheduler', '--no-bokeh'] + tls_args_2) as s:
        with popen(['dask-worker', '--no-bokeh', 'tls://127.0.0.1:8786'] + tls_args_2) as w:
            with Client('tls://127.0.0.1:8786', loop=loop,
                        security=tls_security()) as c:
                wait_for_cores(c)


def test_use_config_file(loop):
    with new_config_file(tls_only_config()):
        with popen(['dask-scheduler', '--no-bokeh', '--host', 'tls://']) as s:
            with popen(['dask-worker', '--no-bokeh', 'tls://127.0.0.1:8786']) as w:
                with Client('tls://127.0.0.1:8786', loop=loop,
                            security=tls_security()) as c:
                    wait_for_cores(c)
