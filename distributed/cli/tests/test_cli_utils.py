from __future__ import print_function, division, absolute_import

import pytest

pytest.importorskip("requests")

from distributed.cli.utils import uri_from_host_port
from distributed.utils import get_ip


external_ip = get_ip()


def test_uri_from_host_port():
    f = uri_from_host_port

    assert f("", 456, None) == "tcp://:456"
    assert f("", 456, 123) == "tcp://:456"
    assert f("", None, 123) == "tcp://:123"
    assert f("", None, 0) == "tcp://"
    assert f("", 0, 123) == "tcp://"

    assert f("localhost", 456, None) == "tcp://localhost:456"
    assert f("localhost", 456, 123) == "tcp://localhost:456"
    assert f("localhost", None, 123) == "tcp://localhost:123"
    assert f("localhost", None, 0) == "tcp://localhost"

    assert f("192.168.1.2", 456, None) == "tcp://192.168.1.2:456"
    assert f("192.168.1.2", 456, 123) == "tcp://192.168.1.2:456"
    assert f("192.168.1.2", None, 123) == "tcp://192.168.1.2:123"
    assert f("192.168.1.2", None, 0) == "tcp://192.168.1.2"

    assert f("tcp://192.168.1.2", 456, None) == "tcp://192.168.1.2:456"
    assert f("tcp://192.168.1.2", 456, 123) == "tcp://192.168.1.2:456"
    assert f("tcp://192.168.1.2", None, 123) == "tcp://192.168.1.2:123"
    assert f("tcp://192.168.1.2", None, 0) == "tcp://192.168.1.2"

    assert f("tcp://192.168.1.2:456", None, None) == "tcp://192.168.1.2:456"
    assert f("tcp://192.168.1.2:456", 0, 0) == "tcp://192.168.1.2:456"
    assert f("tcp://192.168.1.2:456", 0, 123) == "tcp://192.168.1.2:456"
    assert f("tcp://192.168.1.2:456", 456, 123) == "tcp://192.168.1.2:456"

    with pytest.raises(ValueError):
        # Two incompatible port values
        f("tcp://192.168.1.2:456", 123, None)

    assert f("tls://192.168.1.2:456", None, None) == "tls://192.168.1.2:456"
    assert f("tls://192.168.1.2:456", 0, 0) == "tls://192.168.1.2:456"
    assert f("tls://192.168.1.2:456", 0, 123) == "tls://192.168.1.2:456"
    assert f("tls://192.168.1.2:456", 456, 123) == "tls://192.168.1.2:456"

    assert f("tcp://[::1]:456", None, None) == "tcp://[::1]:456"

    assert f("tls://[::1]:456", None, None) == "tls://[::1]:456"
