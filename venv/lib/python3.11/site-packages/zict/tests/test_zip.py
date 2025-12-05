import zipfile
from collections.abc import MutableMapping

import pytest

from zict import Zip
from zict.tests import utils_test


@pytest.fixture
def fn(tmp_path, check_fd_leaks):
    yield tmp_path / "tmp.zip"


def test_simple(fn):
    z = Zip(fn)
    assert isinstance(z, MutableMapping)
    assert not z

    assert list(z) == list(z.keys()) == []
    assert list(z.values()) == []
    assert list(z.items()) == []

    z["x"] = b"123"
    assert list(z) == list(z.keys()) == ["x"]
    assert list(z.values()) == [b"123"]
    assert list(z.items()) == [("x", b"123")]
    assert z["x"] == b"123"

    z.flush()
    zz = zipfile.ZipFile(fn, mode="r")
    assert zz.read("x") == b"123"

    z["y"] = b"456"
    assert z["y"] == b"456"


def test_setitem_typeerror(fn):
    z = Zip(fn)
    with pytest.raises(TypeError):
        z["x"] = 123


def test_contextmanager(fn):
    with Zip(fn) as z:
        z["x"] = b"123"

    zz = zipfile.ZipFile(fn, mode="r")
    assert zz.read("x") == b"123"


def test_missing_key(fn):
    z = Zip(fn)

    with pytest.raises(KeyError):
        z["x"]


def test_close(fn):
    z = Zip(fn)

    z["x"] = b"123"
    z.close()

    zz = zipfile.ZipFile(fn, mode="r")
    assert zz.read("x") == b"123"

    with pytest.raises(IOError):
        z["y"] = b"123"


def test_bytearray(fn):
    data = bytearray(b"123")
    with Zip(fn) as z:
        z["x"] = data

    with Zip(fn) as z:
        assert z["x"] == b"123"


def test_memoryview(fn):
    data = memoryview(b"123")
    with Zip(fn) as z:
        z["x"] = data

    with Zip(fn) as z:
        assert z["x"] == b"123"


def check_mapping(z):
    """Shorter version of utils_test.check_mapping, as zip supports neither update nor
    delete
    """
    assert isinstance(z, MutableMapping)
    utils_test.check_empty_mapping(z)

    z["abc"] = b"456"
    z["xyz"] = b"12"
    assert len(z) == 2
    assert z["abc"] == b"456"

    utils_test.check_items(z, [("abc", b"456"), ("xyz", b"12")])

    assert "abc" in z
    assert "xyz" in z
    assert "def" not in z

    with pytest.raises(KeyError):
        z["def"]


def test_mapping(fn):
    """
    Test mapping interface for Zip().
    """
    with Zip(fn) as z:
        check_mapping(z)
        utils_test.check_closing(z)


def test_no_delete_update(fn):
    with Zip(fn) as z:
        z["x"] = b"123"
        with pytest.raises(NotImplementedError):
            del z["x"]
        with pytest.raises(NotImplementedError):
            z["x"] = b"456"
        assert len(z) == 1
        assert z["x"] == b"123"


def test_bad_types(fn):
    with Zip(fn) as z:
        utils_test.check_bad_key_types(z, has_del=False)
        utils_test.check_bad_value_types(z)
