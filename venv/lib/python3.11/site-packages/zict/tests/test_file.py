import os
import pathlib
import sys

import pytest

from zict import File
from zict.tests import utils_test


def test_mapping(tmp_path, check_fd_leaks):
    """
    Test mapping interface for File().
    """
    z = File(tmp_path)
    utils_test.check_mapping(z)


@pytest.mark.parametrize("dirtype", [str, pathlib.Path, lambda x: x])
def test_implementation(tmp_path, check_fd_leaks, dirtype):
    z = File(dirtype(tmp_path))
    assert not z

    z["x"] = b"123"
    assert os.listdir(tmp_path) == ["x#0"]
    with open(tmp_path / "x#0", "rb") as f:
        assert f.read() == b"123"

    assert "x" in z
    out = z["x"]
    assert isinstance(out, bytearray)
    assert out == b"123"


def test_memmap_implementation(tmp_path, check_fd_leaks):
    z = File(tmp_path, memmap=True)
    assert not z

    mv = memoryview(b"123")
    assert "x" not in z
    z["x"] = mv
    assert os.listdir(tmp_path) == ["x#0"]
    assert "x" in z
    mv2 = z["x"]
    assert mv2 == b"123"
    # Buffer is writeable
    mv2[0] = mv2[1]
    assert mv2 == b"223"


def test_str(tmp_path, check_fd_leaks):
    z = File(tmp_path)
    assert str(z) == repr(z) == f"<File: {tmp_path}, 0 elements>"


def test_setitem_typeerror(tmp_path, check_fd_leaks):
    z = File(tmp_path)
    with pytest.raises(TypeError):
        z["x"] = 123


def test_contextmanager(tmp_path, check_fd_leaks):
    with File(tmp_path) as z:
        z["x"] = b"123"

    with open(tmp_path / "x#0", "rb") as fh:
        assert fh.read() == b"123"


def test_delitem(tmp_path, check_fd_leaks):
    z = File(tmp_path)

    z["x"] = b"123"
    assert os.listdir(tmp_path) == ["x#0"]
    del z["x"]
    assert os.listdir(tmp_path) == []
    # File name is never repeated
    z["x"] = b"123"
    assert os.listdir(tmp_path) == ["x#1"]
    # __setitem__ deletes the previous file
    z["x"] = b"123"
    assert os.listdir(tmp_path) == ["x#2"]


def test_missing_key(tmp_path, check_fd_leaks):
    z = File(tmp_path)

    with pytest.raises(KeyError):
        z["x"]


def test_arbitrary_chars(tmp_path, check_fd_leaks):
    z = File(tmp_path)

    # Avoid hitting the Windows max filename length
    chunk = 16
    for i in range(1, 128, chunk):
        key = "".join(["foo_"] + [chr(i) for i in range(i, min(128, i + chunk))])
        with pytest.raises(KeyError):
            z[key]
        z[key] = b"foo"
        assert z[key] == b"foo"
        assert list(z) == [key]
        assert list(z.keys()) == [key]
        assert list(z.items()) == [(key, b"foo")]
        assert list(z.values()) == [b"foo"]

        zz = File(tmp_path)
        assert zz[key] == b"foo"
        assert list(zz) == [key]
        assert list(zz.keys()) == [key]
        assert list(zz.items()) == [(key, b"foo")]
        assert list(zz.values()) == [b"foo"]
        del zz

        del z[key]
        with pytest.raises(KeyError):
            z[key]


def test_write_list_of_bytes(tmp_path, check_fd_leaks):
    z = File(tmp_path)

    z["x"] = [b"123", b"4567"]
    assert z["x"] == b"1234567"


def test_bad_types(tmp_path, check_fd_leaks):
    z = File(tmp_path)
    utils_test.check_bad_key_types(z)
    utils_test.check_bad_value_types(z)


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
def test_stress_different_keys_threadsafe(tmp_path):
    z = File(tmp_path)
    utils_test.check_different_keys_threadsafe(z)
    utils_test.check_mapping(z)


@pytest.mark.stress
@pytest.mark.repeat(utils_test.REPEAT_STRESS_TESTS)
@pytest.mark.skipif(sys.platform == "win32", reason="Can't delete file with open fd")
def test_stress_same_key_threadsafe(tmp_path):
    z = File(tmp_path)
    utils_test.check_same_key_threadsafe(z)
    utils_test.check_mapping(z)
