import os
import pathlib

import pytest

from zict import LMDB
from zict.tests import utils_test

pytest.importorskip("lmdb")


@pytest.mark.parametrize("dirtype", [str, pathlib.Path, lambda x: x])
def test_dirtypes(tmp_path, check_fd_leaks, dirtype):
    z = LMDB(tmp_path)
    z["x"] = b"123"
    assert z["x"] == b"123"
    del z["x"]


def test_mapping(tmp_path, check_fd_leaks):
    """
    Test mapping interface for LMDB().
    """
    z = LMDB(tmp_path)
    utils_test.check_mapping(z)


def test_bad_types(tmp_path, check_fd_leaks):
    z = LMDB(tmp_path)
    utils_test.check_bad_key_types(z)
    utils_test.check_bad_value_types(z)


def test_reuse(tmp_path, check_fd_leaks):
    """
    Test persistence of a LMDB() mapping.
    """
    with LMDB(tmp_path) as z:
        assert len(z) == 0
        z["abc"] = b"123"

    with LMDB(tmp_path) as z:
        assert len(z) == 1
        assert z["abc"] == b"123"


def test_creates_dir(tmp_path, check_fd_leaks):
    with LMDB(tmp_path, check_fd_leaks):
        assert os.path.isdir(tmp_path)


def test_file_descriptors_dont_leak(tmp_path, check_fd_leaks):
    z = LMDB(tmp_path)
    del z

    z = LMDB(tmp_path)
    z.close()

    with LMDB(tmp_path) as z:
        pass


def test_map_size(tmp_path, check_fd_leaks):
    import lmdb

    z = LMDB(tmp_path, map_size=2**20)
    z["x"] = b"x" * 2**19
    with pytest.raises(lmdb.MapFullError):
        z["y"] = b"x" * 2**20
