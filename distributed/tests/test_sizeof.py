import pytest
import logging
from dask.sizeof import sizeof

from distributed.sizeof import safe_sizeof
from distributed.utils_test import captured_logger


@pytest.mark.parametrize("obj", [list(range(10)), tuple(range(10)), set(range(10))])
def test_safe_sizeof(obj):
    assert safe_sizeof(obj) == sizeof(obj)


def test_safe_sizeof_raises():
    class BadlySized:
        def __sizeof__(self):
            raise ValueError("bar")

    foo = BadlySized()
    with captured_logger(logging.getLogger("distributed.sizeof")) as logs:
        assert safe_sizeof(foo) == 1e6

    assert "Sizeof calculation failed.  Defaulting to 1MB" in logs.getvalue()
