import pytest

from dask_expr._util import RaiseAttributeError


def test_raises_attribute_error():
    class A:
        def x(self):
            ...

    class B(A):
        x = RaiseAttributeError()

    assert hasattr(A, "x")
    assert hasattr(A(), "x")
    assert not hasattr(B, "x")
    assert not hasattr(B(), "x")
    with pytest.raises(AttributeError, match="'B' object has no attribute 'x'"):
        B.x
    with pytest.raises(AttributeError, match="'B' object has no attribute 'x'"):
        B().x
