from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import numpy


def get_host_array(a: numpy.ndarray) -> numpy.ndarray | bytes | bytearray:
    """Given a numpy array, find the underlying memory allocated by either
    distributed.protocol.utils.host_array or internally by numpy
    """
    import numpy

    assert isinstance(a, numpy.ndarray)
    o: object = a
    while True:
        if isinstance(o, memoryview):
            o = o.obj
        elif isinstance(o, numpy.ndarray):
            if o.base is not None:
                o = o.base
            else:
                return o
        elif isinstance(o, (bytes, bytearray)):
            return o
        else:  # pragma: nocover
            raise TypeError(f"Unexpected numpy buffer: {o!r}")
