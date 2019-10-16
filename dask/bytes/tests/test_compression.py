from io import BytesIO

import pytest
from fsspec.compression import compr

from dask.compatibility import PY_VERSION
from dask.bytes.utils import compress


@pytest.mark.parametrize("fmt,File", compr.items())
def test_files(fmt, File):
    if fmt == "zip" and PY_VERSION < "3.6":
        pytest.skip("zipfile is read-only on py35")
    if fmt not in compress:
        pytest.skip("compression function not provided")
    if fmt is None:
        return
    data = b"1234" * 1000
    compressed = compress[fmt](data)

    b = BytesIO(compressed)
    g = File(b, mode="rb")
    data2 = g.read()
    g.close()
    assert data == data2
