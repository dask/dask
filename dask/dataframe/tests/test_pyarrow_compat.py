import math
import pickle
import random
import string

import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow")

from dask.dataframe._pyarrow_compat import (
    pyarrow_stringarray_from_parts,
    pyarrow_stringarray_to_parts,
)

if not hasattr(pd.arrays, "ArrowStringArray"):
    pytestmark = pytest.mark.skip("pandas.arrays.ArrowStringArray is not available")


def randstr(i):
    """A random string, prefixed with the index number to make it clearer what the data
    boundaries are"""
    return str(i) + "".join(
        random.choices(string.ascii_letters, k=random.randint(3, 8))
    )


@pytest.mark.parametrize("length", [6, 8, 12, 17])
@pytest.mark.parametrize(
    "slc",
    [slice(None), slice(0, 5), slice(2), slice(2, 5), slice(2, None, 2), slice(0, 0)],
)
@pytest.mark.parametrize("has_mask", [True, False])
def test_roundtrip_stringarray(length, slc, has_mask):
    x = pa.array(
        [randstr(i) if (not has_mask or i % 3) else None for i in range(length)],
    )[slc]

    def unpack(nitems, offsets, data, mask=None, offset=0):
        return nitems, offsets, data, mask, offset

    parts = pyarrow_stringarray_to_parts(x)
    nitems, offsets, data, mask, offset = unpack(*parts)

    # Check individual serialized components are correct
    assert nitems == len(x)

    assert len(offsets) == 4 * (nitems + offset + 1)

    expected_data = "".join(x.drop_null().tolist()).encode("utf-8")
    assert bytes(data) == expected_data

    if mask is not None:
        assert len(mask) == math.ceil(nitems / 8)
        assert x.offset % 8 == offset

    # Test rebuilding from components works
    x2 = pyarrow_stringarray_from_parts(*parts)
    assert x == x2

    # Test pickle roundtrip works
    pd_x = pd.arrays.ArrowStringArray(x)
    pd_x2 = pickle.loads(pickle.dumps(pd_x))
    assert pd_x.equals(pd_x2)


@pytest.mark.parametrize("has_mask", [True, False])
@pytest.mark.parametrize("start,end", [(None, -1), (1, None), (1, -1)])
def test_pickle_stringarray_slice_doesnt_serialize_whole_array(has_mask, start, end):
    x = pd.array(
        ["apple", "banana", "carrot", "durian", "eggplant", "fennel", "grape"],
        dtype="string[pyarrow]",
    )
    if has_mask:
        x[3] = None

    x_sliced = x[start:end]
    buf = pickle.dumps(x_sliced)
    loaded = pickle.loads(buf)
    assert loaded.equals(x_sliced)

    if start is not None:
        assert b"apple" not in buf
    if end is not None:
        assert b"grape" not in buf


@pytest.mark.parametrize("has_mask", [True, False])
def test_pickle_stringarray_supports_pickle_5(has_mask):
    x = pd.array(
        ["apple", "banana", "carrot", "durian", "eggplant", "fennel", "grape"],
        dtype="string[pyarrow]",
    )
    x[3] = None

    buffers = []
    buf = pickle.dumps(x, protocol=5, buffer_callback=buffers.append)
    assert buffers
    x2 = pickle.loads(buf, buffers=buffers)
    assert x.equals(x2)
