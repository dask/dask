import pytest
import numpy as np

import dask.array as da
from dask.array.utils import assert_eq


@pytest.mark.parametrize(
    "shape, chunks",
    [
        [(5,), (2,)],
        [(5, 3), (2, 2)],
        [(4, 5, 3), (2, 2, 2)],
        [(4, 5, 2, 3), (2, 2, 2, 2)],
        [(2, 5, 2, 4, 3), (2, 2, 2, 2, 2)],
    ],
)
@pytest.mark.parametrize("norm", [None, 1, -1, np.inf, -np.inf])
@pytest.mark.parametrize("keepdims", [False, True])
def test_norm_any_slice(shape, chunks, norm, keepdims):
    a = np.random.random(shape)
    print(a.tolist())
    d = da.from_array(a, chunks=chunks)

    for firstaxis in range(len(shape)):
        for secondaxis in range(len(shape)):
            if firstaxis != secondaxis:
                axis = (firstaxis, secondaxis)
            else:
                axis = firstaxis
            a_r = np.linalg.norm(a, ord=norm, axis=axis, keepdims=keepdims)
            d_r = da.linalg.norm(d, ord=norm, axis=axis, keepdims=keepdims)
            assert_eq(a_r, d_r)
