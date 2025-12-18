from __future__ import annotations

import pytest

import dask.array as da
from dask.base import collections_to_expr


@pytest.mark.xfail(da._array_expr_enabled(), reason="block_id fusion not implemented for array-expr")
def test_map_blocks_block_id_fusion():
    arr = da.ones((20, 10), chunks=(2, 5))

    def dummy(x, block_id=None, block_info=None):
        return x

    result = arr.map_blocks(dummy).astype("f8")
    dsk = collections_to_expr([result])
    assert len(dsk.__dask_graph__()) == 20
